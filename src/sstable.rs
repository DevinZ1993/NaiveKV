use std::cmp::Reverse;
use std::collections::{btree_map, BTreeMap, BinaryHeap};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::memtable::Memtable;
use crate::protos::messages::{Command, CommandType};
use crate::types::{NaiveError, Record, Result};
use crate::utils;

/// Use an architecture-independent type to store generation numbers in files.
type GenerationNumberType = u32;

const N_BYTES_GENERATION_NUMBER: usize = (GenerationNumberType::BITS as usize) >> 3;

/// Write the buffered chunk into the file if its size exceeds this number.
const SSTABLE_CHUNK_SIZE_THRESHOLD: usize = 1024;

// TODO: try replacing this with the skip list.
type SSTableIndex = BTreeMap<String, u64>;

pub struct SSTable {
    /// The generation number.
    gen_no: usize,

    /// The ordered in-memory index.
    index: SSTableIndex,

    /// The segment file reader, shared by multiple threads.
    file_reader: Mutex<BufReader<File>>,

    /// The size of the segment file in bytes.
    file_size: usize,
}

impl<'a> SSTable {
    /// Recover from an existing segment file.
    pub fn open(file_path: &Path) -> Result<Self> {
        let mut segment_file = OpenOptions::new()
            .read(true)
            .create(false)
            .open(file_path)?;
        let file_size = segment_file.metadata()?.len() as usize;

        // Read the generation number at the start of the file.
        let mut gen_no_bytes = [0u8; N_BYTES_GENERATION_NUMBER];
        segment_file.read_exact(&mut gen_no_bytes)?;
        let gen_no = GenerationNumberType::from_be_bytes(gen_no_bytes) as usize;

        let mut file_reader = BufReader::new(segment_file);
        let index = build_sstable_index(&mut file_reader)?;

        let file_reader = Mutex::new(file_reader);

        Ok(SSTable {
            gen_no,
            index,
            file_reader,
            file_size,
        })
    }

    /// Create a new segment file by merging a Memtable with a list of SSTables.
    pub fn create(
        file_path: &Path,
        memtable: &Memtable,
        sstables: &Vec<Arc<SSTable>>,
    ) -> Result<Self> {
        let mut heap = BinaryHeap::with_capacity(sstables.len() + 1);

        let mut memtable_iter = memtable.iter();
        let mut memtable_record = None;
        if let Some((key, record)) = memtable_iter.next() {
            heap.push(Reverse((key.to_owned(), 0)));
            memtable_record = Some(record.to_owned());
        }

        let mut sstable_iters = Vec::with_capacity(sstables.len());
        let mut sstable_records = Vec::with_capacity(sstables.len());
        for sstable in sstables.iter() {
            let index = sstable_iters.len();
            let mut sstable_iter = sstable.pseudo_iter();
            if let Some((key, record)) = sstable_iter.next()? {
                heap.push(Reverse((key, index + 1)));
                sstable_iters.push(sstable_iter);
                sstable_records.push(Some(record));
            }
        }

        let mut index = SSTableIndex::new();
        let segment_file = OpenOptions::new()
            .append(true)
            .create_new(true)
            .read(true)
            .open(file_path)?;
        let mut file_writer = BufWriter::new(segment_file);

        // Write the generation number at the beginning of the file.
        let gen_no = sstables.len();
        file_writer.write(&(gen_no as GenerationNumberType).to_be_bytes())?;

        let mut buffer = Vec::new();
        let mut last_key = None;
        while let Some(Reverse((key, source))) = heap.pop() {
            // With the same key, keep the record from the smallest source number.
            // i.e. If a key exits in the Memtable or an SSTable of younger generation, ignore its
            // existence in older generations.
            let is_new_key = last_key.is_none() || *last_key.as_ref().unwrap() != key;
            if is_new_key {
                last_key = Some(key.clone());
            }
            if source == 0 {
                // This comes from the Memtable.
                if is_new_key {
                    let record = memtable_record.take().unwrap();
                    append_command_to_sstable(
                        &mut index,
                        &mut file_writer,
                        &mut buffer,
                        key,
                        record,
                    );
                }
                if let Some((key, record)) = memtable_iter.next() {
                    heap.push(Reverse((key.clone(), 0)));
                    memtable_record = Some(record.clone());
                }
            } else {
                // This comes from an SSTable.
                if is_new_key {
                    let record = sstable_records[source - 1].take().unwrap();
                    append_command_to_sstable(
                        &mut index,
                        &mut file_writer,
                        &mut buffer,
                        key,
                        record,
                    );
                }
                let sstable_iter = &mut sstable_iters[source - 1];
                if let Some((key, record)) = sstable_iter.next()? {
                    heap.push(Reverse((key, source)));
                    sstable_records[source - 1] = Some(record);
                }
            }
        }
        if !buffer.is_empty() {
            // Write out the remaining buffered bytes into a chunk.
            utils::write_chunk(&mut file_writer, &buffer)?;
        }

        let segment_file = file_writer.into_inner()?;
        let file_size = segment_file.metadata()?.len() as usize;
        let file_reader = Mutex::new(BufReader::new(segment_file));
        Ok(SSTable {
            gen_no,
            index,
            file_reader,
            file_size,
        })
    }

    pub fn get(&self, key: &str) -> Result<Option<Record>> {
        // Find the largest indexed key that is not greater than the query key.
        if let Some((_, &offset)) = self.index.range(..=key.to_owned()).next_back() {
            let mut buffer = Vec::new();
            let num_bytes = seek_and_read_chunk(&self.file_reader, &mut buffer, offset)?;
            if num_bytes == 0 {
                return Err(NaiveError::InvalidData);
            }

            // Deserialize the messages in the chunk in order.
            let mut buffer_reader = &buffer[..];
            while let Some(command) = utils::read_message::<Command, &[u8]>(&mut buffer_reader)? {
                match command.get_key().partial_cmp(&key).unwrap() {
                    std::cmp::Ordering::Less => (),
                    std::cmp::Ordering::Equal => {
                        return Ok(Some(Record::from_command(&command)?));
                    }
                    std::cmp::Ordering::Greater => {
                        return Ok(None);
                    }
                }
            }
        }
        Ok(None)
    }

    pub fn gen_no(&self) -> usize {
        self.gen_no
    }

    pub fn file_size(&self) -> usize {
        self.file_size
    }

    fn pseudo_iter(&'a self) -> SSTableIterator<'a> {
        let index_iter = self.index.iter();
        let file_reader = &self.file_reader;
        let chunk_buffer = Vec::new();
        let chunk_offset = 0;
        SSTableIterator {
            index_iter,
            file_reader,
            chunk_buffer,
            chunk_offset,
        }
    }
}

/// Scan the segment file and build up the in-memory index.
fn build_sstable_index<Reader>(file_reader: &mut Reader) -> Result<SSTableIndex>
where
    Reader: std::io::Read + std::io::Seek,
{
    let mut index = SSTableIndex::new();
    let mut buffer = Vec::new();
    loop {
        let current_offset = file_reader.seek(std::io::SeekFrom::Current(0))?;

        // Read the entire chunk into the buffer.
        let num_bytes = utils::read_chunk(file_reader, &mut buffer)?;
        if num_bytes == 0 {
            break;
        }

        // Read the first message of the chunk and record its key.
        match utils::read_message::<Command, &[u8]>(&mut &buffer[..])? {
            Some(command) => {
                index.insert(command.get_key().to_owned(), current_offset);
            }
            None => {
                return Err(NaiveError::InvalidData);
            }
        }
    }
    Ok(index)
}

/// A pseudo-iterator for SSTable.
struct SSTableIterator<'a> {
    /// The iterator of the SSTable index.
    index_iter: btree_map::Iter<'a, String, u64>,

    /// A reference into the file reader of the SSTable.
    file_reader: &'a Mutex<BufReader<File>>,

    /// A buffer for holding a chunk of bytes read from file_reader.
    chunk_buffer: Vec<u8>,

    /// The offset into chunk_buffer.
    chunk_offset: u64,
}

impl<'a> SSTableIterator<'a> {
    fn next(&mut self) -> Result<Option<(String, Record)>> {
        loop {
            let mut chunk_cursor = std::io::Cursor::new(&self.chunk_buffer);
            chunk_cursor.seek(std::io::SeekFrom::Start(self.chunk_offset))?;
            if let Some(command) =
                utils::read_message::<Command, std::io::Cursor<&Vec<u8>>>(&mut chunk_cursor)?
            {
                self.chunk_offset = chunk_cursor.seek(std::io::SeekFrom::Current(0))?;
                return Ok(Some((
                    command.get_key().to_owned(),
                    Record::from_command(&command)?,
                )));
            }

            if let Some((_, &offset)) = self.index_iter.next() {
                let num_bytes =
                    seek_and_read_chunk(&self.file_reader, &mut self.chunk_buffer, offset)?;
                if num_bytes == 0 {
                    return Err(NaiveError::InvalidData);
                }
                self.chunk_offset = 0;
            } else {
                return Ok(None);
            }
        }
    }
}

fn seek_and_read_chunk(
    file_reader: &Mutex<BufReader<File>>,
    buffer: &mut Vec<u8>,
    offset: u64,
) -> Result<usize> {
    let mut file_reader = file_reader.lock()?;
    file_reader.seek(std::io::SeekFrom::Start(offset))?;
    utils::read_chunk(&mut *file_reader, buffer)
}

fn append_command_to_sstable(
    index: &mut SSTableIndex,
    file_writer: &mut BufWriter<File>,
    buffer: &mut Vec<u8>,
    key: String,
    record: Record,
) -> Result<()> {
    if buffer.is_empty() {
        // This is the first key in the chunk.
        let offset = file_writer.seek(std::io::SeekFrom::Current(0))?;
        index.insert(key.clone(), offset);
    }

    let mut command = Command::new();
    command.set_key(key);
    match record {
        Record::Value(value) => {
            command.set_command_type(CommandType::SET_VALUE);
            command.set_value(value);
        }
        Record::Deleted => {
            command.set_command_type(CommandType::DELETE);
        }
    }

    utils::write_message(&command, buffer)?;
    if buffer.len() >= SSTABLE_CHUNK_SIZE_THRESHOLD {
        // Write the chunk if its size exceeds the threshold.
        utils::write_chunk(file_writer, buffer)?;
        buffer.clear();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sstable() {
        const MAX_NUMBER: i32 = 10000; // Make sure this spans over multiple chunks.
        const MAX_GEN_NO: i32 = 2;

        let mut expected_values = BTreeMap::new();

        let memtable_log_path = Path::new("/tmp/test_sstable_memtable.log");
        let empty_sstables = Vec::new();
        let mut sstables = Vec::new();
        for gen_no in (0..=MAX_GEN_NO).rev() {
            utils::try_remove_file(&memtable_log_path).unwrap();
            let mut memtable = Memtable::open(&memtable_log_path).unwrap();
            for num in 0..MAX_NUMBER {
                let key = (gen_no + 2) * num;
                let value = (gen_no + 2) * num + gen_no + 1;
                expected_values.insert(key, value);
                memtable.set(key.to_string(), value.to_string()).unwrap();
            }
            let sstable_path_str = format!("/tmp/test_gen_{}.sst", gen_no);
            let sstable_path = Path::new(&sstable_path_str);
            utils::try_remove_file(&sstable_path).unwrap();
            let sstable = SSTable::create(&sstable_path, &memtable, &empty_sstables).unwrap();
            for num in 0..MAX_NUMBER {
                let key = ((gen_no + 2) * num).to_string();
                let value = ((gen_no + 2) * num + gen_no + 1).to_string();
                let record = sstable.get(&key).unwrap();
                assert!(record == Some(Record::Value(value)));
            }
            sstables.push(Arc::new(sstable));
        }
        sstables.reverse();

        utils::try_remove_file(&memtable_log_path).unwrap();
        let mut memtable = Memtable::open(&memtable_log_path).unwrap();
        for num in 0..MAX_NUMBER {
            expected_values.insert(num, num);
            let key = num.to_string();
            let value = key.clone();
            memtable.set(key, value).unwrap();
        }

        let sstable_path = Path::new("/tmp/test_sstable.sst");
        utils::try_remove_file(&sstable_path).unwrap();
        SSTable::create(&sstable_path, &memtable, &sstables).unwrap();

        let sstable = SSTable::open(&sstable_path).unwrap();
        for (key, value) in expected_values {
            let key = key.to_string();
            let value = value.to_string();
            let record = sstable.get(&key).unwrap();
            assert!(record == Some(Record::Value(value)));
        }
    }
}
