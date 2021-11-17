use std::collections::{btree_map, BTreeMap};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use std::sync::Mutex;

use crate::protos::messages::{Command, CommandType};
use crate::types::{Record, Result};
use crate::utils;

pub struct Memtable {
    /// The in-memory data.
    data: BTreeMap<String, Record>,

    /// The heuristic size of the in-memory data, used for triggering compaction.
    data_size: usize,

    /// The path of the write-ahead log.
    log_path: PathBuf,

    /// The write-ahead log writer.
    log_writer: BufWriter<File>,

    /// Whether the Memtable is deprecated.
    is_deprecated: Mutex<bool>,
}

impl Memtable {
    pub fn open(log_path: PathBuf) -> Result<Self> {
        let mut data = BTreeMap::new();
        let mut data_size = 0;

        let log_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(log_path.as_path())?;

        // Redo the commands in the log to recover the in-memory data.
        let mut log_reader = BufReader::new(log_file);
        while let Some(command) = utils::read_message::<Command, BufReader<File>>(&mut log_reader)?
        {
            apply_command_to_data(&command, &mut data, &mut data_size)?;
        }
        let log_writer = BufWriter::new(log_reader.into_inner());

        let is_deprecated = Mutex::new(false);

        Ok(Memtable {
            data,
            data_size,
            log_path,
            log_writer,
            is_deprecated,
        })
    }

    pub fn get(&self, key: &str) -> Result<Option<Record>> {
        Ok(self.data.get(key).map(|record| (*record).clone()))
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // Write the log before updating the in-memory data.
        let mut command = Command::new();
        command.set_key(key.clone());
        command.set_command_type(CommandType::SET_VALUE);
        command.set_value(value);
        utils::write_message(&command, &mut self.log_writer)?;

        apply_command_to_data(&command, &mut self.data, &mut self.data_size)
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        // Write the log before updating the in-memory data.
        let mut command = Command::new();
        command.set_key(key.clone());
        command.set_command_type(CommandType::DELETE);
        utils::write_message(&command, &mut self.log_writer)?;

        apply_command_to_data(&command, &mut self.data, &mut self.data_size)
    }

    pub fn iter(&self) -> btree_map::Iter<'_, String, Record> {
        self.data.iter()
    }

    pub fn data_size(&self) -> usize {
        self.data_size
    }

    /// This is called by the compaction daemon once the Memtable is merged into an SSTable.
    pub fn deprecate(&self) -> Result<()> {
        let mut is_deprecated = self.is_deprecated.lock()?;
        *is_deprecated = true;
        Ok(())
    }
}

impl Drop for Memtable {
    fn drop(&mut self) {
        // If is_deprecated is set, remove the write-ahead log on drop.
        let is_deprecated = self
            .is_deprecated
            .lock()
            .expect("Failed to lock the mutex for Memtable::is_deprecated.");
        if *is_deprecated {
            let log_path = self.log_path.as_path();
            utils::try_remove_file(log_path).expect(&format!(
                "Failed to delete Memtable log {}.",
                log_path.display()
            ));
        }
    }
}

fn apply_command_to_data(
    command: &Command,
    data: &mut BTreeMap<String, Record>,
    data_size: &mut usize,
) -> Result<()> {
    let record = Record::from_command(command)?;
    if let Some(ref mut record_mut) = data.get_mut(command.get_key()) {
        // Replace the old record with the new one.
        *data_size -= record_mut.len();
        *data_size += record.len();
        let _ = std::mem::replace(*record_mut, record);
    } else {
        // Insert the key-record pair.
        // Note that even in the case of deletion we cannot simply remove the key from the data,
        // otherwise we cannot overwrite its existence in the SSTables.
        let key = command.get_key().to_owned();
        let record = Record::Value(command.get_value().to_owned());
        *data_size += key.len() + record.len();
        data.insert(key, record);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable() {
        const MAX_NUMBER: i32 = 1000;
        let log_path = PathBuf::from("/tmp/test_memtable.log");
        utils::try_remove_file(&log_path).unwrap();

        let mut memtable = Memtable::open(log_path.clone()).unwrap();
        for num in 0..=MAX_NUMBER {
            let num_str = num.to_string();
            memtable.set(num_str.clone(), num_str.clone()).unwrap();
        }
        for num in 0..=MAX_NUMBER {
            let num_str = num.to_string();
            let record = memtable.get(&num_str).unwrap();
            assert!(record == Some(Record::Value(num_str.clone())));
        }
        assert!(memtable
            .get(&(MAX_NUMBER + 1).to_string())
            .unwrap()
            .is_none());

        // Remove all the odd numbers.
        for num in (1..=MAX_NUMBER).step_by(2) {
            let num_str = num.to_string();
            memtable.remove(num_str).unwrap();
        }
        for num in 0..=MAX_NUMBER {
            let num_str = num.to_string();
            let record = memtable.get(&num_str).unwrap();
            if num % 2 == 0 {
                assert!(record == Some(Record::Value(num_str.clone())));
            } else {
                assert!(record == Some(Record::Deleted));
            }
        }

        // Restart from the disk.
        let memtable = Memtable::open(log_path.clone()).unwrap();
        memtable.deprecate().unwrap();
        for num in 0..=MAX_NUMBER {
            let num_str = num.to_string();
            let record = memtable.get(&num_str).unwrap();
            if num % 2 == 0 {
                assert!(record == Some(Record::Value(num_str.clone())));
            } else {
                assert!(record == Some(Record::Deleted));
            }
        }
    }
}
