use std::collections::{btree_map, BTreeMap};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::Path;

use crate::protos::messages::{Command, CommandType};
use crate::types::{NaiveError, Record, Result};
use crate::utils;

struct Memtable {
    /// The in-memory data.
    data: BTreeMap<String, Record>,

    /// The accumulated size of the key-value store.
    data_size: usize,

    /// The write-ahead log file.
    log_writer: BufWriter<File>,
}

impl Memtable {
    pub fn open(log_path: &Path) -> Result<Self> {
        let mut data = BTreeMap::new();
        let mut data_size = 0;

        let log_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(log_path)?;

        // Redo the commands in the log to recover the in-memory data.
        let mut log_reader = BufReader::new(log_file);
        while let Some(command) = utils::read_message::<Command, BufReader<File>>(&mut log_reader)?
        {
            apply_command_to_data(&command, &mut data, &mut data_size)?;
        }
        let log_writer = BufWriter::new(log_reader.into_inner());

        Ok(Memtable {
            data,
            data_size,
            log_writer,
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
        utils::write_message(&command, &mut self.log_writer, true /* flush */)?;

        apply_command_to_data(&command, &mut self.data, &mut self.data_size)
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        // Write the log before updating the in-memory data.
        let mut command = Command::new();
        command.set_key(key.clone());
        command.set_command_type(CommandType::DELETE);
        utils::write_message(&command, &mut self.log_writer, true /* flush */)?;

        apply_command_to_data(&command, &mut self.data, &mut self.data_size)
    }

    pub fn iter(&self) -> btree_map::Iter<'_, String, Record> {
        self.data.iter()
    }

    pub fn data_size(&self) -> usize {
        self.data_size
    }
}

fn apply_command_to_data(
    command: &Command,
    data: &mut BTreeMap<String, Record>,
    data_size: &mut usize,
) -> Result<()> {
    let record = match command.get_command_type() {
        CommandType::SET_VALUE => {
            if !command.has_value() {
                return Err(NaiveError::InvalidData);
            }
            Record::Value(command.get_value().to_owned())
        }
        CommandType::DELETE => {
            if command.has_value() {
                return Err(NaiveError::InvalidData);
            }
            Record::Deleted
        }
    };
    if let Some(record_mut) = data.get_mut(command.get_key()) {
        // Replace the old record with the new one.
        *data_size -= record_mut.len();
        *data_size += record.len();
        std::mem::replace(record_mut, record);
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
        let log_path = Path::new("/tmp/test_memtable.log");

        let mut memtable = Memtable::open(&log_path).unwrap();
        for num in 0..=MAX_NUMBER {
            let num_str = num.to_string();
            memtable.set(num_str.clone(), num_str.clone()).unwrap();
        }
        for num in 0..=MAX_NUMBER {
            let num_str = num.to_string();
            let record = memtable.get(&num_str).unwrap();
            assert!(record.is_some());
            assert!(record.unwrap() == Record::Value(num_str.clone()));
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
            assert!(record.is_some());
            if num % 2 == 0 {
                assert!(record.unwrap() == Record::Value(num_str.clone()));
            } else {
                assert!(record.unwrap() == Record::Deleted);
            }
        }

        // Restart from the disk.
        let memtable = Memtable::open(&log_path).unwrap();
        for num in 0..=MAX_NUMBER {
            let num_str = num.to_string();
            let record = memtable.get(&num_str).unwrap();
            assert!(record.is_some());
            if num % 2 == 0 {
                assert!(record.unwrap() == Record::Value(num_str.clone()));
            } else {
                assert!(record.unwrap() == Record::Deleted);
            }
        }
    }
}
