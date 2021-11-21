use crossbeam::channel;
use log::SetLoggerError;
use protobuf::ProtobufError;
use std::sync::{MutexGuard, PoisonError, RwLockReadGuard, RwLockWriteGuard};

use crate::protos::messages::{Command, CommandType};

#[derive(Clone, Debug, PartialEq)]
pub enum Record {
    Value(String),
    Deleted,
}

impl Record {
    pub fn len(&self) -> usize {
        match self {
            Record::Value(string) => string.len(),
            Record::Deleted => 2,
        }
    }

    pub fn from_command(command: &Command) -> Result<Record> {
        match command.get_command_type() {
            CommandType::SET_VALUE => {
                if !command.has_value() {
                    return Err(NaiveError::InvalidData);
                }
                Ok(Record::Value(command.get_value().to_owned()))
            }
            CommandType::DELETE => {
                if command.has_value() {
                    return Err(NaiveError::InvalidData);
                }
                Ok(Record::Deleted)
            }
        }
    }
}

#[derive(Debug)]
pub enum NaiveError {
    Unknown,
    IoError(std::io::Error),
    IoIntoInnerError,
    RwLockReadError,
    RwLockWriteError,
    MutexLockError,
    ChannelSendError,
    ProtobufError,
    InvalidData,
    SetLoggerError,
}

impl From<std::io::Error> for NaiveError {
    fn from(error: std::io::Error) -> Self {
        NaiveError::IoError(error)
    }
}

impl<W> From<std::io::IntoInnerError<W>> for NaiveError {
    fn from(_: std::io::IntoInnerError<W>) -> Self {
        NaiveError::IoIntoInnerError
    }
}

impl<T> From<PoisonError<RwLockReadGuard<'_, T>>> for NaiveError {
    fn from(_: PoisonError<RwLockReadGuard<'_, T>>) -> Self {
        NaiveError::RwLockReadError
    }
}

impl<T> From<PoisonError<RwLockWriteGuard<'_, T>>> for NaiveError {
    fn from(_: PoisonError<RwLockWriteGuard<'_, T>>) -> Self {
        NaiveError::RwLockWriteError
    }
}

impl<T> From<PoisonError<MutexGuard<'_, T>>> for NaiveError {
    fn from(_: PoisonError<MutexGuard<'_, T>>) -> Self {
        NaiveError::MutexLockError
    }
}

impl<T> From<channel::SendError<T>> for NaiveError {
    fn from(_: channel::SendError<T>) -> Self {
        NaiveError::ChannelSendError
    }
}

impl From<ProtobufError> for NaiveError {
    fn from(_: ProtobufError) -> Self {
        NaiveError::ProtobufError
    }
}

impl From<SetLoggerError> for NaiveError {
    fn from(_: SetLoggerError) -> Self {
        NaiveError::SetLoggerError
    }
}

pub type Result<T> = std::result::Result<T, NaiveError>;

impl From<Record> for Result<Option<String>> {
    fn from(record: Record) -> Self {
        if let Record::Value(value) = record {
            return Ok(Some(value));
        }
        Ok(None)
    }
}
