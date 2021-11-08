use crossbeam::channel;
use protobuf::ProtobufError;
use std::sync::{MutexGuard, PoisonError, RwLockReadGuard, RwLockWriteGuard};

pub enum Record {
    Value(String),
    Deleted,
}

#[derive(Debug)]
pub enum NaiveError {
    Unknown,
    IoError(std::io::Error),
    RwLockReadError,
    RwLockWriteError,
    MutexLockError,
    ChannelSendError,
    ProtobufError,
}

impl From<std::io::Error> for NaiveError {
    fn from(error: std::io::Error) -> Self {
        NaiveError::IoError(error)
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

pub type Result<T> = std::result::Result<T, NaiveError>;

impl From<Record> for Result<Option<String>> {
    fn from(record: Record) -> Self {
        if let Record::Value(value) = record {
            return Ok(Some(value));
        }
        Ok(None)
    }
}
