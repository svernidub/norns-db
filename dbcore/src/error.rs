use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum NornsDbError {
    #[error("WAL record is corrupted")]
    WalRecordCorrupted,

    #[error("Unhandled error: {error:?}")]
    Unknown {
        error: Box<dyn std::error::Error + Send + Sync>,
        message: String,
    },
}

impl NornsDbError {
    pub fn unknown_with_message<E, S>(error: E, message: S) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
        S: Into<String>,
    {
        NornsDbError::Unknown {
            error: Box::new(error),
            message: message.into(),
        }
    }
}
