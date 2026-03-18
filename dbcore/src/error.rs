use thiserror::Error;

#[derive(Error, Debug)]
pub enum NornsDbError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Encode(#[from] bincode::error::EncodeError),

    #[error("deserialization error: {0}")]
    Decode(#[from] bincode::error::DecodeError),

    #[error("data corrupted: {0}")]
    DataCorrupted(String),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("primary key type mismatch: expected {expected}, got {actual}")]
    PrimaryKeyTypeMismatch { expected: String, actual: String },

    #[error("column count mismatch: expected {expected}, got {actual}")]
    ColumnCountMismatch { expected: usize, actual: usize },

    #[error("column {index} ({name}) type mismatch: expected {expected}, got {actual}")]
    ColumnTypeMismatch {
        index: usize,
        name: String,
        expected: String,
        actual: String,
    },

    #[error("table already exists: {name}")]
    TableAlreadyExists { name: String },

    #[error("table not found: {name}")]
    TableNotFound { name: String },

    #[error("internal error: {message}")]
    Internal {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
        message: String,
    },
}

impl NornsDbError {
    pub fn internal<E, S>(error: E, message: S) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
        S: Into<String>,
    {
        NornsDbError::Internal {
            source: Box::new(error),
            message: message.into(),
        }
    }
}
