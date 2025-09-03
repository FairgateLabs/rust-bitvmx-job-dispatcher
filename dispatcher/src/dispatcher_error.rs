use storage_backend::error::StorageError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DispatcherError {
    #[error("Serialization error {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Result type mismatch {0}")]
    ResultTypeMismatch(String),

    #[error("Job id \"{0}\" already exists")]
    JobIdAlreadyExists(String),

    #[error("Job id not found: {0}")]
    JobIdNotFound(String),

    #[error("Process failed with status: {0}")]
    ProcessFailed(i32),

    #[error("IO error {0}")]
    IoError(#[from] std::io::Error),

    #[error("Storage error {0}")]
    StorageError(#[from] StorageError),

    #[error("mutex poisoned")]
    MutexPoisoned,

    #[error("Parse error")]
    ParseError,

    #[error("Broker error")]
    BrokerError(#[from] bitvmx_broker::rpc::errors::BrokerError),
}
