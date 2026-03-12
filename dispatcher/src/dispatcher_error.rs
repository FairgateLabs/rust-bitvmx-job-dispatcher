use storage_backend::error::StorageError;
use thiserror::Error;
use tracing_subscriber::util::TryInitError;

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

    #[error("Broker error {0}")]
    BrokerError(#[from] bitvmx_broker::rpc::errors::BrokerError),

    #[error("Utils error {0}")]
    UtilsError(#[from] bitvmx_dispatcher_utils::error::UtilsError),

    #[error("Checkpoint path error: {0}")]
    CheckpointPathError(String),

    #[error("Tracing error {0}")]
    TracingError(#[from] TryInitError),
}
