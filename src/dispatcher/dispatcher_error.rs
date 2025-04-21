use thiserror::Error;

#[derive(Error, Debug)]
pub enum DispatcherError {
    #[error("Serialization error {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Result type mismatch {0}")]
    ResultTypeMismatch(String),

    #[error("Job id already exists")]
    JobIdAlreadyExists,
}
