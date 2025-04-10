use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProverError {
    #[error("Serialization error {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Job id already exists")]
    JobIdAlreadyExists,
}
