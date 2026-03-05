use thiserror::Error;

#[derive(Error, Debug)]
pub enum UtilsError {
    #[error("Failed to parse message")]
    ParseError,
}
