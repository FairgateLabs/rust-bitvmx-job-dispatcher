use thiserror::Error;

#[derive(Error, Debug)]
pub enum DispatcherError {
    #[error("Serialization error {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Result type mismatch {0}")]
    ResultTypeMismatch(String),

    #[error("Job id already exists")]
    JobIdAlreadyExists,

    #[error("IO error {0}")]
    IoError(#[from] std::io::Error),

    #[error("EC2 error {0}")]
    Ec2Error(#[from] aws_sdk_ec2::Error),

    #[error("SSM error {0}")]
    SsmError(#[from] aws_sdk_ssm::Error),

    #[error("S3 error {0}")]
    S3Error(#[from] aws_sdk_s3::Error),

    #[error("Command execution failed")]
    CommandExecutionFailed,

    #[error("Instance not running")]
    InstanceNotRunning,

    #[error("Invalid {0} Status")]
    InvalidStatus(String),
}
