use aws_sdk_s3::primitives::ByteStreamError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AwsDispatcherError {
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

    #[error("Byte stream error {0}")]
    ByteStreamError(#[from] ByteStreamError),

    #[error("Command execution failed")]
    CommandExecutionFailed,

    #[error("Instance not running")]
    InstanceNotRunning,

    #[error("Invalid {0} Status")]
    InvalidStatus(String),

    #[error("Mutex Poisoned: {0}")]
    MutexPoisoned(String),

    #[error("No instance IDs were given")]
    NoInstanceIds,

    #[error("Could not parse {0}")]
    ParseError(String),

    #[error("Pending Job not found")]
    PendingJobNotFound,

    #[error("Failed to load configuration {0}")]
    ConfigLoadFailed(#[from] bitvmx_settings::errors::ConfigError),

    #[error("Output path not found")]
    OutputPathNotFound,

    #[error("Sender error {0}")]
    SenderError(#[from] std::sync::mpsc::SendError<()>),

    #[error("The file uploaded to S3 is corrupted")]
    CorruptedS3File,

    #[error("Instance launch failed")]
    InstanceLaunchFailed,

    #[error("Command ID not found")]
    CommandIdNotFound,

    #[error("Instance timeout")]
    InstanceTimeout,
}
