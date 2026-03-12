use crate::dispatcher_error::AwsDispatcherError;
use serde::Deserialize;
use tracing::info;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub aws: AwsConfig,
    pub ec2: Ec2Config,
    pub s3: S3Config,
    pub paths: PathsConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AwsConfig {
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Ec2Config {
    pub image_id: String,
    pub instance_profile_arn: String,
    pub instance_type: String,
    pub max_running_instances: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct S3Config {
    pub bucket: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PathsConfig {
    pub repository_path: String,
}

impl AppConfig {
    pub fn load(path: Option<String>) -> Result<Self, AwsDispatcherError> {
        match path {
            Some(config) => {
                info!("Using configuration: {}", config);
                Ok(bitvmx_settings::settings::load_config_file::<AppConfig>(
                    Some(config),
                )?)
            }
            None => Ok(bitvmx_settings::settings::load::<AppConfig>()?),
        }
    }
}
