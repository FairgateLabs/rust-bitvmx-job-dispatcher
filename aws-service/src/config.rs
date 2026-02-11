use serde::Deserialize;
use std::env;
use std::fs;

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
    pub instance_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct S3Config {
    pub bucket: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PathsConfig {
    pub elf_path: String,
    pub host_bin: String,
}

impl AppConfig {
    pub fn load(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let raw = fs::read_to_string(path)?;
        let mut config: AppConfig = serde_yaml::from_str(&raw)?;

        // Resolve environment placeholders
        config.aws.access_key_id = resolve_env(&config.aws.access_key_id)?;
        config.aws.secret_access_key = resolve_env(&config.aws.secret_access_key)?;

        Ok(config)
    }
}

fn resolve_env(value: &str) -> Result<String, Box<dyn std::error::Error>> {
    if value.starts_with("[env:") && value.ends_with("]") {
        let var_name = &value[5..value.len() - 1];
        Ok(env::var(var_name)?)
    } else {
        Ok(value.to_string())
    }
}
