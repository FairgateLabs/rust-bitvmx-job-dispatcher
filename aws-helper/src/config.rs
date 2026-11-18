use serde::Deserialize;
use tracing::info;

use crate::errors::AwsDispatcherError;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub aws: AwsConfig,
    pub ec2: Ec2Config,
    pub s3: S3Config,
    pub paths: PathsConfig,
    #[serde(default)]
    pub ssm: SsmConfig,
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
    #[serde(default = "default_instance_ready_timeout_secs")]
    pub instance_ready_timeout_secs: u64,
}

/// Instances take 3 to 4 minutes to be ready.
fn default_instance_ready_timeout_secs() -> u64 {
    360
}

#[derive(Debug, Clone, Deserialize)]
pub struct S3Config {
    pub bucket: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct SsmConfig {
    /// SSM `executionTimeout` in seconds for each job command. When unset SSM applies its default (1 hour).
    pub execution_timeout_secs: Option<u64>,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn yaml(ssm_section: &str) -> String {
        yaml_with_ec2("", ssm_section)
    }

    fn yaml_with_ec2(ec2_extra: &str, ssm_section: &str) -> String {
        format!(
            "aws:\n  region: us-east-1\n  access_key_id: key\n  secret_access_key: secret\n\
             ec2:\n  image_id: ami-1\n  instance_profile_arn: arn\n  max_running_instances: 2\n  instance_type: t3.xlarge\n{ec2_extra}\
             s3:\n  bucket: bucket\n\
             paths:\n  repository_path: /repo\n{ssm_section}"
        )
    }

    fn load(name: &str, content: String) -> AppConfig {
        let path = std::env::temp_dir().join(format!(
            "bitvmx-aws-config-{name}-{}.yaml",
            std::process::id()
        ));
        std::fs::write(&path, content).unwrap();
        let config = AppConfig::load(Some(path.to_string_lossy().to_string())).unwrap();
        std::fs::remove_file(&path).unwrap();
        config
    }

    #[test]
    fn config_without_ssm_section_loads() {
        let config = load("no-ssm", yaml(""));
        assert_eq!(config.ssm.execution_timeout_secs, None);
    }

    #[test]
    fn config_reads_ssm_timeout() {
        let config = load("full", yaml("ssm:\n  execution_timeout_secs: 7200\n"));
        assert_eq!(config.ssm.execution_timeout_secs, Some(7200));
    }

    #[test]
    fn instance_ready_timeout_defaults_to_six_minutes() {
        let config = load("ready-default", yaml(""));
        assert_eq!(config.ec2.instance_ready_timeout_secs, 360);
    }

    #[test]
    fn config_reads_instance_ready_timeout() {
        let config = load(
            "ready-custom",
            yaml_with_ec2("  instance_ready_timeout_secs: 120\n", ""),
        );
        assert_eq!(config.ec2.instance_ready_timeout_secs, 120);
    }
}
