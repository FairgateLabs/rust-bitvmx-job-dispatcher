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
}

#[derive(Debug, Clone, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    /// Region of the bucket when it differs from `aws.region`.
    pub region: Option<String>,
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

    fn yaml(s3_extra: &str, ssm_section: &str) -> String {
        format!(
            "aws:\n  region: us-east-1\n  access_key_id: key\n  secret_access_key: secret\n\
             ec2:\n  image_id: ami-1\n  instance_profile_arn: arn\n  max_running_instances: 2\n  instance_type: t3.xlarge\n\
             s3:\n  bucket: bucket\n{s3_extra}\
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
    fn config_without_optional_fields_still_loads() {
        let config = load("legacy", yaml("", ""));
        assert_eq!(config.s3.region, None);
        assert_eq!(config.ssm.execution_timeout_secs, None);
    }

    #[test]
    fn config_reads_s3_region_and_ssm_timeout() {
        let config = load(
            "full",
            yaml(
                "  region: us-east-2\n",
                "ssm:\n  execution_timeout_secs: 7200\n",
            ),
        );
        assert_eq!(config.s3.region.as_deref(), Some("us-east-2"));
        assert_eq!(config.ssm.execution_timeout_secs, Some(7200));
    }
}
