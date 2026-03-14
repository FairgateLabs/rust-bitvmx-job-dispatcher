use std::rc::Rc;

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_s3::config::Credentials;

use crate::{config::AppConfig, errors::AwsDispatcherError};

pub struct AwsHandler {
    config: AppConfig,
    aws_config: SdkConfig,
    runtime: tokio::runtime::Runtime,
}

impl AwsHandler {
    pub fn new(config_path: String) -> Result<Self, AwsDispatcherError> {
        let config = AppConfig::load(Some(config_path))?;
        let creds = Credentials::new(
            config.aws.access_key_id.clone(),
            config.aws.secret_access_key.clone(),
            None,
            None,
            "static-loaded-from-config",
        );

        let runtime = tokio::runtime::Runtime::new()?;

        let aws_config = runtime.block_on(async {
            aws_config::defaults(BehaviorVersion::latest())
                .region(Region::new(config.aws.region.clone()))
                .credentials_provider(creds)
                .load()
                .await
        });

        Ok(Self {
            config,
            aws_config,
            runtime,
        })
    }
}
