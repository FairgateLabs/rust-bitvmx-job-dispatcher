use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_ec2::{
    Client as Ec2Client,
    types::{IamInstanceProfileSpecification, InstanceType, ResourceType, Tag, TagSpecification},
};
use aws_sdk_s3::{Client as S3Client, config::Credentials};
use aws_sdk_ssm::Client as SsmClient;
use tracing::{error, info};

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

    pub fn create_clients(&self) -> (Ec2Client, SsmClient, S3Client) {
        let ec2_client = Ec2Client::new(&self.aws_config);
        let ssm_client = SsmClient::new(&self.aws_config);
        let s3_client = S3Client::new(&self.aws_config);

        (ec2_client, ssm_client, s3_client)
    }

    pub fn create_instance(&self, name: &str) -> Result<String, AwsDispatcherError> {
        let (ec2_client, ..) = self.create_clients();

        let instance_type = InstanceType::from(self.config.ec2.instance_type.as_str());
        let run_response = self
            .runtime
            .block_on(
                ec2_client
                    .run_instances()
                    .image_id(self.config.ec2.image_id.clone())
                    .instance_type(instance_type)
                    .iam_instance_profile(
                        IamInstanceProfileSpecification::builder()
                            .arn(&self.config.ec2.instance_profile_arn)
                            .build(),
                    )
                    .tag_specifications(
                        TagSpecification::builder()
                            .resource_type(ResourceType::Instance)
                            .tags(Tag::builder().key("Name").value(name).build())
                            .build(),
                    )
                    .min_count(1)
                    .max_count(1)
                    .send(),
            )
            .map_err(|e| AwsDispatcherError::Ec2Error(e.into()))?;

        let instances = run_response.instances();

        let instance_id = instances[0].instance_id().ok_or_else(|| {
            error!("No instance ID returned from run_instances response");
            AwsDispatcherError::InstanceLaunchFailed
        })?;

        info!("Launched instance: {:?}", instance_id);
        Ok(instance_id.to_string())
    }

    pub fn terminate_instance(&self, instance_id: &str) -> Result<(), AwsDispatcherError> {
        info!("Terminating instance: {}", instance_id);
        let (ec2_client, ..) = self.create_clients();
        self.runtime
            .block_on(
                ec2_client
                    .terminate_instances()
                    .instance_ids(instance_id)
                    .send(),
            )
            .map_err(|e| AwsDispatcherError::Ec2Error(e.into()))?;

        Ok(())
    }
}
