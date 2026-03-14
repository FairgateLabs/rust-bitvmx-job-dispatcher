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
    ec2_client: Ec2Client,
    ssm_client: SsmClient,
    s3_client: S3Client,
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

        let (ec2_client, ssm_client, s3_client) = (
            Ec2Client::new(&aws_config),
            SsmClient::new(&aws_config),
            S3Client::new(&aws_config),
        );

        Ok(Self {
            config,
            aws_config,
            runtime,
            ec2_client,
            ssm_client,
            s3_client,
        })
    }

    pub fn create_instance(&self, name: &str) -> Result<String, AwsDispatcherError> {
        let instance_type = InstanceType::from(self.config.ec2.instance_type.as_str());
        let run_response = self
            .runtime
            .block_on(
                self.ec2_client
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
        self.runtime
            .block_on(
                self.ec2_client
                    .terminate_instances()
                    .instance_ids(instance_id)
                    .send(),
            )
            .map_err(|e| AwsDispatcherError::Ec2Error(e.into()))?;

        Ok(())
    }

    pub fn upload_file(&self, key: &str, data: Vec<u8>) -> Result<(), AwsDispatcherError> {
        let bucket = &self.config.s3.bucket;

        self.runtime
            .block_on(
                self.s3_client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(data.into())
                    .send(),
            )
            .map_err(|e| AwsDispatcherError::S3Error(e.into()))?;

        info!("File uploaded to S3: s3://{}/{}", bucket, &key);

        Ok(())
    }

    pub fn download_file(&self, key: &str) -> Result<Vec<u8>, AwsDispatcherError> {
        info!(
            "Downloading file from S3: s3://{}/{}",
            self.config.s3.bucket, key
        );
        let bucket = &self.config.s3.bucket;

        let get_response = self
            .runtime
            .block_on(self.s3_client.get_object().bucket(bucket).key(key).send())
            .map_err(|e| AwsDispatcherError::S3Error(e.into()))?;

        let data = self
            .runtime
            .block_on(get_response.body.collect())
            .map_err(|e| AwsDispatcherError::ByteStreamError(e.into()))?
            .into_bytes()
            .to_vec();

        info!("File downloaded from S3: s3://{}/{}", bucket, &key);

        Ok(data)
    }

    pub fn delete_file(&self, key: &str) -> Result<(), AwsDispatcherError> {
        let bucket = &self.config.s3.bucket;

        self.runtime
            .block_on(
                self.s3_client
                    .delete_object()
                    .bucket(bucket)
                    .key(key)
                    .send(),
            )
            .map_err(|e| AwsDispatcherError::S3Error(e.into()))?;

        info!("File deleted from S3: s3://{}/{}", bucket, &key);

        Ok(())
    }
}
