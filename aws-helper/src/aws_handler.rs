use aws_config::{BehaviorVersion, Region};
use aws_sdk_ec2::{
    Client as Ec2Client,
    types::{
        IamInstanceProfileSpecification, InstanceStateName, InstanceType, ResourceType,
        SummaryStatus, Tag, TagSpecification,
    },
};
use aws_sdk_s3::{Client as S3Client, config::Credentials};
use aws_sdk_ssm::{Client as SsmClient, types::CommandInvocationStatus};
use tracing::{error, info};

use crate::{config::AppConfig, errors::AwsDispatcherError};

pub struct AwsHandler {
    config: AppConfig,
    runtime: tokio::runtime::Runtime,
    ec2_client: Ec2Client,
    ssm_client: SsmClient,
    s3_client: S3Client,
}

#[derive(Debug)]
pub struct CompleteStatus {
    pub state: InstanceStateName,
    pub system_status: SummaryStatus,
    pub instance_status: SummaryStatus,
}

#[derive(Debug)]
pub enum CommandStatus {
    Success(String),
    InProgress(String),
    Failed(String, String),
    NotFound,
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

    pub fn send_command(
        &self,
        instance_id: &str,
        command: Vec<String>,
    ) -> Result<String, AwsDispatcherError> {
        let command = self
            .runtime
            .block_on(
                self.ssm_client
                    .send_command()
                    .instance_ids(instance_id)
                    .document_name("AWS-RunShellScript")
                    .parameters("commands", command)
                    .send(),
            )
            .map_err(|e| AwsDispatcherError::SsmError(e.into()))?;

        let command_id = command
            .command()
            .and_then(|c| c.command_id())
            .ok_or_else(|| {
                error!("No command ID returned from send_command response");
                AwsDispatcherError::CommandIdNotFound
            })?;

        info!("Command sent. ID: {}", command_id);

        Ok(command_id.to_string())
    }

    pub fn get_command_status(
        &self,
        instance_id: &str,
        command_id: &str,
    ) -> Result<CommandStatus, AwsDispatcherError> {
        let invocation = self
            .runtime
            .block_on(
                self.ssm_client
                    .get_command_invocation()
                    .command_id(command_id)
                    .instance_id(instance_id)
                    .send(),
            )
            .map_err(|e| AwsDispatcherError::SsmError(e.into()))?;

        let status = match invocation.status() {
            Some(status) => match status {
                CommandInvocationStatus::Success => CommandStatus::Success(
                    invocation
                        .execution_elapsed_time()
                        .unwrap_or("")
                        .to_string(),
                ),
                CommandInvocationStatus::InProgress
                | CommandInvocationStatus::Pending
                | CommandInvocationStatus::Delayed => CommandStatus::InProgress(
                    invocation
                        .execution_elapsed_time()
                        .unwrap_or("")
                        .to_string(),
                ),
                _ => CommandStatus::Failed(
                    invocation
                        .execution_elapsed_time()
                        .unwrap_or("")
                        .to_string(),
                    invocation
                        .standard_error_content()
                        .unwrap_or("no-err")
                        .to_string(),
                ),
            },
            None => CommandStatus::NotFound,
        };

        info!("Command status for {}: {:?}", command_id, status);

        Ok(status)
    }

    pub fn get_instance_status(
        &self,
        instance_id: &str,
    ) -> Result<Option<CompleteStatus>, AwsDispatcherError> {
        let describe_response = self
            .runtime
            .block_on(
                self.ec2_client
                    .describe_instance_status()
                    .instance_ids(instance_id)
                    .send(),
            )
            .map_err(|e| AwsDispatcherError::Ec2Error(e.into()))?;

        if describe_response
            .instance_statuses
            .as_ref()
            .is_none_or(|x| x.len() != 1)
        {
            return Ok(None);
        }

        let instance_status = describe_response.instance_statuses.as_ref().unwrap().get(0);

        let state = instance_status
            .and_then(|s| s.instance_state())
            .and_then(|s| s.name())
            .ok_or_else(|| AwsDispatcherError::InstanceNotRunning)?;

        let system_status = instance_status
            .and_then(|s| s.system_status())
            .and_then(|s| s.status())
            .ok_or_else(|| AwsDispatcherError::InstanceNotRunning)?;

        let instance_status = instance_status
            .and_then(|s| s.instance_status())
            .and_then(|s| s.status())
            .ok_or_else(|| AwsDispatcherError::InstanceNotRunning)?;

        Ok(Some(CompleteStatus {
            state: state.to_owned(),
            system_status: system_status.to_owned(),
            instance_status: instance_status.to_owned(),
        }))
    }

    pub fn is_instance_ready(&self, instance_id: &str) -> Result<bool, AwsDispatcherError> {
        let status = self.get_instance_status(instance_id)?;
        info!("Instance status for {}: {:?}", instance_id, status);
        if let Some(status) = status {
            if status.state == InstanceStateName::Running
                && status.system_status == SummaryStatus::Ok
                && status.instance_status == SummaryStatus::Ok
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn wait_for_instance_ready(
        &self,
        instance_id: &str,
        timeout_secs: u64,
    ) -> Result<bool, AwsDispatcherError> {
        let start_time = std::time::Instant::now();
        loop {
            if self.is_instance_ready(instance_id)? {
                return Ok(true);
            }
            if start_time.elapsed().as_secs() >= timeout_secs {
                return Ok(false);
            }
            std::thread::sleep(std::time::Duration::from_secs(5));
        }
    }

    pub fn is_command_finished(
        &self,
        instance_id: &str,
        command_id: &str,
    ) -> Result<bool, AwsDispatcherError> {
        let status = self.get_command_status(instance_id, command_id)?;
        Ok(matches!(
            status,
            CommandStatus::Success(_) | CommandStatus::Failed(_, _)
        ))
    }

    pub fn wait_for_command_finished(
        &self,
        instance_id: &str,
        command_id: &str,
        timeout_secs: u64,
    ) -> Result<CommandStatus, AwsDispatcherError> {
        let start_time = std::time::Instant::now();
        while !self.is_command_finished(instance_id, command_id)? {
            if start_time.elapsed().as_secs() >= timeout_secs {
                return Ok(CommandStatus::Failed(
                    "Timeout".to_string(),
                    "Command did not finish within the timeout".to_string(),
                ));
            }
            std::thread::sleep(std::time::Duration::from_secs(5));
        }
        self.get_command_status(instance_id, command_id)
    }

    pub fn get_max_running_instances(&self) -> usize {
        self.config.ec2.max_running_instances
    }

    pub fn bucket_name(&self) -> &str {
        &self.config.s3.bucket
    }

    pub fn running_path(&self) -> &str {
        &self.config.paths.repository_path
    }
}
