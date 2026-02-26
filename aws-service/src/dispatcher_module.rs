use crate::{
    config::AppConfig,
    dispatcher_error::DispatcherError,
    dispatcher_job::{DispatcherJob, ProverJobType},
};
use aws_config::{BehaviorVersion, Region};
use aws_sdk_ec2::{
    Client as Ec2Client,
    types::{IamInstanceProfileSpecification, SummaryStatus},
};
use aws_sdk_s3::{Client as S3Client, config::Credentials, types::ChecksumMode};
use aws_sdk_ssm::{
    Client as SsmClient,
    types::{CommandInvocation, CommandInvocationStatus, PingStatus},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs, sync::mpsc::Sender, time::Duration};
use tokio::{
    fs::File,
    io::copy,
    time::{Instant, sleep},
};
use tracing::{debug, error, info};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JobContext {
    pub job_id: String,
    pub job_name: String,
    pub job_args: Vec<String>,
    pub upload_bucket: Vec<(String, Vec<u8>)>,
    pub download_bucket: HashMap<String, String>,
}

impl JobContext {
    pub fn new(
        job_id: String,
        job_name: String,
        job_args: Vec<String>,
        upload_bucket: Vec<(String, Vec<u8>)>,
        download_bucket: HashMap<String, String>,
    ) -> Self {
        Self {
            job_id,
            job_name,
            job_args,
            upload_bucket,
            download_bucket,
        }
    }
}

#[derive(Clone)]
pub struct Dispatcher {
    jobs: HashMap<String, String>,
    config: AppConfig,
}

impl Dispatcher {
    pub fn new(config_path: String) -> Result<Self, DispatcherError> {
        let config = AppConfig::load(Some(config_path))?;
        Ok(Self {
            jobs: HashMap::new(),
            config,
        })
    }

    pub fn process_msg(&mut self, msg: &str) -> Result<JobContext, DispatcherError> {
        let msg: DispatcherJob = serde_json::from_str(msg)?;
        if self.jobs.contains_key(&msg.job_id) {
            error!("Job id already exists: {}", msg.job_id);
            return Err(DispatcherError::JobIdAlreadyExists);
        }

        let job_context = match msg.job_type {
            ProverJobType::Prove(input_value, elf, output_file_path) => {
                let elf = format!("{}/{}", self.config.paths.repository_path, elf);
                let host_bin = format!("{}/target/release/host", self.config.paths.repository_path);

                let output_file =
                    format!("s3://{}/output_{}.json", self.config.s3.bucket, msg.job_id);
                let input_file = format!("s3://{}/input_{}.bin", self.config.s3.bucket, msg.job_id);

                let job_args = vec![
                    format!("aws s3 cp {input_file} /tmp/input.bin"),
                    format!(
                        "{host_bin} prove-stark \
                    --input /tmp/input.bin \
                    --elf {elf} \
                    --output /tmp/stark_proof.bin \
                    --json /tmp/output.json"
                    ),
                    format!(
                        "{host_bin} prove-snark \
                    --input /tmp/stark_proof.bin \
                    --json /tmp/output.json \
                    --json-input /tmp/output.json \
                    > /tmp/upload.log 2>&1"
                    ),
                    format!(
                        "aws s3 cp /tmp/output.json {output_file} --checksum-algorithm SHA256 > /tmp/upload.log 2>&1"
                    ),
                ];

                JobContext::new(
                    msg.job_id.clone(),
                    "Prove".to_string(),
                    job_args,
                    vec![(format!("input_{}.bin", msg.job_id), input_value)],
                    HashMap::from([(format!("output_{}.json", msg.job_id), output_file_path)]),
                )
            }
        };

        self.add_job(job_context.clone())?;

        Ok(job_context)
    }

    pub fn add_job(&mut self, context: JobContext) -> Result<(), DispatcherError> {
         let output_file_path = match context
            .download_bucket
            .get(&format!("output_{}.json", context.job_id))
        {
            Some(path) => path.clone(),
            None => {
                error!("Output path not found for job ID: {}", context.job_id);
                return Err(DispatcherError::OutputPathNotFound);
            }
        };

        self.jobs.insert(context.job_id.clone(), output_file_path);
        Ok(())
    }

    pub fn discard_job(&mut self, id: &str) -> Option<String> {
        self.jobs.remove(id)
    }

    pub fn process_result(&mut self, id: &str) -> Option<String> {
        if let Some(output_file_path) = self.discard_job(id) {
            match fs::read_to_string(&output_file_path) {
                Ok(buf) => {
                    info!("Worker output from file: {}", buf);
                    match Self::extract_structured_json("ProveResult", &buf) {
                        Some(result) => return Some(result),
                        None => {
                            error!(
                                "Unexpected result format in output file {}",
                                output_file_path
                            );
                            return None;
                        }
                    }
                }
                Err(e) => {
                    error!("Error reading output file {}: {:?}", output_file_path, e);
                    return None;
                }
            }
        } else {
            error!("No output found for job ID: {}", id);
        }
        None
    }

    fn extract_structured_json(expected_type: &str, result: &str) -> Option<String> {
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(result) {
            if parsed.get("type") == Some(&serde_json::Value::String(expected_type.to_string())) {
                return Some(result.to_string());
            }
        }
        None
    }

    pub async fn obtain_new_instance(&self) -> Result<String, DispatcherError> {
        let (ec2_client, ..) = self.create_service().await;
        let run_response = ec2_client
            .run_instances()
            .image_id(self.config.ec2.image_id.clone())
            .instance_type(aws_sdk_ec2::types::InstanceType::T3Xlarge)
            .iam_instance_profile(
                IamInstanceProfileSpecification::builder()
                    .arn(&self.config.ec2.instance_profile_arn)
                    .build(),
            )
            .min_count(1)
            .max_count(1)
            .send()
            .await
            .map_err(|e| DispatcherError::Ec2Error(e.into()))?;

        let instance_id = run_response
            .instances()
            .first()
            .unwrap()
            .instance_id()
            .unwrap();

        Ok(instance_id.to_string())
    }

    pub async fn check_job_finished(
        &self,
        instance_id: &str,
        context: &JobContext,
    ) -> Result<bool, DispatcherError> {
        let (_, ssm_client, s3_client) = self.create_service().await;
        if !self
            .command_runned_finished_succesfully(instance_id, ssm_client)
            .await?
        {
            return Ok(false);
        }
        match self.download_file(&s3_client, context).await {
            Ok(_) => Ok(true),
            Err(DispatcherError::CorruptedS3File) => {
                error!(
                    "Output file for job ID {} is corrupted, treating as not finished",
                    context.job_id
                );
                Ok(false)
            }
            Err(e) => match e {
                DispatcherError::S3Error(aws_sdk_s3::Error::NotFound(_)) => {
                    info!(
                        "Output file for job ID {} not found in S3, treating as not finished",
                        context.job_id
                    );
                    return Ok(false);
                }
                _ => {
                    error!(
                        "Error checking job finished for job ID {}: {:?}",
                        context.job_id, e
                    );
                    return Err(e);
                }
            },
        }
    }

    pub async fn restart_petition(
        &mut self,
        old_instance_id: &str,
        new_instance_id: &str,
        context: JobContext,
        tx: Sender<()>,
    ) -> Result<(), DispatcherError> {
        info!(
            "Restarting petition for job ID: {} on instance ID: {}",
            context.job_id, old_instance_id
        );

        let (ec2_client, ..) = self.create_service().await;
        self.terminate_instance(&ec2_client, old_instance_id)
            .await?;
        info!("Send termination message to old instance, sending task to new instance");
        self.manage_petition(new_instance_id, context, tx).await
    }

    pub async fn manage_petition(
        &self,
        instance_id: &str,
        context: JobContext,
        tx: Sender<()>,
    ) -> Result<(), DispatcherError> {
        info!(
            "Managing petition for job ID: {} on instance ID: {}",
            context.job_id, instance_id
        );

        let (ec2_client, ssm_client, s3_client) = self.create_service().await;

        self.upload_file(&s3_client, &context).await?;

        info!(
            "Checking if instance {} is able to run the command",
            instance_id
        );
        self.wait_for_instance_to_be_able_to_run_command(&ec2_client, &ssm_client, instance_id)
            .await?;
        info!("Instance {} is able to run the command", instance_id);

        info!("Sending command to instance {}", instance_id);
        let command_id = self
            .send_command(&ssm_client, instance_id, &context)
            .await?;
        info!("Command sent to instance {}", instance_id);

        self.wait_for_command_completion(&ssm_client, instance_id, &command_id)
            .await?;

        self.download_file(&s3_client, &context).await?;
        info!("File downloaded");

        info!("Terminating instance {}", instance_id);
        self.terminate_instance(&ec2_client, instance_id).await?;
        info!("Instance terminated");

        tx.send(())?;
        info!("Sent completion signal for job ID: {} on instance ID: {}", context.job_id, instance_id);
        Ok(())
    }

    async fn upload_file(
        &self,
        client: &S3Client,
        context: &JobContext,
    ) -> Result<(), DispatcherError> {
        for (file_name, data) in &context.upload_bucket {
            let bucket = &self.config.s3.bucket;

            client
                .put_object()
                .bucket(bucket)
                .key(file_name)
                .body(data.clone().into())
                .send()
                .await
                .map_err(|e| DispatcherError::S3Error(e.into()))?;

            info!("File uploaded to S3: s3://{}/{}", bucket, file_name);
        }

        Ok(())
    }
    async fn wait_for_instance_to_be_able_to_run_command(
        &self,
        client: &Ec2Client,
        ssm_client: &SsmClient,
        instance_id: &str,
    ) -> Result<(), DispatcherError> {
        loop {
            let resp = client
                .describe_instances()
                .instance_ids(instance_id)
                .send()
                .await
                .map_err(|e| DispatcherError::Ec2Error(e.into()))?;

            let state = resp
                .reservations()
                .first()
                .and_then(|r| r.instances().first())
                .and_then(|i| i.state())
                .and_then(|s| s.name())
                .map(|s| s.as_str())
                .unwrap_or("unknown");

            match state {
                "running" => break,
                "shutting-down" | "stopped" | "stopping" | "terminated" => {
                    error!("Instance {} is in state {}", instance_id, state);
                    return Err(DispatcherError::InstanceNotRunning);
                }
                _ => debug!("Instance state is {}, waiting for 'running'...", state),
            }

            sleep(Duration::from_secs(1)).await;
        }

        info!("Instance is running, checking system and instance status...");

        loop {
            let resp = client
                .describe_instance_status()
                .instance_ids(instance_id)
                .include_all_instances(true)
                .send()
                .await
                .map_err(|e| DispatcherError::Ec2Error(e.into()))?;

            if let Some(status) = resp.instance_statuses().first() {
                let sys_ok = match status.system_status().and_then(|s| s.status()) {
                    Some(&SummaryStatus::Ok) => true,
                    Some(&SummaryStatus::Impaired)
                    | Some(&SummaryStatus::InsufficientData)
                    | Some(&SummaryStatus::NotApplicable) => {
                        error!("Invalid System Status for instance {}", instance_id);
                        return Err(DispatcherError::InvalidStatus("System".into()));
                    }
                    _ => false,
                };

                let inst_ok = match status.instance_status().and_then(|s| s.status()) {
                    Some(&SummaryStatus::Ok) => true,
                    Some(&SummaryStatus::Impaired)
                    | Some(&SummaryStatus::InsufficientData)
                    | Some(&SummaryStatus::NotApplicable) => {
                        error!("Invalid Instance Status for instance {}", instance_id);
                        return Err(DispatcherError::InvalidStatus("Instance".into()));
                    }
                    _ => false,
                };

                if sys_ok && inst_ok {
                    break;
                }

                debug!("Instance status is not ok yet, waiting...");
            }

            sleep(Duration::from_secs(1)).await;
        }

        info!("Instance Status is ok, checking SSM status...");

        loop {
            let resp = ssm_client
                .describe_instance_information()
                .send()
                .await
                .map_err(|e| DispatcherError::SsmError(e.into()))?;

            for instance_info in resp.instance_information_list().iter() {
                if instance_info.instance_id() == Some(instance_id) {
                    match instance_info.ping_status() {
                        Some(&PingStatus::Online) => {
                            info!("SSM status is online");
                            return Ok(());
                        }
                        Some(&PingStatus::ConnectionLost) => {
                            error!("Connection lost for instance {}", instance_id);
                            return Err(DispatcherError::InvalidStatus("SSM".into()));
                        }
                        _ => {}
                    }
                }
            }
            debug!("SSM status is not online yet, waiting...");
            sleep(Duration::from_secs(1)).await;
        }
    }

    async fn create_service(&self) -> (Ec2Client, SsmClient, S3Client) {
        let creds = Credentials::new(
            self.config.aws.access_key_id.clone(),
            self.config.aws.secret_access_key.clone(),
            None,
            None,
            "static-loaded-from-config",
        );

        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(self.config.aws.region.clone()))
            .credentials_provider(creds)
            .load()
            .await;

        let client = Ec2Client::new(&config);
        let ssm_client = SsmClient::new(&config);
        let s3_client = S3Client::new(&config);

        (client, ssm_client, s3_client)
    }

    async fn terminate_instance(
        &self,
        client: &Ec2Client,
        instance_id: &str,
    ) -> Result<(), DispatcherError> {
        client
            .terminate_instances()
            .instance_ids(instance_id)
            .send()
            .await
            .map_err(|e| DispatcherError::Ec2Error(e.into()))?;

        Ok(())
    }

    async fn send_command(
        &self,
        client: &SsmClient,
        instance_id: &str,
        context: &JobContext,
    ) -> Result<String, DispatcherError> {
        let command = client
            .send_command()
            .instance_ids(instance_id)
            .document_name("AWS-RunShellScript")
            .parameters("commands", context.job_args.clone())
            .send()
            .await
            .map_err(|e| DispatcherError::SsmError(e.into()))?;

        let command_id = command
            .command()
            .expect("No command received")
            .command_id()
            .expect("No command_id received");

        info!("Command sent. ID: {}", command_id);

        Ok(command_id.to_string())
    }

    async fn wait_for_command_completion(
        &self,
        client: &SsmClient,
        instance_id: &str,
        command_id: &str,
    ) -> Result<(), DispatcherError> {
        let time = Instant::now();
        loop {
            let inv = client
                .get_command_invocation()
                .command_id(command_id)
                .instance_id(instance_id)
                .send()
                .await
                .map_err(|e| DispatcherError::SsmError(e.into()))?;

            match inv.status() {
                Some(status) => match status {
                    CommandInvocationStatus::Success => {
                        info!(
                            "Command execution succeeded, duration: {:?}",
                            time.elapsed()
                        );
                        break;
                    }
                    CommandInvocationStatus::InProgress | CommandInvocationStatus::Pending => {
                        info!(
                            "Command is still in progress, time passed: {:?}",
                            time.elapsed()
                        );
                    }
                    _ => {
                        error!("Command execution failed with status: {:?}", status);
                        return Err(DispatcherError::CommandExecutionFailed);
                    }
                },
                None => {
                    error!("No status received for command invocation");
                    return Err(DispatcherError::CommandExecutionFailed);
                }
            }

            sleep(Duration::from_secs(5)).await;
        }

        Ok(())
    }
    async fn download_file(
        &self,
        client: &S3Client,
        context: &JobContext,
    ) -> Result<(), DispatcherError> {
        for (file_name, _) in &context.download_bucket {
            let bucket = &self.config.s3.bucket;
            let resp = client
                .head_object()
                .bucket(bucket)
                .key(file_name)
                .checksum_mode(ChecksumMode::Enabled)
                .send()
                .await
                .map_err(|e| DispatcherError::S3Error(e.into()))?;

            if resp.checksum_sha256().is_none() {
                info!(
                    "Output file {} does not have a checksum, treating as corrupted",
                    file_name
                );
                return Err(DispatcherError::CorruptedS3File);
            }
        }

        for (file_name, file_local_path) in &context.download_bucket {
            let bucket = &self.config.s3.bucket;
            let resp = client
                .get_object()
                .bucket(bucket)
                .key(file_name)
                .send()
                .await
                .map_err(|e| DispatcherError::S3Error(e.into()))?;

            let mut file = File::create(file_local_path).await?;
            let mut body = resp.body.into_async_read();
            copy(&mut body, &mut file).await?;

            info!("File downloaded from S3: {}", file_local_path);
        }

        Ok(())
    }

    async fn command_runned_finished_succesfully(
        &self,
        instance_id: &str,
        ssm_client: SsmClient,
    ) -> Result<bool, DispatcherError> {
        let resp = ssm_client
            .list_command_invocations()
            .details(true)
            .send()
            .await
            .map_err(|e| DispatcherError::SsmError(e.into()))?;

        let mut invocations: Vec<&CommandInvocation> = resp
            .command_invocations()
            .iter()
            .filter(|inv| inv.instance_id() == Some(instance_id))
            .collect();

        invocations.sort_by_key(|inv| inv.requested_date_time());
        invocations.reverse();

        if let Some(last) = invocations.first() {
            match last.status() {
                Some(status) => match status {
                    CommandInvocationStatus::Success => {
                        info!(
                            "Last command invocation for instance {} succeeded, checking output file",
                            instance_id
                        );
                        return Ok(true);
                    }
                    CommandInvocationStatus::InProgress | CommandInvocationStatus::Pending => {
                        info!(
                            "Last command invocation for instance {} is still in progress, treating as not finished",
                            instance_id
                        );
                        return Ok(false);
                    }
                    _ => {
                        error!(
                            "Last command invocation for instance {} failed with status: {:?}",
                            instance_id, status
                        );
                        return Ok(false);
                    }
                },
                None => {
                    error!(
                        "No status received for last command invocation of instance {}",
                        instance_id
                    );
                    return Ok(false);
                }
            }
        } else {
            info!(
                "No command invocations found for instance {}, treating as not finished",
                instance_id
            );
            return Ok(false);
        }
    }

    pub fn obtain_max_running_instances(&self) -> Result<usize, DispatcherError> {
        Ok(self.config.ec2.max_running_instances)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_config::{BehaviorVersion, meta::region::RegionProviderChain};
    use aws_sdk_ec2::Client as Ec2Client;
    use tracing_subscriber::{
        EnvFilter, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt,
    };

    #[tokio::test]
    async fn test_connects_to_aws_ec2() {
        init_trace().unwrap();

        let region_provider = RegionProviderChain::default_provider().or_else("us-east-2");

        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .load()
            .await;

        let client = Ec2Client::new(&config);

        let resp = client
            .describe_regions()
            .send()
            .await
            .expect("Failed to connect to EC2");

        let regions = resp.regions();

        assert!(!regions.is_empty(), "No regions returned from EC2");

        info!("Connected to AWS EC2. Regions count: {}", regions.len());
    }

    #[tokio::test]
    async fn test_functions() {
        init_trace().unwrap();

        let config_path = format!("{}/config/config.yaml", env!("CARGO_MANIFEST_DIR"));
        let dispatcher = Dispatcher::new(config_path.clone()).unwrap();

        let (ec2_client, ssm_client, s3_client) = dispatcher.create_service().await;

        let instance_id = dispatcher.obtain_new_instance().await.unwrap();

        dispatcher
            .wait_for_instance_to_be_able_to_run_command(&ec2_client, &ssm_client, &instance_id)
            .await
            .unwrap();

        let context = JobContext::new(
            "test_job".to_string(),
            "Test".to_string(),
            vec![
                format!(
                    "aws s3 cp s3://{}/test.bin /tmp/test.bin",
                    dispatcher.config.s3.bucket
                )
                .to_string(),
                "echo 'Hello World' > /tmp/hello.txt".to_string(),
                format!(
                    "aws s3 cp /tmp/hello.txt s3://{}/hello.txt --checksum-algorithm SHA256",
                    dispatcher.config.s3.bucket
                )
                .to_string(),
            ],
            vec![(
                "test.bin".to_string(),
                "Hello World".to_string().as_bytes().to_vec(),
            )],
            HashMap::from([("hello.txt".to_string(), "hello.txt".to_string())]),
        );
        dispatcher.upload_file(&s3_client, &context).await.unwrap();
        info!("File uploaded successfully");

        let command_id = dispatcher
            .send_command(&ssm_client, &instance_id, &context)
            .await
            .unwrap();
        dispatcher
            .wait_for_command_completion(&ssm_client, &instance_id, &command_id)
            .await
            .unwrap();
        info!("Command completed successfully");

        dispatcher
            .download_file(&s3_client, &context)
            .await
            .unwrap();
        assert!(
            std::path::Path::new("hello.txt").exists(),
            "File was not downloaded"
        );
        assert!(
            std::fs::read_to_string("hello.txt").unwrap() == "Hello World\n",
            "File contents are incorrect"
        );
        info!("File downloaded successfully");
        fs::remove_file("hello.txt").unwrap();

        dispatcher
            .terminate_instance(&ec2_client, &instance_id)
            .await
            .unwrap();
        info!("Instance Terminated");
    }

    #[tokio::test]
    async fn test_check_job_finished_for_inexistent_instance_id() {
        init_trace().unwrap();

        let config_path = format!("{}/config/config.yaml", env!("CARGO_MANIFEST_DIR"));
        let dispatcher = Dispatcher::new(config_path.clone()).unwrap();

        let inexistent_instance_id = "i-1234567890abcdef0";
        let test_context = JobContext::new(
            "test_job".to_string(),
            "Test".to_string(),
            vec![],
            vec![],
            HashMap::new(),
        );

        assert!(
            !dispatcher
                .check_job_finished(inexistent_instance_id, &test_context)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_check_job_finished_for_unfinished_job() {
        init_trace().unwrap();

        let config_path = format!("{}/config/config.yaml", env!("CARGO_MANIFEST_DIR"));
        let dispatcher = Dispatcher::new(config_path.clone()).unwrap();
        let (ec2_client, ssm_client, _) = dispatcher.create_service().await;

        let test_context_2 = JobContext::new(
            "test_job".to_string(),
            "Test".to_string(),
            vec!["sleep 35s".to_string()],
            vec![],
            HashMap::new(),
        );

        let instance_id = dispatcher.obtain_new_instance().await.unwrap();

        dispatcher
            .wait_for_instance_to_be_able_to_run_command(&ec2_client, &ssm_client, &instance_id)
            .await
            .unwrap();

        dispatcher
            .send_command(&ssm_client, &instance_id, &test_context_2)
            .await
            .unwrap();

        assert!(
            !dispatcher
                .check_job_finished(&instance_id, &test_context_2)
                .await
                .unwrap()
        );

        dispatcher
            .terminate_instance(&ec2_client, &instance_id)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_check_job_finished_for_corrupt_file() {
        init_trace().unwrap();

        let config_path = format!("{}/config/config.yaml", env!("CARGO_MANIFEST_DIR"));
        let dispatcher = Dispatcher::new(config_path.clone()).unwrap();
        let (ec2_client, ssm_client, _) = dispatcher.create_service().await;

        let instance_id = dispatcher.obtain_new_instance().await.unwrap();
        let test_context = JobContext::new(
            "test_job".to_string(),
            "Test".to_string(),
            vec![
                "echo 'Hello World' > /tmp/hello.txt".to_string(),
                format!(
                    "aws s3 cp /tmp/hello.txt s3://{}/hello.txt", //This is to simulate a corrupt file, No Checksum = File did not upload correctly
                    dispatcher.config.s3.bucket
                )
                .to_string(),
            ],
            vec![],
            HashMap::from([("hello.txt".to_string(), "hello.txt".to_string())]),
        );

        dispatcher
            .wait_for_instance_to_be_able_to_run_command(&ec2_client, &ssm_client, &instance_id)
            .await
            .unwrap();

        let command_id = dispatcher
            .send_command(&ssm_client, &instance_id, &test_context)
            .await
            .unwrap();

        dispatcher
            .wait_for_command_completion(&ssm_client, &instance_id, &command_id)
            .await
            .unwrap();

        assert!(
            !dispatcher
                .check_job_finished(&instance_id, &test_context)
                .await
                .unwrap()
        );

        dispatcher
            .terminate_instance(&ec2_client, &instance_id)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_check_job_finished_successful() {
        init_trace().unwrap();

        let config_path = format!("{}/config/config.yaml", env!("CARGO_MANIFEST_DIR"));
        let dispatcher = Dispatcher::new(config_path.clone()).unwrap();
        let (ec2_client, ssm_client, _) = dispatcher.create_service().await;

        let instance_id = dispatcher.obtain_new_instance().await.unwrap();
        let test_context = JobContext::new(
            "test_job".to_string(),
            "Test".to_string(),
            vec![
                "echo 'Hello World' > /tmp/hello.txt".to_string(),
                format!(
                    "aws s3 cp /tmp/hello.txt s3://{}/successful_test.txt --checksum-algorithm SHA256",
                    dispatcher.config.s3.bucket
                )
                .to_string(),
            ],
            vec![],
            HashMap::from([("successful_test.txt".to_string(), "successful_test.txt".to_string())]),
        );

        dispatcher
            .wait_for_instance_to_be_able_to_run_command(&ec2_client, &ssm_client, &instance_id)
            .await
            .unwrap();

        let command_id = dispatcher
            .send_command(&ssm_client, &instance_id, &test_context)
            .await
            .unwrap();

        dispatcher
            .wait_for_command_completion(&ssm_client, &instance_id, &command_id)
            .await
            .unwrap();

        assert!(
            dispatcher
                .check_job_finished(&instance_id, &test_context)
                .await
                .unwrap()
        );

        fs::remove_file("successful_test.txt").unwrap();

        dispatcher
            .terminate_instance(&ec2_client, &instance_id)
            .await
            .unwrap();
    }

    fn init_trace() -> Result<(), anyhow::Error> {
        let filter = EnvFilter::builder()
            .parse("info,tarpc=off")
            .expect("Invalid filter");

        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().with_span_events(FmtSpan::NEW | FmtSpan::CLOSE))
            .try_init()?;
        Ok(())
    }
}
