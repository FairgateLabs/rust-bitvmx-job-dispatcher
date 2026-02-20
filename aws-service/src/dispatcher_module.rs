use crate::{
    config::AppConfig,
    dispatcher_error::DispatcherError,
    dispatcher_job::{DispatcherJob, ProverJobType},
};
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_ec2::{Client as Ec2Client, types::SummaryStatus};
use aws_sdk_s3::{Client as S3Client, config::Credentials};
use aws_sdk_ssm::{
    Client as SsmClient,
    types::{CommandInvocationStatus, PingStatus},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs, sync::mpsc::Sender, time::Duration};
use tokio::{
    fs::File, io::copy, time::{Instant, sleep}
};
use tracing::{error, info};

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

    pub fn get_instance_ids(&self) -> Vec<String> {
        self.config
            .ec2
            .instance_id
            .split(',')
            .map(|s| s.trim().to_string())
            .collect()
    }

    pub fn process_msg(&mut self, msg: &str) -> Result<JobContext, DispatcherError> {
        let msg: DispatcherJob = serde_json::from_str(msg)?;
        if self.jobs.contains_key(&msg.job_id) {
            error!("Job id already exists: {}", msg.job_id);
            return Err(DispatcherError::JobIdAlreadyExists);
        }

        let job_context = match msg.job_type {
            ProverJobType::Prove(input_value, _elf, output_file_path) => {
                let elf = format!("{}/target/riscv-guest/methods/bitvmx/riscv32im-risc0-zkvm-elf/release/bitvmx.bin", self.config.paths.repository_path); //TODO: Match different types of ELF binaries
                let host_bin = format!("{}/target/release/host", self.config.paths.repository_path);

                let output_file = format!(
                    "s3://{}/output_{}.json",
                    self.config.s3.bucket, msg.job_id
                );
                let input_file = format!(
                    "s3://{}/input_{}.bin",
                    self.config.s3.bucket, msg.job_id
                );

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
                    format!("aws s3 cp /tmp/output.json {output_file} > /tmp/upload.log 2>&1"),
                ];

                JobContext::new(
                    msg.job_id.clone(),
                    "Prove".to_string(),
                    job_args,
                    vec![
                        (format!("input_{}.bin", msg.job_id), input_value),
                    ],
                    HashMap::from([(format!("output_{}.json", msg.job_id), output_file_path)]),
                )
            }
        };

        let output_file_path = match job_context.download_bucket.get(&format!("output_{}.json", msg.job_id)) {
            Some(path) => path.clone(),
            None => {
                error!("Output path not found for job ID: {}", msg.job_id);
                return Err(DispatcherError::OutputPathNotFound);
            }
        };

        self.jobs
            .insert(msg.job_id.clone(), output_file_path);

        Ok(job_context)
    }

    pub fn discard_job(&mut self, id: &str) {
        self.jobs.remove(id);
    }

    pub fn process_result(&mut self, id: &str) -> Option<String> {
        if let Some(output_file_path) = self.jobs.remove(id) {
            match fs::read_to_string(&output_file_path) {
                Ok(buf) => {
                    info!("Worker output from file: {}", buf);
                    match Self::extract_structured_json("ProveResult", &buf) {
                        Some(result) => return Some(result),
                        None => {
                            error!("Unexpected result format in output file {}", output_file_path);
                            return None;
                        }
                    }
                }
                Err(e) => {
                    error!("Error reading output file {}: {:?}", output_file_path, e);
                    return None;
                }
            }
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
        //TODO: Add Deletion of already used files & name them with job_id
        let (ec2_client, config) = self.create_service().await;

        let ssm_client = SsmClient::new(&config);
        let s3_client = S3Client::new(&config);

        info!("Starting instance {}", instance_id);
        self.start_instance(&ec2_client, instance_id).await?;
        info!("Instance started");

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

        info!("Stopping instance {}", instance_id);
        self.stop_instance(&ec2_client, instance_id).await?;
        info!("Instance stopped");

        tx.send(())?;
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
                _ => info!("Instance state is {}, waiting for 'running'...", state),
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

            sleep(Duration::from_secs(1)).await;
        }
    }

    async fn create_service(&self) -> (Ec2Client, SdkConfig) {
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

        (client, config)
    }

    async fn start_instance(
        &self,
        client: &Ec2Client,
        instance_id: &str,
    ) -> Result<(), DispatcherError> {
        //TODO: Start from snapshot
        client
            .start_instances()
            .instance_ids(instance_id)
            .send()
            .await
            .map_err(|e| DispatcherError::Ec2Error(e.into()))?;

        Ok(())
    }

    async fn stop_instance(
        &self,
        client: &Ec2Client,
        instance_id: &str,
    ) -> Result<(), DispatcherError> {
        client
            .stop_instances()
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

        let (ec2_client, config) = dispatcher.create_service().await;

        let ssm_client = SsmClient::new(&config);
        let s3_client = S3Client::new(&config);

        let instance_id = &dispatcher.get_instance_ids()[0]; // TODO: use all instance ids

        info!("Starting instance {}", instance_id);
        dispatcher
            .start_instance(&ec2_client, instance_id)
            .await
            .unwrap();
        info!("Instance started");

        dispatcher
            .wait_for_instance_to_be_able_to_run_command(&ec2_client, &ssm_client, instance_id)
            .await
            .unwrap();

        let context = JobContext::new(
            "test_job".to_string(),
            "Prove".to_string(),
            vec!["echo 'Hello World' > /tmp/hello.txt".to_string()],
            vec![("test.bin".to_string(), vec![0, 1, 2, 3])],
            HashMap::from([("test.bin".to_string(), "test.bin".to_string())]),
        );
        dispatcher.upload_file(&s3_client, &context).await.unwrap();
        info!("File uploaded successfully");

        let command_id = dispatcher.send_command(&ssm_client, instance_id, &context).await.unwrap();
        dispatcher
            .wait_for_command_completion(&ssm_client, instance_id, &command_id)
            .await
            .unwrap();
        info!("Command completed successfully");

        dispatcher.download_file(&s3_client, &context).await.unwrap();
        assert!(std::path::Path::new("test.bin").exists(), "File was not downloaded");
        info!("File downloaded successfully");
        fs::remove_file("test.bin").unwrap();

        dispatcher
            .stop_instance(&ec2_client, instance_id)
            .await
            .unwrap();
        info!("Instance stopped");
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
