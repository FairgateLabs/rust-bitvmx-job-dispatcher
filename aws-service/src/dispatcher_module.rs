use crate::{
    dispatcher_error::DispatcherError,
    dispatcher_job::{DispatcherJob, ProverJobType},
};
use aws_config::{BehaviorVersion, SdkConfig, meta::region::RegionProviderChain};
use aws_sdk_ec2::{Client as Ec2Client, types::SummaryStatus};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_ssm::{
    Client as SsmClient,
    types::{CommandInvocationStatus, PingStatus},
};
use std::{collections::HashMap, fs, time::Duration};
use tokio::{
    fs::File,
    time::{Instant, sleep},
};
use tracing::{debug, error, info};

#[derive(Clone)]
pub struct JobContext {
    pub job_id: String,
    pub input_value: Vec<u8>,
    pub elf: String,
    pub command_file: String,
}

impl JobContext {
    pub fn new(job_id: String, input_value: Vec<u8>, elf: String, command_file: String) -> Self {
        Self {
            job_id,
            input_value,
            elf,
            command_file,
        }
    }
}

pub struct Dispatcher {
    jobs: HashMap<String, String>,
}

impl Dispatcher {
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
        }
    }

    pub fn process_msg(&mut self, msg: &str) -> Result<JobContext, DispatcherError> {
        let msg: DispatcherJob = serde_json::from_str(msg)?;
        if self.jobs.contains_key(&msg.job_id) {
            error!("Job id already exists: {}", msg.job_id);
            return Err(DispatcherError::JobIdAlreadyExists);
        }

        let job_context = match msg.job_type {
            ProverJobType::Prove(input_value, elf, command_file) => {
                JobContext::new(msg.job_id.clone(), input_value, elf, command_file)
            }
        };

        self.jobs
            .insert(msg.job_id.clone(), job_context.command_file.clone());

        Ok(job_context)
    }

    pub fn discard_job(&mut self, id: &str) {
        self.jobs.remove(id);
    }

    pub fn process_result(&mut self, id: &str) -> Option<String> {
        if let Some(command_file) = self.jobs.remove(id) {
            match fs::read_to_string(&command_file) {
                Ok(buf) => {
                    info!("Worker output from file: {}", buf);
                    match Self::extract_structured_json("ProveResult", &buf) {
                        Some(result) => return Some(result),
                        None => {
                            error!("Unexpected result format in command file {}", command_file);
                            return None;
                        }
                    }
                }
                Err(e) => {
                    error!("Error reading command file {}: {:?}", command_file, e);
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
    ) -> Result<(), DispatcherError> {
        let (ec2_client, config) = self.create_service().await;

        let ssm_client = SsmClient::new(&config);
        let s3_client = S3Client::new(&config);

        info!("Starting instance {}", instance_id);
        self.start_instance(&ec2_client, instance_id)
            .await
            .expect("Could not start the instance");
        info!("Instance started");

        self.upload_file(&s3_client, &context.input_value).await?;

        info!(
            "Checking if instance {} is able to run the command",
            instance_id
        );
        self.check_instance_status(&ec2_client, &ssm_client, instance_id)
            .await
            .expect("Could not check the instance status");
        info!("Instance {} is able to run the command", instance_id);

        info!("Sending command to instance {}", instance_id);
        let command_id = self
            .send_command(&ssm_client, instance_id)
            .await
            .expect("Could not send the command");
        info!("Command sent to instance {}", instance_id);

        self.wait_for_command_completion(&ssm_client, instance_id, &command_id)
            .await?;

        self.download_file(&s3_client)
            .await
            .expect("Could not download the file");

        info!("File downloaded");

        info!("Stopping instance {}", instance_id);
        self.stop_instance(&ec2_client, instance_id)
            .await
            .expect("Could not stop the instance");
        info!("Instance stopped");
        Ok(())
    }

    async fn upload_file(
        &self,
        client: &S3Client,
        input_value: &Vec<u8>,
    ) -> Result<(), DispatcherError> {
        let bucket = "prueba-zkp";
        let key = "input.bin";

        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(input_value.clone().into())
            .send()
            .await
            .map_err(|e| DispatcherError::S3Error(e.into()))?;

        info!("File uploaded to S3: s3://{}/{}", bucket, key);

        Ok(())
    }
    async fn check_instance_status(
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

            if state == "running" {
                break;
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
                let sys_ok =
                    status.system_status().and_then(|s| s.status()) == Some(&SummaryStatus::Ok);
                let inst_ok =
                    status.instance_status().and_then(|s| s.status()) == Some(&SummaryStatus::Ok);

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

            let online = resp.instance_information_list().iter().any(|i| {
                i.instance_id() == Some(instance_id) && i.ping_status() == Some(&PingStatus::Online)
            });

            if online {
                break;
            }

            sleep(Duration::from_secs(1)).await;
        }

        info!("SSM status is online");

        Ok(())
    }

    async fn create_service(&self) -> (Ec2Client, SdkConfig) {
        let region_provider = RegionProviderChain::default_provider().or_else("us-east-2");
        let behavior = BehaviorVersion::latest();
        let config = aws_config::defaults(behavior)
            .region(region_provider)
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
    ) -> Result<String, DispatcherError> {
        // aws s3 cp s3://prueba-zkp/input.bin" /tmp/input.bin &&
        // ./rust-bitvmx-zk-proof/target/release/host prove-stark \
        //                 --input /tmp/input.bin \
        //                 --elf ./rust-bitvmx-zk-proof/target/riscv-guest/methods/bitvmx/riscv32im-risc0-zkvm-elf/release/bitvmx.bin \
        //                 --output /tmp/stark_proof.bin \
        //                 --json /tmp/output.json \
        //                 && ../rust-bitvmx-zk-proof/target/release/host \
        //                 prove-snark \
        //                 --input /tmp/stark_proof.bin \
        //                 --json /tmp/output.json \
        //                 --json-input /tmp/output.json
        // && aws s3 cp /tmp/output.json s3://prueba-zkp/output.json > /tmp/upload.log 2>&1
        let command_to_send = "echo 'Hello from Rust SDK' > /tmp/output.json && aws s3 cp /tmp/output.json s3://prueba-zkp/output.json > /tmp/upload.log 2>&1";
        let command = client
            .send_command()
            .instance_ids(instance_id)
            .document_name("AWS-RunShellScript")
            .comment("Create file and upload to S3")
            .parameters("commands", vec![command_to_send.to_string()])
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

            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        Ok(())
    }
    async fn download_file(&self, client: &S3Client) -> Result<(), DispatcherError> {
        let bucket = "prueba-zkp";
        let key = "output.json";
        let resp = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| DispatcherError::S3Error(e.into()))?;

        let mut file = File::create("output.json")
            .await
            .expect("Could not create the file");
        let mut body = resp.body.into_async_read();
        tokio::io::copy(&mut body, &mut file)
            .await
            .expect("Could not copy the data to the file");

        Ok(())
    }
}
