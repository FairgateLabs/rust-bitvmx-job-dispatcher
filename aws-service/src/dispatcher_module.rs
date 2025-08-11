use crate::{dispatcher_error::DispatcherError, dispatcher_job::{DispatcherJob, ProverJobType}};
use aws_config::{BehaviorVersion, SdkConfig, meta::region::RegionProviderChain};
use aws_sdk_ec2::{Client as Ec2Client, Error as EC2Error};
use aws_sdk_s3::{Client as S3Client, Error as S3Error};
use aws_sdk_ssm::{Client as SsmClient, Error as SsmError};
use std::{collections::HashMap, fs};
use tokio::{fs::File};
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
            ProverJobType::Prove { input_value, elf, command_file } => {
                JobContext::new(msg.job_id.clone(), input_value, elf, command_file)
            },
        };

        self.jobs.insert(msg.job_id.clone(), job_context.command_file.clone());

        Ok(job_context)
    }

    pub fn discard_job(&mut self, id: &str) {
        self.jobs.remove(id);
    }

    pub fn process_result(&mut self, id: &str) -> Option<String> {
        if let Some(command_file) = self.jobs.remove(id){
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
                },
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

    pub async fn manage_petition(&self, instance_id: &str, context: JobContext) -> Result<(), DispatcherError> {
        let (ec2_client, config) = self
            .create_service()
            .await
            .expect("Failed to run the service");

        let client = SsmClient::new(&config);

        debug!("Starting instance {}", instance_id);
        self.start_instance(&ec2_client, instance_id)
            .await
            .expect("Could not start the instance");
        debug!("Instance started");

        self.send_command(&client, instance_id, "".to_string())
            .await
            .expect("Could not send the command");

        self.download_file(&config)
            .await
            .expect("Could not download the file");

        debug!("File downloaded");

        debug!("Stopping instance {}", instance_id);
        self.stop_instance(&ec2_client, instance_id)
            .await
            .expect("Could not stop the instance");
        debug!("Instance stopped");
        Ok(())
    }

    async fn create_service(&self) -> Result<(Ec2Client, SdkConfig), EC2Error> {
        let region_provider = RegionProviderChain::default_provider().or_else("us-east-2");
        let behavior = BehaviorVersion::latest();
        let config = aws_config::defaults(behavior)
            .region(region_provider)
            .load()
            .await;
        let client = Ec2Client::new(&config);

        Ok((client, config))
    }

    async fn start_instance(&self, client: &Ec2Client, instance_id: &str) -> Result<(), EC2Error> {
        client
            .start_instances()
            .instance_ids(instance_id)
            .send()
            .await?;

        Ok(())
    }

    async fn stop_instance(&self, client: &Ec2Client, instance_id: &str) -> Result<(), EC2Error> {
        client
            .stop_instances()
            .instance_ids(instance_id)
            .send()
            .await?;

        Ok(())
    }

    async fn send_command(
        &self,
        client: &SsmClient,
        instance_id: &str,
        zkp_to_run: String,
    ) -> Result<(), SsmError> {
        let command_to_send = "echo 'Hello from Rust SDK' > /tmp/greeting.txt && aws s3 cp /tmp/greeting.txt s3://prueba2025b1/greeting.txt > /tmp/upload.log 2>&1";
        let command = client
            .send_command()
            .instance_ids(instance_id)
            .document_name("AWS-RunShellScript")
            .comment("Create file and upload to S3")
            .parameters("commands", vec![command_to_send.to_string()])
            .send()
            .await?;

        let command_id = command
            .command()
            .expect("No command received")
            .command_id()
            .expect("No command_id received");

        info!("Command sent. ID: {}", command_id);

        Ok(())
    }

    async fn download_file(&self, config: &SdkConfig) -> Result<(), S3Error> {
        let client = S3Client::new(&config);

        let bucket = "prueba2025b1";
        let key = "greeting.txt";
        let resp = client.get_object().bucket(bucket).key(key).send().await?;

        let mut file = File::create("downloaded.txt")
            .await
            .expect("Could not create the file");
        let mut body = resp.body.into_async_read();
        tokio::io::copy(&mut body, &mut file)
            .await
            .expect("Could not copy the data to the file");

        Ok(())
    }

    pub async fn is_instance_stopped(
        &self,
        ec2: &Ec2Client,
        instance_id: &str,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        debug!("Checking if instance {} is stopped...", instance_id);
        let resp = ec2
            .describe_instances()
            .instance_ids(instance_id)
            .send()
            .await?;

        let state = resp
            .reservations()
            .first()
            .unwrap()
            .instances()
            .first()
            .unwrap()
            .state();

        match state {
            Some(s) => {
                let name = s.name().unwrap().as_str();
                if name == "stopped" {
                    debug!("Instance is stopped, ready to run command");
                    return Ok(true);
                } else if name == "shutting-down" || name == "terminated" {
                    debug!("Instance is shutting down or terminated, cannot run command");
                    return Ok(false);
                } else {
                    debug!("Instance is not stopped yet, current state: {name}");
                    return Ok(false);
                }
            }

            None => {
                error!("Instance state is unknown");
                return Ok(false);
            }
        }
    }
}
