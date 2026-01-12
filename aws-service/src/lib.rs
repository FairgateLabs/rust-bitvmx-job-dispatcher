mod dispatcher_error;
mod dispatcher_job;
mod dispatcher_module;

use aws_config::{BehaviorVersion, meta::region::RegionProviderChain};
use aws_sdk_ec2::{Client as Ec2Client, Error as EC2Error};
use bitvmx_broker::{channel::channel::DualChannel, identification::identifier::Identifier};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tracing::{debug, error, info};

use crate::{
    dispatcher_job::ResultMessage,
    dispatcher_module::{Dispatcher, JobContext},
};

pub fn process_msg(dispatcher: &mut Dispatcher, msg: &str) -> Option<JobContext> {
    info!("Received: {:?}", msg);
    let job_context = dispatcher.process_msg(msg).ok()?;
    Some(job_context)
}

pub struct DispatcherHandler {
    channel: DualChannel,
    ready_instance_ids: HashMap<String, (bool, Option<JobContext>, Option<Identifier>)>,
    pending_jobs: Vec<(Identifier, JobContext)>,
    dispatcher: Dispatcher,
}

impl DispatcherHandler {
    pub async fn new(channel: DualChannel, config_path: String) -> Self {
        let dispatcher = Dispatcher::new();
        let instance_ids = load_config(config_path);
        if instance_ids.is_empty() {
            panic!("No instance IDs provided in the config file");
        }
        let mut ready_instance_ids = HashMap::new();
        for instance_id in instance_ids {
            ready_instance_ids.insert(
                instance_id.clone(),
                (true, None, None),
            );
        }

        Self {
            channel,
            ready_instance_ids,
            pending_jobs: Vec::new(),
            dispatcher,
        }
    }

    pub async fn tick(&mut self) {
        let msg = self.channel.recv();
        match msg {
            Ok(msg) => {
                if let Some(msg) = msg {
                    if let Some(context) = process_msg(&mut self.dispatcher, &msg.0) {
                        self.pending_jobs.push((msg.1, context));
                    } else {
                        error!("Error processing message: {:?}", msg);
                    }
                }
            }
            Err(e) => error!("Error: {:?}", e),
        }

        if !self.pending_jobs.is_empty() {
            let ready_instances_ids: Vec<String> = self
                .ready_instance_ids
                .iter()
                .filter_map(|(id, (ready, _, _))| if *ready { Some(id.clone()) } else { None })
                .collect();

            if !ready_instances_ids.is_empty() {
                for ready_instance_id in ready_instances_ids {
                    if let Some((id, context)) = self.pending_jobs.pop() {
                        self.dispatcher
                            .manage_petition(&ready_instance_id, context.clone())
                            .await
                            .expect("Failed to manage petition");
                        self.ready_instance_ids
                            .insert(ready_instance_id.clone(), (false, Some(context), Some(id)));
                    }
                }
            }
        }

        for (instance_id, (ready, context, id)) in self.ready_instance_ids.iter_mut() {
            if !*ready {
                if is_instance_ready(&instance_id).await.unwrap_or(false) {
                    *ready = true;
                    let job_id = &context.as_ref().unwrap().job_id;
                    if let Some(result) = self.dispatcher.process_result(&job_id)
                    {
                        if let Err(e) = self.channel.send(
                            &id.clone().unwrap(),
                            ResultMessage::new(job_id.clone(), result)
                                .to_string(),
                        ) {
                            error!("Failed to send result: {}", e);
                        }
                    }
                } else {
                    debug!("Instance {} is still not ready", instance_id);
                }
            }
        }
        
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

pub async fn dispatcher_loop(
    channel: DualChannel,
    check_interval: Duration,
    running: Arc<AtomicBool>,
    config_path: String,
) -> Result<(), anyhow::Error> {
    let mut dispacher_handler = DispatcherHandler::new(channel, config_path).await;

    while running.load(Ordering::SeqCst) {
        dispacher_handler.tick().await;
        tokio::time::sleep(check_interval).await;
    }

    Ok(())
}

fn load_config(config_path: String) -> Vec<String> {
    let file = std::fs::File::open(config_path).expect("Could not open config file");
    let reader = std::io::BufReader::new(file);
    let config: serde_json::Value =
        serde_json::from_reader(reader).expect("Could not parse config file");

    if let Some(instance_ids) = config.get("instance_ids") {
        instance_ids
            .as_array()
            .expect("instance_ids should be an array")
            .iter()
            .filter_map(|id| id.as_str().map(String::from))
            .collect()
    } else {
        panic!("No instance_ids found in the config file");
    }
}

async fn create_service() -> Result<Ec2Client, EC2Error> {
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-2");
    let behavior = BehaviorVersion::latest();
    let config = aws_config::defaults(behavior)
        .region(region_provider)
        .load()
        .await;
    let client = Ec2Client::new(&config);

    Ok(client)
}

async fn is_instance_ready(
    instance_id: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let ec2 = create_service().await?;
    println!("Checking if instance {} is stopped...", instance_id);
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
                println!("Instance is stopped, ready to run command");
                return Ok(true);
            } else if name == "shutting-down" || name == "terminated" {
                println!("Instance is shutting down or terminated, cannot run command");
                return Ok(false);
            } else {
                println!("Instance is not stopped yet, current state: {name}");
                return Ok(false);
            }
        }

        None => {
            println!("Instance state is unknown");
            return Ok(false);
        }
    }
}
