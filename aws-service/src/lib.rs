mod dispatcher_error;
pub mod dispatcher_job;
mod dispatcher_module;

use aws_config::{BehaviorVersion, meta::region::RegionProviderChain};
use aws_sdk_ec2::{Client as Ec2Client, types::InstanceStateName};
use bitvmx_broker::{channel::channel::DualChannel, identification::identifier::Identifier};
use utils::{Msg, PingMessage};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::runtime::Runtime;
use tracing::{debug, error, info, warn};

use crate::{
    dispatcher_error::DispatcherError, dispatcher_job::ResultMessage, dispatcher_module::{Dispatcher, JobContext}
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
            ready_instance_ids.insert(instance_id.clone(), (true, None, None));
        }

        Self {
            channel,
            ready_instance_ids,
            pending_jobs: Vec::new(),
            dispatcher,
        }
    }

    pub fn tick(&mut self, rt: Arc<Mutex<Runtime>>) -> Result<(), DispatcherError> {
        let msg = self.channel.recv()?;
        if let Some(msg) = msg {
            let msg = Msg::from_msg(msg.clone());
            if let Some(message) = serde_json::from_str::<PingMessage>(&msg.raw).ok() {
                match message {
                    PingMessage::Ping => debug!("Received Ping"),
                    PingMessage::Pong => {
                        warn!("Job Dispatcher should not receive Pong");
                        return Ok(());
                    }
                }

                let pong = serde_json::to_string(&PingMessage::Pong)?;

                self.channel.send(&msg.id, pong)?;
            } else if let Some(context) = process_msg(&mut self.dispatcher, &msg.raw) {
                info!("Dispatching job ID: {}", context.job_id);
                self.pending_jobs.push((msg.id, context));
            } else {
                error!("Error processing message: {}", msg);
            }
        }

        if !self.pending_jobs.is_empty() {
            let ready_instances_ids: Vec<String> = self
                .ready_instance_ids
                .iter()
                .filter_map(|(id, (ready, _, _))| if *ready { Some(id.clone()) } else { None })
                .collect();

            info!("Ready instance IDs: {:?}", ready_instances_ids);

            if !ready_instances_ids.is_empty() {
                for ready_instance_id in ready_instances_ids {
                    if let Some((id, context)) = self.pending_jobs.pop() {
                        rt.lock()
                            .map_err(|_| DispatcherError::MutexPoisoned("Runtime".to_string()))?
                            .block_on(
                                self.dispatcher
                                    .manage_petition(&ready_instance_id, context.clone()),
                            )?;
                        self.ready_instance_ids
                            .insert(ready_instance_id.clone(), (false, Some(context), Some(id)));
                    }
                }
            }
        }

        let ec2 = rt
                    .lock()
                    .map_err(|_| DispatcherError::MutexPoisoned("Runtime".to_string()))?
                    .block_on(create_service());

        for (instance_id, (ready, context, id)) in self.ready_instance_ids.iter_mut() {
            if !*ready {
                if rt
                    .lock()
                    .map_err(|_| DispatcherError::MutexPoisoned("Runtime".to_string()))?
                    .block_on(is_instance_ready(&instance_id, &ec2))
                    .unwrap_or(false)
                {
                    *ready = true;
                    let job_id = &context.as_ref().unwrap().job_id;
                    if let Some(result) = self.dispatcher.process_result(&job_id) {
                        if let Err(e) = self.channel.send(
                            &id.clone().unwrap(),
                            ResultMessage::new(job_id.clone(), result).to_string(),
                        ) {
                            error!("Failed to send result: {}", e);
                        }
                    }
                } else {
                    debug!("Instance {} is still not ready", instance_id);
                }
            }
        }

        Ok(())
    }
}

pub fn dispatcher_loop(
    channel: DualChannel,
    check_interval: Duration,
    running: Arc<AtomicBool>,
    rt: Arc<Mutex<Runtime>>,
    config_path: String,
) -> Result<(), DispatcherError> {
    let mut dispatcher_handler = rt
        .lock()
        .map_err(|_| DispatcherError::MutexPoisoned("Runtime".to_string()))?
        .block_on(DispatcherHandler::new(channel, config_path));
    info!("Starting dispatcher loop");
    while running.load(Ordering::SeqCst) {
        if let Err(e) = dispatcher_handler.tick(rt.clone()) {
            error!("Error occurred in dispatcher tick: {}", e);
            return Err(e);
        }
        std::thread::sleep(check_interval);
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

async fn create_service() -> Ec2Client {
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-2");
    let behavior = BehaviorVersion::latest();
    let config = aws_config::defaults(behavior)
        .region(region_provider)
        .load()
        .await;
    let client = Ec2Client::new(&config);

    client
}

async fn is_instance_ready(instance_id: &str, ec2: &Ec2Client) -> Result<bool, DispatcherError> {
    info!("Checking if instance {} is stopped...", instance_id);
    let resp = ec2
        .describe_instances()
        .instance_ids(instance_id)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to describe instances: {}", e);
            DispatcherError::Ec2Error(e.into())
        })?;

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
            match s.name(){
                Some(name) => {
                    match name {
                        InstanceStateName::Stopped => {
                            debug!("Instance {} is stopped", instance_id);
                            Ok(true)
                        },
                        InstanceStateName::Running => {
                            debug!("Instance {} is running", instance_id);
                            Ok(false)
                        },
                        _ => {
                            debug!("Instance {} is in state {:?}", instance_id, name);
                            Ok(false)
                        }
                    }
                },
                None => {
                    warn!("Instance state name is unknown");
                    Ok(false)
                }
            }
            
        } None => {
            warn!("Instance state is unknown");
            return Ok(false);
        }
    }
}
