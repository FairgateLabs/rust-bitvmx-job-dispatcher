mod config;
mod dispatcher_error;
pub mod dispatcher_job;
mod dispatcher_module;
mod dispatcher_storage;

use bitvmx_broker::{channel::channel::DualChannel, identification::identifier::Identifier};
use dispatcher_utils::{Msg, PingMessage};
use std::{
    collections::HashMap,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering}, mpsc::{Receiver, channel},
    },
    time::Duration,
};
use storage_backend::storage::Storage;
use tokio::runtime::{Handle, Runtime};
use tracing::{debug, error, info, warn};

use crate::{
    dispatcher_error::DispatcherError,
    dispatcher_job::ResultMessage,
    dispatcher_module::{Dispatcher, JobContext},
    dispatcher_storage::DispatcherStorage,
};

pub fn process_msg(dispatcher: &mut Dispatcher, msg: &str) -> Option<JobContext> {
    info!("Received: {:?}", msg);
    let job_context = dispatcher.process_msg(msg).ok()?;
    Some(job_context)
}

pub struct DispatcherHandler {
    channel: DualChannel,
    instances_status: HashMap<String, (bool, Option<JobContext>, Option<Identifier>, Option<Receiver<()>>)>,
    pending_jobs: Vec<(Identifier, JobContext)>,
    dispatcher: Dispatcher,
    storage: DispatcherStorage,
    handle: Handle,
}

impl DispatcherHandler {
    pub fn new(
        message_channel: DualChannel,
        config_path: String,
        handle: Handle,
        storage: Rc<Storage>,
    ) -> Result<Self, DispatcherError> {
        let dispatcher = Dispatcher::new(config_path.clone())?;
        let instance_ids = dispatcher.get_instance_ids();
        if instance_ids.is_empty() {
            return Err(DispatcherError::NoInstanceIds);
        }
        let storage = DispatcherStorage::new(storage);
        let mut instances_status = HashMap::new();
        let (restored_pending_jobs, restored_instances_status) = storage.restore_data()?;
        for instance_id in instance_ids {
            if let Some(instance_status) = restored_instances_status.get(&instance_id).cloned() {
                //TODO: Check with Martin if not restarting if all output in s3
                let (tx, rx) = channel::<()>();
                let value = instance_id.clone();
                let dispatcher_clone = dispatcher.clone();
                let restored_context = instance_status.1.clone().unwrap();
                handle.spawn(async move {
                    if let Err(e) = dispatcher_clone
                        .restart_petition(&value, restored_context, tx)
                        .await
                    {
                        error!("Error processing {}: {:?}", value, e);
                    }
                });

                let instance_status = (
                    instance_status.0,
                    instance_status.1.clone(),
                    instance_status.2.clone(),
                    Some(rx),
                );
                instances_status.insert(instance_id.clone(), instance_status);
            } else {
                instances_status.insert(instance_id.clone(), (true, None, None, None));
            }
        }

        Ok(Self {
            channel: message_channel,
            instances_status,
            pending_jobs: restored_pending_jobs,
            dispatcher,
            handle,
            storage,
        })
    }

    pub fn tick(&mut self) -> Result<(), DispatcherError> {
        let msg = self.channel.recv()?;
        if let Some(msg) = msg {
            let msg = Msg::from_msg(msg.clone());
            if let Some(message) = serde_json::from_str::<PingMessage>(&msg.raw).ok() {
                match message {
                    PingMessage::Ping => info!("Received Ping"),
                    PingMessage::Pong => {
                        warn!("Job Dispatcher should not receive Pong");
                        return Ok(());
                    }
                }

                let pong = serde_json::to_string(&PingMessage::Pong)?;

                self.channel.send(&msg.id, pong)?;
            } else if let Some(context) = process_msg(&mut self.dispatcher, &msg.raw) {
                info!("Dispatching job ID: {}", context.job_id);
                self.storage.save_pending_job(&msg.id, &context)?;
                self.pending_jobs.push((msg.id, context));
            } else {
                error!("Error processing message: {}", msg);
            }
        }

        if !self.pending_jobs.is_empty() {
            let ready_instances_ids: Vec<String> = self
                .instances_status
                .iter()
                .filter_map(|(id, (ready, _, _, _))| if *ready { Some(id.clone()) } else { None })
                .collect();

            info!("Ready instance IDs: {:?}", ready_instances_ids);

            if !ready_instances_ids.is_empty() {
                for ready_instance_id in ready_instances_ids {
                    if let Some((id, context)) = self.pending_jobs.pop() {
                        self.storage
                            .update_instance_status(&ready_instance_id, &id)?;
                        let (tx, rx) = channel::<()>();
                        self.instances_status.insert(
                            ready_instance_id.clone(),
                            (false, Some(context.clone()), Some(id), Some(rx)),
                        );
                        let dispatcher = self.dispatcher.clone();
                        self.handle.spawn(async move {
                            if let Err(e) = dispatcher
                                .manage_petition(&ready_instance_id, context, tx)
                                .await
                            {
                                error!("Error processing {}: {:?}", ready_instance_id, e);
                            }
                        });
                    }
                }
            }
        }

        // TODO: instead of checking in every tick, we can use an event-driven or a timestamp-based approach to check the instance status less frequently
        for (instance_id, (finished, context, id, rx)) in self.instances_status.iter_mut() {
            if !*finished {
                let ready = rx.as_ref().unwrap().try_recv().is_ok();
                if ready {
                    *finished = true;
                    let job_id = &context.as_ref().unwrap().job_id;
                    if let Some(result) = self.dispatcher.process_result(&job_id) {
                        if let Err(e) = self.channel.send(
                            &id.clone().unwrap(),
                            ResultMessage::new(job_id.clone(), result).to_string(),
                        ) {
                            error!("Failed to send result: {}", e);
                        }
                    }
                    self.storage.delete_instance_status(instance_id)?;
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
    storage: Rc<Storage>,
    config_path: String,
) -> Result<(), DispatcherError> {
    info!("Starting dispatcher loop");

    let runtime = rt
        .lock()
        .map_err(|_| DispatcherError::MutexPoisoned("Runtime".to_string()))?;

    let handle = runtime.handle().clone();
    let mut dispatcher_handler =
        DispatcherHandler::new(channel, config_path, handle, storage)?;

    drop(runtime);
    while running.load(Ordering::SeqCst) {
        if let Err(e) = dispatcher_handler.tick() {
            error!("Error occurred in dispatcher tick: {}", e);
            return Err(e);
        }
        std::thread::sleep(check_interval);
    }

    Ok(())
}
