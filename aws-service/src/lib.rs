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
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, channel},
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
    instances_status:
        HashMap<String, (Option<JobContext>, Option<Identifier>, Option<Receiver<()>>)>,
    max_running_instances: usize,
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
        rt: &MutexGuard<'_, Runtime>,
    ) -> Result<Self, DispatcherError> {
        let mut dispatcher = handle.block_on(Dispatcher::new(config_path.clone()))?;
        let storage = DispatcherStorage::new(storage);
        let max_running_instances = dispatcher.obtain_max_running_instances();
        let mut instances_status = HashMap::new();
        let (restored_pending_jobs, restored_instances_status) = storage.restore_data()?;
        for (instance_id, instance_status) in restored_instances_status {
            let context = instance_status.0.clone().unwrap();
            let id = instance_status.1.clone().unwrap();
            if rt.block_on(dispatcher.check_job_finished(&instance_id, &context))? {
                let job_id = &context.job_id;
                if let Some(result) = dispatcher.process_result(&job_id) {
                    if let Err(e) = message_channel
                        .send(&id, ResultMessage::new(job_id.clone(), result).to_string()?)
                    {
                        error!("Failed to send result: {}", e);
                    }
                }
                storage.delete_instance_status(&instance_id)?;
            } else {
                let new_instance_id = handle.block_on(dispatcher.obtain_new_instance())?;
                let (tx, rx) = channel::<()>();
                let old_instance_id = instance_id.clone();
                let new_instance_id_cloned = new_instance_id.clone();
                let mut dispatcher_clone = dispatcher.clone();
                let restored_context = instance_status.0.clone().unwrap();
                dispatcher.add_job(restored_context.clone())?;
                handle.spawn(async move {
                    if let Err(e) = dispatcher_clone
                        .restart_petition(
                            &old_instance_id,
                            &new_instance_id_cloned,
                            restored_context,
                            tx,
                        )
                        .await
                    {
                        error!("Error processing {}: {:?}", old_instance_id, e);
                    }
                });
                storage.replace_instance_id(&instance_id, &new_instance_id)?;
                let new_instance_status = (
                    instance_status.0.clone(),
                    instance_status.1.clone(),
                    Some(rx),
                );
                instances_status.insert(new_instance_id.clone(), new_instance_status);
            }
        }

        Ok(Self {
            channel: message_channel,
            instances_status,
            pending_jobs: restored_pending_jobs,
            max_running_instances,
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
            let running_instances = self.instances_status.iter().count();

            if running_instances <= self.max_running_instances {
                if let Some((id, context)) = self.pending_jobs.pop() {
                    let instance_id = self
                        .handle
                        .block_on(self.dispatcher.obtain_new_instance())?;
                    self.storage.update_instance_status(&instance_id, &id)?;
                    let (tx, rx) = channel::<()>();
                    self.instances_status.insert(
                        instance_id.clone(),
                        (Some(context.clone()), Some(id), Some(rx)),
                    );
                    let dispatcher = self.dispatcher.clone();
                    self.handle.spawn(async move {
                        if let Err(e) = dispatcher.manage_petition(&instance_id, context, tx).await
                        {
                            error!("Error processing {}: {:?}", instance_id, e);
                        }
                    });
                }
            } else {
                debug!(
                    "Max running instances reached ({}). Pending jobs will be processed later.",
                    self.max_running_instances
                );
            }
        }

        let mut ready_instances: Vec<String> = Vec::new();

        for (instance_id, (context, id, rx)) in self.instances_status.iter() {
            let ready = rx.as_ref().unwrap().try_recv().is_ok();
            if ready {
                debug!("Instance {} is ready", instance_id);
                let job_id = &context.as_ref().unwrap().job_id;
                if let Some(result) = self.dispatcher.process_result(&job_id) {
                    debug!("Processed result for job ID: {}", job_id);
                    if let Err(e) = self.channel.send(
                        &id.clone().unwrap(),
                        ResultMessage::new(job_id.clone(), result).to_string()?,
                    ) {
                        error!("Failed to send result: {}", e);
                    }
                } else {
                    warn!("No result found for job ID: {}", job_id);
                }
                self.storage.delete_instance_status(instance_id)?;
                ready_instances.push(instance_id.clone());
            } else {
                debug!("Instance {} is still not ready", instance_id);
            }
        }

        for instance_id in ready_instances {
            self.instances_status.remove(&instance_id);
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
        DispatcherHandler::new(channel, config_path, handle, storage, &runtime)?;

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
