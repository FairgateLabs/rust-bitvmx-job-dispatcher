pub mod cli;
pub mod dispatcher_error;
pub mod dispatcher_job;
pub mod dispatcher_message;
pub mod dispatcher_module;
pub mod dispatcher_storage;
pub mod helper;

use std::{
    fs,
    process::{Child, Command},
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use bitvmx_broker::{channel::channel::DualChannel, identification::identifier::Identifier};

use bitvmx_dispatcher_utils::{Msg, PingMessage};
use dispatcher_job::ResultMessage;
use dispatcher_message::DispatcherMessage;
use dispatcher_module::{process_result, JobContext};
use dispatcher_storage::DispatcherStorage;
use serde::de::{self, DeserializeOwned};
use storage_backend::{storage::Storage, storage_config::StorageConfig};
use tracing::{debug, error, info, warn};

use crate::{
    dispatcher_error::DispatcherError, dispatcher_job::DispatcherJob, helper::resolve_command_path,
};

pub struct DispatcherHandler<T: DispatcherMessage + DeserializeOwned> {
    channel: DualChannel,
    workers: Vec<(Child, Identifier, JobContext)>,
    //jobs: HashMap<String, T>,
    storage: DispatcherStorage,
    _phantom_data: std::marker::PhantomData<T>,
}

impl<T> DispatcherHandler<T>
where
    T: DispatcherMessage + DeserializeOwned,
{
    pub fn new(channel: DualChannel, storage: Rc<Storage>) -> Result<Self, DispatcherError> {
        let storage = DispatcherStorage::new(storage);
        //let mut jobs = HashMap::new();

        /*let workers = storage
        .lock()
        .map_err(|_| DispatcherError::MutexPoisoned)?
        .restore_jobs(&mut jobs)?;*/

        Ok(Self {
            channel,
            workers: Vec::new(),
            //jobs,
            storage,
            _phantom_data: std::marker::PhantomData,
        })
    }

    pub fn tick(&mut self) -> Result<bool, DispatcherError> {
        let msg = self.channel.recv();
        if msg.is_err() {
            warn!("Failed to receive message from channel: {:?}", msg.err());
            return Ok(false);
        }
        let msg = msg.unwrap();

        let mut job_completed = false;

        if let Some(msg) = msg {
            let msg = Msg::from_msg(msg.clone());
            if let Some(message) = serde_json::from_str::<PingMessage>(&msg.raw).ok() {
                match message {
                    PingMessage::Ping => debug!("Received Ping"),
                    PingMessage::Pong => {
                        warn!("Job Dispatcher should not receive Pong");
                        return Ok(false);
                    }
                }

                let pong = serde_json::to_string(&PingMessage::Pong)?;

                self.channel.send(&msg.id, pong)?;
            } else {
                let job = decode_msg(&msg.raw)?;
                if self.storage.contains_job(&job.job_id)? {
                    warn!("Job with id {} already exists, skipping", job.job_id);
                }
                {
                    let (child, context) = spawn_local_job(&job)?;
                    self.storage.persist_job(&context.job_id, &msg.raw)?;
                }
            }
        }

        if !self.workers.is_empty() {
            let mut new_workers = Vec::new();

            for (mut child, id, context) in self.workers.drain(..) {
                let keep = (|| -> Result<bool, DispatcherError> {
                    match child.try_wait() {
                        Ok(Some(status)) => {
                            job_completed = true;

                            let (result, is_err) = if status.success() {
                                match fs::read_to_string(&context.result_file) {
                                    Ok(buf) => (buf, false),
                                    Err(e) => (
                                        format!(
                                            "Failed to read result file for job {}: {}",
                                            context.job_id, e
                                        )
                                        .to_string(),
                                        true,
                                    ),
                                }
                            } else {
                                (
                                    format!(
                                    "Worker process for job {} exited with non-zero status: {:?}",
                                    context.job_id, status
                                )
                                    .to_string(),
                                    true,
                                )
                            };

                            info!("Worker output from file: {}", result);
                            info!("Worker exited with status: {:?}", status);

                            let job = self.storage.get_job(&context.job_id)?;
                            if job.is_none() {
                                error!(
                                    "Job {} not found in storage, skipping result processing",
                                    context.job_id
                                );
                                return Ok(false);
                            }

                            let job = decode_msg(&job.unwrap())?;
                            let expected_msg_type = job.job_type().message_type();
                            extract_structured_json(&expected_type, &result);

                            let result =
                                process_result(&mut self.jobs, &context.job_id, buf, status)?;

                            let result = self.channel.send(
                                &id,
                                ResultMessage::new(context.job_id.clone(), result).to_string()?,
                            )?;

                            if result {
                                self.storage
                                    .lock()
                                    .map_err(|_| DispatcherError::MutexPoisoned)?
                                    .remove_job(&context.job_id)?;
                            } else {
                                warn!("Failed to send job result to client");
                            }
                            Ok(false)
                        }
                        Ok(None) => Ok(true),
                        Err(e) => {
                            let _ = self
                                .channel
                                .send(&id, "Error checking worker status".to_string());
                            Err(DispatcherError::IoError(e))
                        }
                    }
                })()?;
                if keep {
                    new_workers.push((child, id, context));
                }
            }
            self.workers = new_workers;
        }

        Ok(job_completed)
    }
}

pub fn dispatcher_loop<T: DispatcherMessage + DeserializeOwned + std::fmt::Debug>(
    channel: DualChannel,
    check_interval: Duration,
    running: Arc<AtomicBool>,
    storage: Rc<Storage>,
) -> Result<(), DispatcherError> {
    let mut dispacher_handler: DispatcherHandler<T> =
        DispatcherHandler::<T>::new(channel, storage)?;

    while running.load(Ordering::SeqCst) {
        dispacher_handler.tick()?;
        std::thread::sleep(check_interval);
    }

    Ok(())
}

// Just for testing purposes
pub fn get_storage_with_path(storage_path: &str) -> Result<Rc<Storage>, DispatcherError> {
    let config = StorageConfig::new(storage_path.to_string(), None);
    Ok(Rc::new(Storage::new(&config)?))
}

fn decode_msg<V>(msg: &str) -> Result<DispatcherJob<V>, DispatcherError>
where
    V: DispatcherMessage + DeserializeOwned,
{
    let msg: DispatcherJob<V> = serde_json::from_str(msg)?;
    Ok(msg)
}

fn spawn_local_job<V>(msg: &DispatcherJob<V>) -> Result<(Child, JobContext), DispatcherError>
where
    V: DispatcherMessage + DeserializeOwned,
{
    let (cmd, args, command_file) = msg.job_type.command()?;
    let cmd = resolve_command_path(&cmd)?;
    info!("Command: {:?}", cmd);
    info!("Args: {:?}", args);

    let job_context = JobContext::new(msg.job_id.clone(), command_file.clone());
    let child = Command::new(cmd).args(args).spawn()?;

    Ok((child, job_context))
}
