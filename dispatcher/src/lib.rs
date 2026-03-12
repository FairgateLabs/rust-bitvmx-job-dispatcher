pub mod cli;
pub mod dispatcher_error;
pub mod dispatcher_job;
pub mod dispatcher_message;
pub mod dispatcher_module;
pub mod dispatcher_storage;
pub mod helper;

use std::{
    collections::HashMap,
    fs,
    process::Child,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use bitvmx_broker::{channel::channel::DualChannel, identification::identifier::Identifier};

use bitvmx_dispatcher_utils::{Msg, PingMessage};
use dispatcher_job::ResultMessage;
use dispatcher_message::DispatcherMessage;
use dispatcher_module::{process_result, JobContext};
use dispatcher_storage::DispatcherStorage;
use serde::de::DeserializeOwned;
use storage_backend::{storage::Storage, storage_config::StorageConfig};
use tracing::{debug, info, warn};

use crate::{
    dispatcher_error::DispatcherError,
    helper::{persist_job, process_msg},
};

pub struct DispatcherHandler<T: DispatcherMessage + DeserializeOwned> {
    channel: DualChannel,
    workers: Vec<(Child, Identifier, JobContext)>,
    jobs: HashMap<String, T>,
    storage: Arc<Mutex<DispatcherStorage>>,
}

impl<T> DispatcherHandler<T>
where
    T: DispatcherMessage + DeserializeOwned,
{
    pub fn new(channel: DualChannel, storage: Rc<Storage>) -> Result<Self, DispatcherError> {
        let storage = Arc::new(Mutex::new(DispatcherStorage::new(storage)));
        let mut jobs = HashMap::new();

        let workers = storage
            .lock()
            .map_err(|_| DispatcherError::MutexPoisoned)?
            .restore_jobs(&mut jobs)?;

        Ok(Self {
            channel,
            workers,
            jobs,
            storage,
        })
    }

    pub fn new_with_path(
        channel: DualChannel,
        storage_path: &str,
    ) -> Result<Self, DispatcherError> {
        let config = StorageConfig::new(storage_path.to_string(), None);
        let storage = Rc::new(Storage::new(&config)?);
        Self::new(channel, storage)
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
                let (child, context) = process_msg(&mut self.jobs, &msg.raw)?;
                persist_job(&context, &msg, self.storage.clone())?;
                self.workers.push((child, msg.id, context));
            }
        }

        if !self.workers.is_empty() {
            let mut new_workers = Vec::new();

            for (mut child, id, context) in self.workers.drain(..) {
                let keep = (|| -> Result<bool, DispatcherError> {
                    match child.try_wait() {
                        Ok(Some(status)) => {
                            job_completed = true;

                            let buf = match fs::read_to_string(&context.command_file) {
                                Ok(buf) => buf,
                                Err(e) => {
                                    let _ =
                                        self.channel.send(&id, "Failed to read file".to_string());
                                    return Err(DispatcherError::IoError(e));
                                }
                            };

                            info!("Worker output from file: {}", buf);
                            info!("Worker exited with status: {:?}", status);

                            if status.success() {
                                if let Some(job_type) =
                                    self.jobs.get(&context.job_id)
                                {
                                    job_type.commit_checkpoint()?;
                                }
                            }

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
