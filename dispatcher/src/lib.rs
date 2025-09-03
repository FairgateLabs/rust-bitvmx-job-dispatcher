pub mod dispatcher_error;
pub mod dispatcher_job;
pub mod dispatcher_message;
pub mod dispatcher_module;
pub mod dispatcher_storage;
pub mod helper;

use std::{
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

use dispatcher_job::ResultMessage;
use dispatcher_message::DispatcherMessage;
use dispatcher_module::{Dispatcher, JobContext};
use dispatcher_storage::DispatcherStorage;
use serde::de::DeserializeOwned;
use storage_backend::{storage::Storage, storage_config::StorageConfig};
use tracing::info;

use crate::{
    dispatcher_error::DispatcherError,
    helper::{process_msg, Msg},
};

pub struct DispatcherHandler<T: DispatcherMessage + DeserializeOwned> {
    channel: DualChannel,
    workers: Vec<(Child, Identifier, JobContext)>,
    dispatcher: Dispatcher<T>,
    storage: Arc<Mutex<DispatcherStorage>>,
}

impl<T> DispatcherHandler<T>
where
    T: DispatcherMessage + DeserializeOwned,
{
    pub fn new(channel: DualChannel, storage: Rc<Storage>) -> Result<Self, DispatcherError> {
        let mut dispatcher = Dispatcher::<T>::new();

        let storage = Arc::new(Mutex::new(DispatcherStorage::new(storage)));
        let workers = storage
            .lock()
            .map_err(|_| DispatcherError::MutexPoisoned)?
            .restore_jobs(&mut dispatcher)?;

        Ok(Self {
            channel,
            workers,
            dispatcher,
            storage,
        })
    }

    pub fn tick(&mut self) -> Result<bool, DispatcherError> {
        let mut job_completed = false;

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
                                    let _ = self
                                        .channel
                                        .send(id.clone(), "Failed to read file".to_string());
                                    return Err(DispatcherError::IoError(e));
                                }
                            };

                            info!("Worker output from file: {}", buf);
                            info!("Worker exited with status: {:?}", status);

                            let result =
                                self.dispatcher
                                    .process_result(&context.job_id, buf, status)?;

                            self.channel.send(
                                id.clone(),
                                ResultMessage::new(context.job_id.clone(), result).to_string()?,
                            )?;

                            self.storage
                                .lock()
                                .map_err(|_| DispatcherError::MutexPoisoned)?
                                .remove_job(&context.job_id)?;
                            Ok(false)
                        }
                        Ok(None) => Ok(true),
                        Err(e) => {
                            let _ = self
                                .channel
                                .send(id.clone(), "Error checking worker status".to_string());
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

        let msg = self.channel.recv()?;
        if let Some(msg) = msg {
            let msg = Msg::from_msg(msg);
            let (child, context) =
                process_msg(&mut self.dispatcher, &msg, Some(self.storage.clone()))?;
            self.workers.push((child, msg.id, context));
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
    //     let dispatcher_backend = Storage::new(&config)?;
    //     let dispatcher_backend = Arc::new(Mutex::new(dispatcher_backend));

    //     Ok(Arc::new(Mutex::new(DispatcherStorage::new(
    //         dispatcher_backend,
    //     ))))
    Ok(Rc::new(Storage::new(&config)?))
}
