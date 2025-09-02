pub mod dispatcher_error;
pub mod dispatcher_job;
pub mod dispatcher_message;
pub mod dispatcher_module;
pub mod dispatcher_storage;
pub mod helper;

use std::{
    fs,
    process::Child,
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
use tracing::{error, info};

use crate::helper::{process_msg, Msg};

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
    pub fn new(channel: DualChannel, storage_path: String) -> Self {
        let mut dispatcher = Dispatcher::<T>::new();

        let config = StorageConfig::new(storage_path, None);
        let dispatcher_backend = Storage::new(&config).unwrap();
        let dispatcher_backend = Arc::new(Mutex::new(dispatcher_backend));

        let storage = Arc::new(Mutex::new(DispatcherStorage::new(dispatcher_backend)));
        let workers = storage.lock().unwrap().restore_jobs(&mut dispatcher);

        Self {
            channel,
            workers,
            dispatcher,
            storage,
        }
    }

    pub fn tick(&mut self) -> bool {
        let mut job_completed = false;

        if !self.workers.is_empty() {
            self.workers
                .retain_mut(|(child, id, context)| match child.try_wait() {
                    Ok(Some(status)) => {
                        job_completed = true;
                        match fs::read_to_string(&context.command_file) {
                            Ok(buf) => {
                                info!("Worker output from file: {}", buf);
                                info!("Worker exited with status: {:?}", status);

                                if let Some(result) =
                                    self.dispatcher.process_result(&context.job_id, buf, status)
                                {
                                    if let Err(e) = self.channel.send(
                                        id.clone(),
                                        ResultMessage::new(context.job_id.clone(), result)
                                            .to_string(),
                                    ) {
                                        error!("Failed to send result: {}", e);
                                    }
                                }
                                self.storage.lock().unwrap().remove_job(&context.job_id);
                                false
                            }
                            Err(e) => {
                                error!("Failed to read file {}: {}", context.command_file, e);
                                if let Err(e) = self
                                    .channel
                                    .send(id.clone(), "Failed to read file".to_string())
                                {
                                    error!("Failed to send error message: {}", e);
                                }
                                self.storage.lock().unwrap().remove_job(&context.job_id);
                                false
                            }
                        }
                    }
                    Ok(None) => true,
                    Err(e) => {
                        job_completed = true;
                        error!("Error checking worker: {}", e);
                        if let Err(e) = self
                            .channel
                            .send(id.clone(), "Error checking worker status".to_string())
                        {
                            error!("Failed to send error message: {}", e);
                        }
                        false
                    }
                });
        }

        let msg = self.channel.recv();
        match msg {
            Ok(msg) => {
                if let Some(msg) = msg {
                    let msg = Msg::from_msg(msg);
                    if let Some((child, context)) =
                        process_msg(&mut self.dispatcher, &msg, Some(self.storage.clone()))
                    {
                        self.workers.push((child, msg.id, context));
                    } else {
                        error!("Error processing message: {:?}", msg.to_string());
                    }
                }
            }
            Err(e) => error!("Error: {:?}", e),
        }

        job_completed
    }
}

pub fn dispatcher_loop<T: DispatcherMessage + DeserializeOwned + std::fmt::Debug>(
    channel: DualChannel,
    check_interval: Duration,
    running: Arc<AtomicBool>,
    storage_path: String,
) -> Result<(), anyhow::Error> {
    let mut dispacher_handler: DispatcherHandler<T> =
        DispatcherHandler::<T>::new(channel, storage_path);

    while running.load(Ordering::SeqCst) {
        dispacher_handler.tick();
        std::thread::sleep(check_interval);
    }

    Ok(())
}
