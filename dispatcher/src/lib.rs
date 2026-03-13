pub mod cli;
pub mod dispatcher_error;
pub mod dispatcher_job;
pub mod dispatcher_message;
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
use dispatcher_storage::DispatcherStorage;
use serde::de::DeserializeOwned;
use storage_backend::{storage::Storage, storage_config::StorageConfig};
use tracing::{debug, error, info, warn};

use crate::{
    dispatcher_error::DispatcherError, dispatcher_job::DispatcherJob, helper::resolve_command_path,
};

#[derive(Clone)]
pub struct JobContext {
    pub job_id: String,
    pub result_file: String,
}

impl JobContext {
    pub fn new(job_id: String, result_file: String) -> Self {
        Self {
            job_id,
            result_file,
        }
    }
}
pub struct DispatcherHandler<T: DispatcherMessage + DeserializeOwned> {
    channel: DualChannel,
    workers: Vec<(Child, Identifier, JobContext)>,
    storage: DispatcherStorage,
    _phantom_data: std::marker::PhantomData<T>,
}

impl<T> DispatcherHandler<T>
where
    T: DispatcherMessage + DeserializeOwned,
{
    pub fn new(channel: DualChannel, storage: Rc<Storage>) -> Result<Self, DispatcherError> {
        let storage = DispatcherStorage::new(storage);

        let mut ok = Self {
            channel,
            workers: Vec::new(),
            storage,
            _phantom_data: std::marker::PhantomData,
        };

        ok.restore_jobs()?;

        Ok(ok)
    }

    pub fn new_with_path(
        channel: DualChannel,
        storage_path: &str,
    ) -> Result<Self, DispatcherError> {
        let config = StorageConfig::new(storage_path.to_string(), None);
        let storage = Rc::new(Storage::new(&config)?);
        Self::new(channel, storage)
    }

    pub fn restore_jobs(&mut self) -> Result<(), DispatcherError> {
        let keys = self.storage.list_jobs()?;

        for key in keys {
            let original_msg = self
                .storage
                .get_job(&key)?
                .ok_or(DispatcherError::JobIdNotFound(key.clone()))?;
            info!("Restoring job from key {}: {}", key, &original_msg);
            let msg = Msg::from_string(&original_msg)?;
            let job: DispatcherJob<T> = decode_msg(&msg.raw)?;

            let (child, context) = spawn_local_job(&job)?;
            self.workers.push((child, msg.id.clone(), context));
        }

        Ok(())
    }

    fn create_new_jobs(&mut self) -> Result<(), DispatcherError> {
        let msg = self.channel.recv();
        if msg.is_err() {
            warn!("Failed to receive message from channel: {:?}", msg.err());
            return Ok(());
        }
        let msg = msg.unwrap();

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
            } else {
                let job: DispatcherJob<T> = decode_msg(&msg.raw)?;
                if self.storage.contains_job(&job.job_id())? {
                    warn!("Job with id {} already exists, skipping", job.job_id());
                } else {
                    let (child, context) = spawn_local_job(&job)?;
                    self.storage
                        .persist_job(&context.job_id, &msg.to_string())?;
                    self.workers.push((child, msg.id.clone(), context));
                }
            }
        }
        Ok(())
    }

    fn process_running_jobs(&mut self) -> Result<bool, DispatcherError> {
        let mut job_completed = false;

        if !self.workers.is_empty() {
            let mut new_workers = Vec::new();

            for (mut child, id, context) in self.workers.drain(..) {
                let keep = (|| -> Result<bool, DispatcherError> {
                    match child.try_wait() {
                        Ok(Some(status)) => {
                            job_completed = true;

                            // Read the result from the file if the process exited successfully, otherwise capture the error
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

                            let original_msg = self
                                .storage
                                .get_job(&context.job_id)?
                                .ok_or(DispatcherError::JobIdNotFound(context.job_id.clone()))?;

                            // Process the result and extract structured JSON if possible
                            let (result, is_err) = if !is_err {
                                let msg = Msg::from_string(&original_msg)?;
                                let job: DispatcherJob<T> = decode_msg(&msg.raw)?;
                                let expected_msg_type = job.job_type().message_type();
                                let processed_result =
                                    extract_structured_json(&expected_msg_type, &result);

                                match processed_result {
                                    Ok(res) => {
                                        info!(
                                            "Successfully extracted structured JSON for job {}: {}",
                                            context.job_id, res
                                        );
                                        job.job_type().commit_checkpoint()?;
                                        (res, false)
                                    }
                                    Err(e) => {
                                        error!(
                                            "Failed to extract structured JSON for job {}: {:?}",
                                            context.job_id, e
                                        );
                                        (
                                            format!("Failed to extract structured JSON: {:?}", e),
                                            true,
                                        )
                                    }
                                }
                            } else {
                                (result, true)
                            };

                            // Save the result to be send back
                            let result_message =
                                ResultMessage::new(context.job_id.clone(), result, is_err)
                                    .to_string()?;

                            self.storage
                                .save_result(&context.job_id, (result_message, id.clone()))?;
                            self.storage.remove_job(&context.job_id)?;

                            Ok(false)
                        }
                        Ok(None) => Ok(true),

                        Err(e) => {
                            let result_message = ResultMessage::new(
                                context.job_id.clone(),
                                format!("Error checking worker status: {:?}", e),
                                true,
                            )
                            .to_string()?;
                            self.storage
                                .save_result(&context.job_id, (result_message, id.clone()))?;
                            self.storage.remove_job(&context.job_id)?;
                            Ok(false)
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

    fn send_results(&self) -> Result<(), DispatcherError> {
        let results = self.storage.get_results()?;

        for (job_id, result) in results {
            let attempt_to_send = self.channel.send(&result.1, result.0);
            if attempt_to_send.is_ok_and(|x| x) {
                self.storage.remove_result(&job_id)?;
            } else {
                warn!("Failed to send result for job {}", job_id,);
                continue;
            }
        }

        Ok(())
    }

    pub fn tick(&mut self) -> Result<bool, DispatcherError> {
        self.create_new_jobs()?;
        let job_completed = self.process_running_jobs()?;
        self.send_results()?;
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

fn extract_structured_json(expected_type: &str, result: &str) -> Result<String, DispatcherError> {
    let parsed: serde_json::Value = serde_json::from_str(result)?;
    if parsed.get("type") == Some(&serde_json::Value::String(expected_type.to_string())) {
        Ok(result.to_string())
    } else {
        Err(DispatcherError::ResultTypeMismatch(
            expected_type.to_string(),
        ))
    }
}
