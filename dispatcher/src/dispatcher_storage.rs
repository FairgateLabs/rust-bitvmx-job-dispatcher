use std::{process::Child, rc::Rc};

use bitvmx_broker::identification::identifier::Identifier;
use bitvmx_dispatcher_utils::Msg;
use serde::de::DeserializeOwned;
use storage_backend::storage::{KeyValueStore, Storage};
use tracing::info;

use crate::{
    dispatcher_error::DispatcherError,
    dispatcher_job::DispatcherJob,
    dispatcher_message::DispatcherMessage,
    dispatcher_module::{is_expected_type, JobContext},
    helper::{job_key, process_msg},
};

/// Persists and restores jobs from Storage.
pub struct DispatcherStorage {
    storage: Rc<Storage>,
}

impl DispatcherStorage {
    pub fn new(storage: Rc<Storage>) -> Self {
        Self { storage }
    }

    pub fn persist_job(&self, job_id: &str, raw_msg: &str) -> Result<(), DispatcherError> {
        let key = job_key(job_id);
        self.storage.set(&key, raw_msg.to_string(), None)?;
        Ok(())
    }

    pub fn remove_job(&self, job_id: &str) -> Result<(), DispatcherError> {
        let key = job_key(job_id);
        self.storage.remove(&key, None)?;
        Ok(())
    }

    pub fn has_job(&self, job_id: &str) -> bool {
        let key = job_key(job_id);
        matches!(self.storage.get::<_, String>(&key), Ok(Some(_)))
    }

    /// Deserializes the stored raw message to retrieve the job type.
    pub fn get_job_type<T>(&self, job_id: &str) -> Result<Option<T>, DispatcherError>
    where
        T: DispatcherMessage + DeserializeOwned,
    {
        let key = job_key(job_id);
        if let Ok(Some(raw)) = self.storage.get::<_, String>(&key) {
            let msg = Msg::from_string(&raw)?;
            let job: DispatcherJob<T> = serde_json::from_str(&msg.raw)?;
            Ok(Some(job.job_type))
        } else {
            Ok(None)
        }
    }

    pub fn restore_jobs<T>(
        &self,
    ) -> Result<Vec<(Child, Identifier, JobContext)>, DispatcherError>
    where
        T: DispatcherMessage + DeserializeOwned,
    {
        let mut workers = Vec::new();

        // list all keys starting with job_
        let keys = self.storage.partial_compare_keys("job_").unwrap_or(vec![]);

        for key in keys {
            let raw = match self.storage.get::<_, String>(&key) {
                Ok(Some(val)) => val.to_string(),
                _ => continue,
            };
            info!("Restoring job from key {}: {}", key, raw);
            let msg = Msg::from_string(&raw)?;

            // Parse the job to get the expected message type
            let job: DispatcherJob<T> = serde_json::from_str(&msg.raw)?;
            let expected_type = job.job_type.message_type();

            let (child, context) = process_msg::<T>(&msg.raw)?;

            // if command file exists and corresponds to this same job, skip restoring
            if let Ok(buf) = std::fs::read_to_string(&context.command_file) {
                if is_expected_type(&expected_type, &buf) {
                    info!(
                        "Job {:?} was already completed (command file exists and matches expected type), skipping restore",
                        context.job_id
                    );
                    self.remove_job(&context.job_id)?;
                    continue;
                }
            }

            workers.push((child, msg.id, context));
        }

        Ok(workers)
    }
}
