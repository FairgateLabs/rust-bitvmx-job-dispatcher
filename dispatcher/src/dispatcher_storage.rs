use std::{collections::HashMap, process::Child, rc::Rc};

use bitvmx_broker::identification::identifier::Identifier;
use bitvmx_dispatcher_utils::Msg;
use serde::de::DeserializeOwned;
use storage_backend::storage::{KeyValueStore, Storage};
use tracing::info;

use crate::{
    dispatcher_error::DispatcherError,
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

    pub fn contains_job(&self, job_id: &str) -> Result<bool, DispatcherError> {
        let key = job_key(job_id);
        Ok(self.storage.has_key(&key)?)
    }

    pub fn persist_job(&self, job_id: &str, raw_msg: &str) -> Result<(), DispatcherError> {
        let key = job_key(job_id);
        self.storage.set(&key, raw_msg.to_string(), None)?;
        Ok(())
    }

    pub fn get_job(&self, job_id: &str) -> Result<Option<String>, DispatcherError> {
        let key = job_key(job_id);
        Ok(self.storage.get(&key)?)
    }

    pub fn remove_job(&self, job_id: &str) -> Result<(), DispatcherError> {
        let key = job_key(job_id);
        self.storage.remove(&key, None)?;
        Ok(())
    }

    pub fn list_jobs(&self) -> Result<Vec<String>, DispatcherError> {
        let keys = self.storage.partial_compare_keys("job_")?;
        Ok(keys)
    }

    pub fn job_completed(&self, job_id: &str, result: &str) -> Result<(), DispatcherError> {
        let key = job_key(job_id);
        self.storage.set(&key, result.to_string(), None)?;
        Ok(())
    }

    pub fn restore_jobs<T>(
        &self,
        jobs: &mut HashMap<String, T>,
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

            let (child, context) = process_msg(jobs, &msg.raw)?;

            // if command file exists and corresponds to this same job, skip restoring
            if let Ok(buf) = std::fs::read_to_string(&context.result_file) {
                if is_expected_type(jobs, &context.job_id, &buf) {
                    info!(
                        "Job {:?} was already completed (command file exists and matches expected type), skipping restore",
                        context.job_id
                    );
                    jobs.remove(&context.job_id);
                    continue;
                }
            }

            workers.push((child, msg.id, context));
        }

        Ok(workers)
    }
}
