use std::{
    process::Child,
    sync::{Arc, Mutex},
};

use bitvmx_broker::identification::identifier::Identifier;
use serde::de::DeserializeOwned;
use storage_backend::storage::{KeyValueStore, Storage};
use tracing::info;

use crate::{
    dispatcher_error::DispatcherError,
    dispatcher_message::DispatcherMessage,
    dispatcher_module::{Dispatcher, JobContext},
    helper::{job_key, process_msg, Msg},
};

/// Persists and restores jobs from Storage.
pub struct DispatcherStorage {
    storage: Arc<Mutex<Storage>>,
}

impl DispatcherStorage {
    pub fn new(storage: Arc<Mutex<Storage>>) -> Self {
        Self { storage }
    }

    pub fn persist_job(&self, job_id: &str, raw_msg: &str) -> Result<(), DispatcherError> {
        let key = job_key(job_id);
        self.storage
            .lock()
            .map_err(|_| DispatcherError::MutexPoisoned)?
            .set(&key, raw_msg.to_string(), None)?;
        Ok(())
    }

    pub fn remove_job(&self, job_id: &str) -> Result<(), DispatcherError> {
        let key = job_key(job_id);
        self.storage
            .lock()
            .map_err(|_| DispatcherError::MutexPoisoned)?
            .delete(&key)?;
        Ok(())
    }

    pub fn restore_jobs<T>(
        &self,
        dispatcher: &mut Dispatcher<T>,
    ) -> Result<Vec<(Child, Identifier, JobContext)>, DispatcherError>
    where
        T: DispatcherMessage + DeserializeOwned,
    {
        let mut workers = Vec::new();

        // list all keys starting with job_
        let keys = self
            .storage
            .lock()
            .map_err(|_| DispatcherError::MutexPoisoned)?
            .partial_compare_keys("job_")
            .unwrap_or(vec![]);

        for key in keys {
            let raw = match self
                .storage
                .lock()
                .map_err(|_| DispatcherError::MutexPoisoned)?
                .get::<_, String>(&key)
            {
                Ok(Some(val)) => val.to_string(),
                _ => continue,
            };
            info!("Restoring job from key {}: {}", key, raw);
            let msg = Msg::from_string(&raw)?;

            let (child, context) = process_msg(dispatcher, &msg, None)?; // None because it is already saved
            workers.push((child, msg.id, context));
        }

        Ok(workers)
    }
}
