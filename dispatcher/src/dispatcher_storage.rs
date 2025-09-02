use std::{
    process::Child,
    sync::{Arc, Mutex},
};

use bitvmx_broker::identification::identifier::Identifier;
use serde::de::DeserializeOwned;
use storage_backend::storage::{KeyValueStore, Storage};
use tracing::{error, info};

use crate::{
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

    pub fn persist_job(&self, job_id: &str, raw_msg: &str) {
        let key = job_key(job_id);
        if let Err(e) = self
            .storage
            .lock()
            .unwrap()
            .set(&key, raw_msg.to_string(), None)
        {
            error!("Failed to persist job {}: {}", job_id, e);
        }
    }

    pub fn remove_job(&self, job_id: &str) {
        let key = job_key(job_id);
        if let Err(e) = self.storage.lock().unwrap().delete(&key) {
            error!("Failed to delete job {}: {}", job_id, e);
        }
    }

    pub fn restore_jobs<T>(
        &self,
        dispatcher: &mut Dispatcher<T>,
    ) -> Vec<(Child, Identifier, JobContext)>
    where
        T: DispatcherMessage + DeserializeOwned,
    {
        let mut workers = Vec::new();

        // list all keys starting with job_
        let keys = self
            .storage
            .lock()
            .unwrap()
            .partial_compare_keys("job_")
            .unwrap_or(vec![]);

        for key in keys {
            let raw = match self.storage.lock().unwrap().get::<_, String>(&key) {
                Ok(Some(val)) => val.to_string(),
                _ => continue,
            };
            info!("Restoring job from key {}: {}", key, raw);
            let msg = Msg::from_string(&raw).unwrap();

            if let Some((child, context)) = process_msg(dispatcher, &msg, None) {
                // None because it is already saved
                workers.push((child, msg.id, context));
            } else {
                error!("Error processing message: {:?}", msg.to_string());
            }
        }

        workers
    }
}
