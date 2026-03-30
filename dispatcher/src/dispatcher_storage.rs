use std::rc::Rc;

use bitvmx_broker::identification::identifier::Identifier;
use storage_backend::storage::{KeyValueStore, Storage};

use crate::dispatcher_error::DispatcherError;

/// Persists and restores jobs from Storage.
pub struct DispatcherStorage {
    pub(crate) storage: Rc<Storage>,
}

pub fn job_key(job_id: &str) -> String {
    format!("job_{}", job_id)
}

pub fn result_key(job_id: &str) -> String {
    format!("result_{}", job_id)
}

impl DispatcherStorage {
    pub fn new(storage: Rc<Storage>) -> Self {
        Self { storage }
    }

    pub fn contains_job(&self, job_id: &str) -> Result<bool, DispatcherError> {
        let key = job_key(job_id);
        Ok(self.storage.has_key(&key, None)?)
    }

    pub fn persist_job(&self, job_id: &str, raw_msg: &str) -> Result<(), DispatcherError> {
        let key = job_key(job_id);
        self.storage.set(&key, raw_msg.to_string(), None)?;
        Ok(())
    }

    pub fn get_job(&self, job_id: &str) -> Result<Option<String>, DispatcherError> {
        let key = job_key(job_id);
        Ok(self.storage.get(&key, None)?)
    }

    pub fn remove_job(&self, job_id: &str) -> Result<(), DispatcherError> {
        let key = job_key(job_id);
        self.storage.remove(&key, None)?;
        Ok(())
    }

    pub fn list_jobs(&self) -> Result<Vec<String>, DispatcherError> {
        let keys = self.storage.partial_compare_keys("job_", None)?;
        keys.iter()
            .map(|key| {
                key.strip_prefix("job_")
                    .map(|s| s.to_string())
                    .ok_or_else(|| DispatcherError::JobIdNotFound(key.clone()))
            })
            .collect()
    }

    pub fn job_completed(&self, job_id: &str, result: &str) -> Result<(), DispatcherError> {
        let key = job_key(job_id);
        self.storage.set(&key, result.to_string(), None)?;
        Ok(())
    }

    pub fn complete_job(
        &self,
        job_id: &str,
        result: (String, Identifier),
    ) -> Result<(), DispatcherError> {
        let key = result_key(job_id);
        let tx = Some(self.storage.begin_transaction());
        self.storage.set(&key, result, tx)?;
        self.storage.remove(&job_key(job_id), tx)?;
        self.storage.commit_transaction(tx.unwrap())?;
        Ok(())
    }

    pub fn get_results(&self) -> Result<Vec<(String, (String, Identifier))>, DispatcherError> {
        let mut results = Vec::new();
        let keys = self.storage.partial_compare_keys("result_", None)?;

        for jobs in keys {
            let result: (String, Identifier) = match self.storage.get(&jobs, None)? {
                Some(res) => res,
                None => continue,
            };
            let job_id = jobs.strip_prefix("result_").unwrap_or(&jobs).to_string();

            results.push((job_id, result));
        }

        Ok(results)
    }

    pub fn remove_result(&self, job_id: &str) -> Result<(), DispatcherError> {
        let key = result_key(job_id);
        self.storage.remove(&key, None)?;
        Ok(())
    }
}
