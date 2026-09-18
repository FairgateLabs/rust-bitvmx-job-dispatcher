use std::rc::Rc;

use bitvmx_broker::identification::identifier::Identifier;
use storage_backend::{
    key::StorageKey,
    storage::{KeyValueStore, Storage},
};

use crate::dispatcher_error::DispatcherError;

pub type JobResult = (String, (String, Identifier)); // (job_id, (result, identifier))

/// Persists and restores jobs from Storage.
pub struct DispatcherStorage {
    pub(crate) storage: Rc<Storage>,
}

fn dispatcher_key<'a>(namespace: &[&str], tail: impl IntoIterator<Item = &'a str>) -> StorageKey {
    StorageKey::new(
        std::iter::once("dispatcher")
            .chain(namespace.iter().copied())
            .map(str::to_string)
            .chain(tail.into_iter().map(str::to_string)),
    )
}

fn job_key(job_id: &str) -> StorageKey {
    dispatcher_key(&["job"], [job_id])
}

fn result_key(job_id: &str) -> StorageKey {
    dispatcher_key(&["result"], [job_id])
}

impl DispatcherStorage {
    pub fn new(storage: Rc<Storage>) -> Self {
        Self { storage }
    }

    pub fn contains_job(&self, job_id: &str) -> Result<bool, DispatcherError> {
        Ok(self.storage.has_key(job_key(job_id), None)?)
    }

    /// Persists a job to the storage backend. Uses a transaction to ensure that the job is fully persisted.
    pub fn persist_job(&self, job_id: &str, raw_msg: &str) -> Result<(), DispatcherError> {
        let tx = self.storage.begin_transaction();
        match self
            .storage
            .set(job_key(job_id), raw_msg.to_string(), Some(tx))
        {
            Ok(()) => Ok(self.storage.commit_transaction(tx)?),
            Err(e) => {
                let _ = self.storage.rollback_transaction(tx);
                Err(e.into())
            }
        }
    }

    pub fn get_job(&self, job_id: &str) -> Result<Option<String>, DispatcherError> {
        Ok(self.storage.get(job_key(job_id), None)?)
    }

    pub fn remove_job(&self, job_id: &str) -> Result<(), DispatcherError> {
        self.storage.remove(job_key(job_id), None)?;
        Ok(())
    }

    pub fn list_jobs(&self) -> Result<Vec<String>, DispatcherError> {
        let prefix = dispatcher_key(&["job"], []).to_scan_prefix();
        let keys = self.storage.partial_compare_keys(&prefix, None)?;
        keys.iter()
            .map(|key| {
                key.strip_prefix(&prefix)
                    .map(|s| s.to_string())
                    .ok_or_else(|| DispatcherError::JobIdNotFound(key.clone()))
            })
            .collect()
    }

    pub fn job_completed(&self, job_id: &str, result: &str) -> Result<(), DispatcherError> {
        self.storage
            .set(job_key(job_id), result.to_string(), None)?;
        Ok(())
    }

    pub fn complete_job(
        &self,
        job_id: &str,
        result: (String, Identifier),
    ) -> Result<(), DispatcherError> {
        let tx = self.storage.begin_transaction();
        let written = self
            .storage
            .set(result_key(job_id), result, Some(tx))
            .and_then(|_| self.storage.remove(job_key(job_id), Some(tx)));

        match written {
            Ok(()) => Ok(self.storage.commit_transaction(tx)?),
            Err(e) => {
                let _ = self.storage.rollback_transaction(tx);
                Err(e.into())
            }
        }
    }

    pub fn get_results(&self) -> Result<Vec<JobResult>, DispatcherError> {
        let mut results = Vec::new();
        let prefix = dispatcher_key(&["result"], []).to_scan_prefix();
        let keys = self.storage.partial_compare_keys(&prefix, None)?;

        for jobs in keys {
            let result: (String, Identifier) =
                match self.storage.get(StorageKey::from_joined(&jobs), None)? {
                    Some(res) => res,
                    None => continue,
                };
            let job_id = jobs.strip_prefix(&prefix).unwrap_or(&jobs).to_string();

            results.push((job_id, result));
        }

        Ok(results)
    }

    pub fn remove_result(&self, job_id: &str) -> Result<(), DispatcherError> {
        self.storage.remove(result_key(job_id), None)?;
        Ok(())
    }
}
