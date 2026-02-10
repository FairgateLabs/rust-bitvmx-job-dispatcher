use crate::{dispatcher_error::DispatcherError, dispatcher_module::JobContext};
use bitvmx_broker::identification::identifier::Identifier;
use std::{collections::HashMap, rc::Rc};
use storage_backend::storage::{KeyValueStore, Storage};

const PENDING_JOB_KEY: &str = "pending_aws_dispatcher_job_";
const INSTANCE_STATUS_KEY: &str = "aws_dispatcher_instance_status_";

pub struct DispatcherStorage {
    storage: Rc<Storage>,
}

impl DispatcherStorage {
    pub fn new(storage: Rc<Storage>) -> Self {
        Self { storage }
    }

    pub fn restore_data(
        &self,
    ) -> Result<
        (
            Vec<(Identifier, JobContext)>,
            HashMap<String, (bool, Option<JobContext>, Option<Identifier>)>,
        ),
        DispatcherError,
    > {
        let mut pending_jobs = Vec::new();
        let mut instances_status = HashMap::new();

        let mut unparsed_pending_jobs = self.storage.partial_compare(PENDING_JOB_KEY)?;
        let mut unparsed_instances_status = self.storage.partial_compare(INSTANCE_STATUS_KEY)?;

        while let Some((_, data)) = unparsed_pending_jobs.pop() {
            let (job_context, identifier): (JobContext, Identifier) = serde_json::from_str(&data)?;
            pending_jobs.push((identifier, job_context));
        }

        while let Some((key, data)) = unparsed_instances_status.pop() {
            let (job_context, identifier): (JobContext, Identifier) = serde_json::from_str(&data)?;
            let instance_id = key.split(INSTANCE_STATUS_KEY).collect::<Vec<&str>>();
            let instance_id = instance_id.last().unwrap();
            instances_status.insert(
                instance_id.to_string(),
                (false, Some(job_context), Some(identifier)),
            );
        }

        Ok((pending_jobs, instances_status))
    }

    pub fn save_pending_job(
        &self,
        id: &Identifier,
        context: &JobContext,
    ) -> Result<(), DispatcherError> {
        let key = self.pending_job_key(&id);
        self.storage.set(key, (context, id), None)?;
        Ok(())
    }

    pub fn update_instance_status(
        &self,
        instance_id: &str,
        id: &Identifier,
    ) -> Result<(), DispatcherError> {
        let transaction_id = self.storage.begin_transaction();
        let key_pending = self.pending_job_key(&id);
        let context: Option<(JobContext, Identifier)> = self.storage.get(key_pending.clone())?;
        let context = match context {
            Some((context, _)) => context,
            None => {
                self.storage.rollback_transaction(transaction_id)?;
                return Err(DispatcherError::PendingJobNotFound);
            }
        };
        self.storage.remove(key_pending, Some(transaction_id))?;
        let key_status = self.instance_status_key(instance_id);
        self.storage
            .set(key_status, (context, id), Some(transaction_id))?;
        self.storage.commit_transaction(transaction_id)?;
        Ok(())
    }

    pub fn delete_instance_status(&self, instance_id: &str) -> Result<(), DispatcherError> {
        let key = self.instance_status_key(instance_id);
        self.storage.remove(key, None)?;
        Ok(())
    }

    fn pending_job_key(&self, job_id: &Identifier) -> String {
        format!("{}{}", PENDING_JOB_KEY, job_id)
    }

    fn instance_status_key(&self, instance_id: &str) -> String {
        format!("{}{}", INSTANCE_STATUS_KEY, instance_id)
    }
}

#[cfg(test)]
mod tests {
    use std::{env, path::PathBuf};

    use crate::load_config;

    use super::*;
    use rand::{RngCore, rng};
    use storage_backend::storage_config::StorageConfig;

    fn temp_storage() -> PathBuf {
        let dir = env::temp_dir();
        let mut rang = rng();
        let index = rang.next_u32();
        dir.join(format!("storage_{}.db", index))
    }

    #[test]
    fn test_restore_data() -> Result<(), DispatcherError> {
        let config = StorageConfig::new(temp_storage().display().to_string(), None);
        let dispatcher_storage = DispatcherStorage::new(Rc::new(Storage::new(&config)?));

        let identifier_1 = Identifier::new("test1".to_string(), 1);
        let identifier_2 = Identifier::new("test2".to_string(), 2);
        let context_1 = JobContext::new(
            "test_1".to_string(),
            50_u32.to_be_bytes().to_vec(),
            "elf".to_string(),
            "command_file_path".to_string(),
        );
        let context_2 = JobContext::new(
            "test_2".to_string(),
            50_u32.to_be_bytes().to_vec(),
            "elf".to_string(),
            "command_file_path".to_string(),
        );
        let config_path = format!("{}/config/config.json", env!("CARGO_MANIFEST_DIR"));
        let instance_id = &load_config(config_path)[0]; // TODO: use all instance ids

        dispatcher_storage.save_pending_job(&identifier_1, &context_1)?;
        dispatcher_storage.save_pending_job(&identifier_2, &context_2)?;
        dispatcher_storage.update_instance_status(&instance_id, &identifier_1)?;
        let (restored_pending_jobs, restored_instances_status) =
            dispatcher_storage.restore_data()?;

        let mut result_hashmap = HashMap::new();
        result_hashmap.insert(
            instance_id.to_string(),
            (false, Some(context_1), Some(identifier_1)),
        );
        assert_eq!(restored_pending_jobs, vec![(identifier_2, context_2)]);
        assert_eq!(restored_instances_status, result_hashmap);
        Ok(())
    }
}
