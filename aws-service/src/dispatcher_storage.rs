use std::rc::Rc;
use bitvmx_broker::identification::identifier::Identifier;
use storage_backend::storage::{Storage, KeyValueStore};
use crate::{dispatcher_error::DispatcherError, dispatcher_module::JobContext};

pub struct DispatcherStorage {
    storage: Rc<Storage>,
}

impl DispatcherStorage {
    pub fn new(storage: Rc<Storage>) -> Self {
        Self { storage }
    }

    pub fn save_pending_job(&self, id: &Identifier, context: &JobContext) -> Result<(), DispatcherError> {
        let key = self.pending_job_key(&id);
        self.storage.set(key, context, None)?;
        Ok(())
    }

    pub fn change_to_working_instance(&self, instance_id: &str, id: &Identifier, context: &JobContext) -> Result<(), DispatcherError> {
        let transaction_id = self.storage.begin_transaction();
        let key_pending = self.pending_job_key(&id);
        self.storage.remove(key_pending, Some(transaction_id))?;
        let key_working = self.working_instance_key(instance_id);
        self.storage.set(key_working, context, Some(transaction_id))?;
        self.storage.commit_transaction(transaction_id)?;
        Ok(())
    }

    pub fn delete_working_instance(&self, instance_id: &str) -> Result<(), DispatcherError> {
        let key = self.working_instance_key(instance_id);
        self.storage.remove(key, None)?;
        Ok(())
    }

    fn pending_job_key(&self, job_id: &Identifier) -> String {
        format!("pending_job_{}", job_id)
    }

    fn working_instance_key(&self, instance_id: &str) -> String {
        format!("working_instance_{}", instance_id)
    }
}