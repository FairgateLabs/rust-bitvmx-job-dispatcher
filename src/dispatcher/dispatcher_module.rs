use std::{collections::HashMap, process::ExitStatus};

use serde::de::DeserializeOwned;
use tracing::error;

use crate::dispatcher::{
    dispatcher_error::DispatcherError, dispatcher_job::DispatcherJob,
    dispatcher_message::DispatcherMessage,
};

pub struct Dispatcher<V>
where
    V: DeserializeOwned,
{
    jobs: HashMap<String, V>,
}

impl<V> Dispatcher<V>
where
    V: DispatcherMessage + DeserializeOwned,
{
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
        }
    }

    pub fn process_msg(
        &mut self,
        msg: &str,
    ) -> Result<(String, Vec<String>, String), DispatcherError> {
        let msg: DispatcherJob<V> = serde_json::from_str(msg)?;

        if self.jobs.contains_key(msg.job_id()) {
            error!("Job id already exists: {}", msg.job_id());
            return Err(DispatcherError::JobIdAlreadyExists);
        }

        let (cmd, args) = msg.job_type.command();

        self.jobs.insert(msg.job_id().clone(), msg.job_type);

        Ok((cmd.to_string(), args, msg.job_id))
    }

    pub fn discard_job(&mut self, id: &str) {
        self.jobs.remove(id);
    }

    pub fn process_result(
        &mut self,
        id: &str,
        result: String,
        status: ExitStatus,
    ) -> Option<String> {
        if self.jobs.remove(id).is_some() {
            if status.success() {
                Some(result)
            } else {
                Some("Error".to_string())
            }
        } else {
            None
        }
    }
}
