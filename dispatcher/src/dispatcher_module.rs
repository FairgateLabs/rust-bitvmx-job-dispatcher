use std::{collections::HashMap, process::ExitStatus};

use bitvmx_job_dispatcher_types::{
    dispatcher_job::DispatcherJob, dispatcher_message::DispatcherMessage,
};
use serde::de::DeserializeOwned;
use tracing::error;

use crate::dispatcher_error::DispatcherError;

#[derive(Clone)]
pub struct JobContext {
    pub job_id: String,
    pub command_file: String,
}

impl JobContext {
    pub fn new(job_id: String, command_file: String) -> Self {
        Self {
            job_id,
            command_file,
        }
    }
}

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
    ) -> Result<(String, Vec<String>, JobContext), DispatcherError> {
        let msg: DispatcherJob<V> = serde_json::from_str(msg)?;

        if self.jobs.contains_key(msg.job_id()) {
            error!("Job id already exists: {}", msg.job_id());
            return Err(DispatcherError::JobIdAlreadyExists);
        }

        let (cmd, args, command_file) = msg.job_type.command()?;

        let job_context = JobContext::new(msg.job_id.clone(), command_file.clone());

        self.jobs.insert(msg.job_id().clone(), msg.job_type);

        Ok((cmd.to_string(), args, job_context))
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
        if let Some(msg_type) = self.jobs.remove(id) {
            if status.success() {
                let expected_type = msg_type.message_type();

                if let Some(json_result) = Self::extract_structured_json(&expected_type, &result) {
                    return Some(json_result);
                }
                // No structured result found
                None
            } else {
                // Process exited with error
                Some("Error".to_string())
            }
        } else {
            None
        }
    }

    fn extract_structured_json(expected_type: &str, result: &str) -> Option<String> {
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(result) {
            if parsed.get("type") == Some(&serde_json::Value::String(expected_type.to_string())) {
                return Some(result.to_string());
            }
        }
        None
    }
}
