use std::{collections::HashMap, process::ExitStatus};

use crate::{
    dispatcher_error::DispatcherError, dispatcher_job::DispatcherJob,
    dispatcher_message::DispatcherMessage,
};
use serde::de::DeserializeOwned;
use tracing::error;

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

pub fn parse_and_register_job<V>(
    jobs: &mut HashMap<String, V>,
    msg: &str,
) -> Result<(String, Vec<String>, JobContext), DispatcherError>
where
    V: DispatcherMessage + DeserializeOwned,
{
    let msg: DispatcherJob<V> = serde_json::from_str(msg)?;

    if jobs.contains_key(msg.job_id()) {
        error!("Job id already exists: {}", msg.job_id());
        return Err(DispatcherError::JobIdAlreadyExists(
            msg.job_id().to_string(),
        ));
    }

    let (cmd, args, command_file) = msg.job_type.command()?;

    let job_context = JobContext::new(msg.job_id.clone(), command_file.clone());

    jobs.insert(msg.job_id().clone(), msg.job_type);

    Ok((cmd.to_string(), args, job_context))
}

pub fn process_result<V>(
    jobs: &mut HashMap<String, V>,
    id: &str,
    result: String,
    status: ExitStatus,
) -> Result<String, DispatcherError>
where
    V: DispatcherMessage,
{
    if let Some(msg_type) = jobs.remove(id) {
        if status.success() {
            let expected_type = msg_type.message_type();
            extract_structured_json(&expected_type, &result)
        } else {
            Err(DispatcherError::ProcessFailed(status.code().unwrap_or(-1)))
        }
    } else {
        Err(DispatcherError::JobIdNotFound(id.to_string()))
    }
}

fn extract_structured_json(expected_type: &str, result: &str) -> Result<String, DispatcherError> {
    let parsed: serde_json::Value = serde_json::from_str(result)?;
    if parsed.get("type") == Some(&serde_json::Value::String(expected_type.to_string())) {
        Ok(result.to_string())
    } else {
        Err(DispatcherError::ResultTypeMismatch(
            expected_type.to_string(),
        ))
    }
}

pub fn is_expected_type<V>(jobs: &HashMap<String, V>, job_id: &str, buf: &str) -> bool
where
    V: DispatcherMessage,
{
    if let Some(job_type) = jobs.get(job_id) {
        let jobtype = job_type.message_type();
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(buf) {
            parsed.get("type") == Some(&serde_json::Value::String(jobtype))
        } else {
            false
        }
    } else {
        false
    }
}
