use std::process::ExitStatus;

use crate::{
    dispatcher_error::DispatcherError, dispatcher_job::DispatcherJob,
    dispatcher_message::DispatcherMessage,
};
use serde::de::DeserializeOwned;

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

/// Parses a raw JSON message into command info and a JobContext.
pub fn parse_job_msg<V>(msg: &str) -> Result<(String, Vec<String>, JobContext), DispatcherError>
where
    V: DispatcherMessage + DeserializeOwned,
{
    let msg: DispatcherJob<V> = serde_json::from_str(msg)?;
    let (cmd, args, command_file) = msg.job_type.command()?;
    let job_context = JobContext::new(msg.job_id.clone(), command_file);
    Ok((cmd.to_string(), args, job_context))
}

/// Validates a completed job result against the expected message type.
pub fn validate_result(
    expected_type: &str,
    result: String,
    status: ExitStatus,
) -> Result<String, DispatcherError> {
    if status.success() {
        extract_structured_json(expected_type, &result)
    } else {
        Err(DispatcherError::ProcessFailed(status.code().unwrap_or(-1)))
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

/// Checks whether a result buffer matches the expected message type.
pub fn is_expected_type(expected_type: &str, buf: &str) -> bool {
    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(buf) {
        parsed.get("type") == Some(&serde_json::Value::String(expected_type.to_string()))
    } else {
        false
    }
}
