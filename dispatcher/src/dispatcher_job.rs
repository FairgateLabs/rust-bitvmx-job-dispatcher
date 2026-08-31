use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::{dispatcher_error::DispatcherError, dispatcher_message::DispatcherMessage};

#[derive(Debug, Serialize, Deserialize)]
pub struct DispatcherJob<P>
where
    P: DispatcherMessage,
{
    pub job_id: String,
    pub job_type: P,
}

impl<P> DispatcherJob<P>
where
    P: DispatcherMessage,
{
    pub fn job_id(&self) -> &String {
        &self.job_id
    }
    pub fn job_type(&self) -> &P {
        &self.job_type
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ResultMessage {
    pub job_id: String,
    pub result: String,
    pub is_error: bool,
}

impl ResultMessage {
    pub fn new(job_id: String, result: String, is_error: bool) -> Self {
        Self {
            job_id,
            result,
            is_error,
        }
    }
    pub fn to_string(&self) -> Result<String, DispatcherError> {
        Ok(serde_json::to_string(self)?)
    }

    pub fn result_as_value(&self) -> Result<serde_json::Value, DispatcherError> {
        Ok(serde_json::from_str(&self.result)?)
    }
}

impl FromStr for ResultMessage {
    type Err = DispatcherError;

    fn from_str(msg: &str) -> Result<Self, Self::Err> {
        Ok(serde_json::from_str(msg)?)
    }
}
