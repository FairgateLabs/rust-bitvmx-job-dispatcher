use serde::{Deserialize, Serialize};

use crate::dispatcher_message::DispatcherMessage;

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
}

impl ResultMessage {
    pub fn new(job_id: String, result: String) -> Self {
        Self { job_id, result }
    }
    pub fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    pub fn from_str(msg: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(msg)
    }
    pub fn result_as_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::from_str(&self.result)
    }
}
