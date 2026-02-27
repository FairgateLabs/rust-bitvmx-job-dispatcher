use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DispatcherJob {
    pub job_id: String,
    pub job_type: ProverJobType,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ProverJobType {
    Prove(Vec<u8>, String, String),
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
    pub fn to_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    pub fn from_str(msg: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(msg)
    }
    pub fn result_as_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::from_str(&self.result)
    }
}
