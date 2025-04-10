use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ProverJob {
    pub job_id: String,
    pub job_type: ProverJobType,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ProverJobType {
    ProveStark(String), // output binary file path
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ProverResultType {
    ProveStark(String), //
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProverResult {
    pub job_id: String,
    pub job_type: ProverResultType,
}