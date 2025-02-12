use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct EmulatorJob {
    pub job_id: String,
    pub job_type: EmulatorJobType,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum EmulatorJobType {
    Execute(String), // elf path
}

#[derive(Debug, Serialize, Deserialize)]
pub enum EmulatorResultType {
    Execute(String), //result hash
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EmulatorResult {
    pub job_id: String,
    pub job_type: EmulatorResultType,
}
