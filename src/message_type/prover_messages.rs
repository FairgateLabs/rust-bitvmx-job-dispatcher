use serde::{Deserialize, Serialize};
use crate::dispatcher::{dispatcher_error::DispatcherError ,dispatcher_message::DispatcherMessage};

#[derive(Debug, Serialize, Deserialize)]
pub enum ProverJobType {
    ProveStark(u32, String, String),         // output binary file path, output json file path
    ProveSnark(String, String), // input binary file path, output file path, output json file path
}

impl DispatcherMessage for ProverJobType {
    fn command(&self) -> Result<(String, Vec<String>, String), DispatcherError> {
        match self {
            ProverJobType::ProveStark(input_value, output_file, json) => {
                let cmd = "../rust-bitvmx-zk-proof/target/release/host".to_string();
                let args = vec![
                    "prove-stark".to_string(),
                    "--input".to_string(),
                    input_value.to_string(),
                    "--output".to_string(),
                    output_file.clone(),
                    "--json".to_string(),
                    json.clone(),
                ];
                Ok((cmd, args, json.clone()))
            }
            ProverJobType::ProveSnark(input_file, json) => {
                let cmd = "../rust-bitvmx-zk-proof/target/release/host".to_string();
                let args = vec![
                    "prove-snark".to_string(),
                    "--input".to_string(),
                    input_file.clone(),
                    "--json".to_string(),
                    json.clone(),
                ];
                Ok((cmd, args, json.clone()))
            }
        }
    }
    
    fn message_type(&self) -> String {
        match self {
            ProverJobType::ProveStark(..) => "ProveStarkResult".to_string(),
            ProverJobType::ProveSnark(..) => "ProveSnarkResult".to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ProverResultType {
    ProveStarkResult(bool), //result
    ProveSnarkResult(Vec<u8>), //result
}

impl ProverResultType {
    pub fn from_json_string(json: String) -> Result<Self, DispatcherError> {
        let value = serde_json::from_str::<serde_json::Value>(&json)?;
        let result: Self = serde_json::from_value(value)?;
        Ok(result)
    }
    
    pub fn is_prove_stark(&self) -> bool {
        match self {
            ProverResultType::ProveStarkResult(_) => true,
            _ => false,
        }
    }
    
    pub fn is_prove_snark(&self) -> bool {
        match self {
            ProverResultType::ProveSnarkResult(_) => true,
            _ => false,
        }
    }
}
