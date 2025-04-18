use serde::{Deserialize, Serialize};
use crate::dispatcher::{dispatcher_error::DispatcherError ,dispatcher_message::DispatcherMessage};

#[derive(Debug, Serialize, Deserialize)]
pub enum ProverJobType {
    ProveStark(String, String),         // output binary file path, output json file path
    ProveSnark(String, String, String), // input binary file path, output file path, output json file path
}

impl DispatcherMessage for ProverJobType {
    fn command(&self) -> Result<(String, Vec<String>, String), DispatcherError> {
        match self {
            ProverJobType::ProveStark(output_file, json) => {
                let cmd = "../rust-bitvmx-zk-proof/target/release/host".to_string();
                let args = vec![
                    "prove-stark".to_string(),
                    "--input".to_string(),
                    "50".to_string(),
                    "--output".to_string(),
                    output_file.clone(),
                    "--json".to_string(),
                    json.clone(),
                ];
                Ok((cmd, args, json.clone()))
            }
            ProverJobType::ProveSnark(input_file, output_file, json) => {
                let cmd = "../rust-bitvmx-zk-proof/target/release/host".to_string();
                let args = vec![
                    "prove-snark".to_string(),
                    "--input".to_string(),
                    input_file.clone(),
                    "--output".to_string(),
                    output_file.clone(),
                    "--json".to_string(),
                    json.clone(),
                ];
                Ok((cmd, args, json.clone()))
            }
        }
    }
    
    fn message_type(&self) -> String {
        match self {
            ProverJobType::ProveStark(..) => "ProveStark".to_string(),
            ProverJobType::ProveSnark(..) => "ProveSnark".to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ProverResultType {
    ProveStarkResult, //result
    ProveSnarkResult, //result
}

impl ProverResultType {
    pub fn from_value(value: String) -> Result<Self, DispatcherError> {
        let result: Self = serde_json::from_str(&value)?;
        Ok(result)
    }
    
    pub fn is_prove_stark(&self) -> bool {
        match self {
            ProverResultType::ProveStarkResult => true,
            _ => false,
        }
    }
    
    pub fn is_prove_snark(&self) -> bool {
        match self {
            ProverResultType::ProveSnarkResult => true,
            _ => false,
        }
    }
}
