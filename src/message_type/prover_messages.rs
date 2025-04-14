use serde::{Deserialize, Serialize};

use crate::dispatcher::dispatcher_message::DispatcherMessage;

#[derive(Debug, Serialize, Deserialize)]
pub enum ProverJobType {
    ProveStark(String),         // output binary file path
    ProveSnark(String, String), // input binary file path, output json file path
}

impl DispatcherMessage for ProverJobType {
    fn command(&self) -> (String, Vec<String>) {
        match self {
            ProverJobType::ProveStark(output_file) => {
                let cmd = "../rust-bitvmx-zk-proof/target/release/host".to_string();
                let args = vec![
                    "prove-stark".to_string(),
                    "--input".to_string(),
                    "50".to_string(),
                    "--output".to_string(),
                    output_file.clone(),
                ];
                (cmd, args)
            }
            ProverJobType::ProveSnark(input_file, output_file) => {
                let cmd = "../rust-bitvmx-zk-proof/target/release/host".to_string();
                let args = vec![
                    "prove-snark".to_string(),
                    "--input".to_string(),
                    input_file.clone(),
                    "--output".to_string(),
                    output_file.clone(),
                ];
                (cmd, args)
            }
        }
    }
}
