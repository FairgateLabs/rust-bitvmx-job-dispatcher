use std::path::PathBuf;

use bitvmx_job_dispatcher::{
    dispatcher_error::DispatcherError, dispatcher_message::DispatcherMessage,
};
use serde::{Deserialize, Serialize};

impl DispatcherMessage for ProverJobType {
    fn command(&self) -> Result<(String, Vec<String>, String), DispatcherError> {
        match self {
            ProverJobType::Prove(input_value, _elf, output_file_path) => {
                std::fs::create_dir_all(output_file_path)?;
                let json = format!("{output_file_path}/output.json");
                let stark_proof = format!("{output_file_path}/stark_proof.bin");
                let input_value = u32::from_be_bytes(input_value.as_slice().try_into().unwrap());
                let cmd = "sh".to_string();
                let args = vec![
                    "-c".to_string(),
                    format!(
                        "../rust-bitvmx-zk-proof/target/release/host prove-stark \
                        --input {input_value} \
                        --output {stark_proof} \
                        --json {json} \
                        && ../rust-bitvmx-zk-proof/target/release/host \
                        prove-snark \
                        --input {stark_proof} \
                        --json {json} \
                        --json-input {json}"
                    ),
                ];
                Ok((cmd, args, json))
            }
        }
    }

    fn message_type(&self) -> String {
        match self {
            ProverJobType::Prove(..) => "ProveResult".to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ProverJobType {
    Prove(Vec<u8>, String, String),
}
