use bitvmx_job_dispatcher::{
    dispatcher_error::DispatcherError, dispatcher_message::DispatcherMessage,
};
use serde::{Deserialize, Serialize};

impl DispatcherMessage for ProverJobType {
    fn command(&self) -> Result<(String, Vec<String>, String), DispatcherError> {
        match self {
            ProverJobType::Prove(input_value, elf, output_file_path) => {
                std::fs::create_dir_all(output_file_path)?;
                let json = format!("{output_file_path}/output.json");
                let stark_proof = format!("{output_file_path}/stark_proof.bin");
                let input_file = format!("{output_file_path}/input.bin");
                std::fs::write(&input_file, input_value)?;

                let cmd = "sh".to_string();
                let args = vec![
                    "-c".to_string(),
                    format!(
                        "../rust-bitvmx-zk-proof/target/release/host prove-stark \
                        --input {input_file} \
                        --elf {elf} \
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
