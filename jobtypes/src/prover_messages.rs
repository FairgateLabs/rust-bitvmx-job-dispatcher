use bitvmx_job_dispatcher::{
    dispatcher_error::DispatcherError,
    dispatcher_message::{DispatcherMessage, JobCommand},
};
use serde::{Deserialize, Serialize};

impl DispatcherMessage for ProverJobType {
    fn prepare_local_input(&self) -> Result<(), DispatcherError> {
        match self {
            ProverJobType::Prove(input_value, _, output_file_path) => {
                std::fs::create_dir_all(output_file_path)?;
                let input_file = format!("{output_file_path}/input.bin");
                std::fs::write(&input_file, input_value)?;
            }
        }
        Ok(())
    }

    fn prepare_remote_input(&self) -> Result<Vec<(Vec<u8>, String, String)>, DispatcherError> {
        match self {
            ProverJobType::Prove(input_value, _, output_file_path) => {
                let input_file = format!("{output_file_path}/input.bin");
                Ok(vec![(
                    input_value.clone(),
                    "input.bin".to_string(),
                    input_file,
                )])
            }
        }
    }

    fn command(&self) -> Result<JobCommand, DispatcherError> {
        match self {
            ProverJobType::Prove(_input_value, elf, output_file_path) => {
                let input_file = format!("{output_file_path}/input.bin");
                let json = format!("{output_file_path}/output.json");
                let stark_proof = format!("{output_file_path}/stark_proof.bin");

                // The script is a fixed literal and every path is passed after it as a shell positional parameter, so
                // the shell binds those values as data and can never parse them as syntax.
                let cmd = "sh".to_string();
                let args = vec![
                    "-c".to_string(),
                    concat!(
                        "../rust-bitvmx-zk-proof/target/release/host prove-stark ",
                        "--input \"$1\" --elf \"$2\" --output \"$3\" --json \"$4\" && ",
                        "../rust-bitvmx-zk-proof/target/release/host prove-snark ",
                        "--input \"$3\" --json \"$4\" --json-input \"$4\""
                    )
                    .to_string(),
                    "bitvmx-prove".to_string(),
                    input_file,
                    elf.clone(),
                    stark_proof,
                    json.clone(),
                ];

                Ok(JobCommand::new(cmd, args, json, String::new()))
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
