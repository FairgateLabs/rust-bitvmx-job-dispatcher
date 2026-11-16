use bitvmx_job_dispatcher::{
    dispatcher_error::DispatcherError, dispatcher_message::DispatcherMessage,
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

    fn command(&self) -> Result<(String, Vec<String>, String, String), DispatcherError> {
        match self {
            ProverJobType::Prove(_input_value, elf, output_file_path) => {
                let input_file = format!("{output_file_path}/input.bin");
                let json = format!("{output_file_path}/output.json");
                let stark_json = format!("{output_file_path}/stark.json");
                let stark_proof = format!("{output_file_path}/stark_proof.bin");
                let cmd = "sh".to_string();
                // `host` is the CPU build and `host-gpu` the cuda build, present only on GPU images.
                // The stark runs with `host-gpu` when the image has it and with `host` otherwise. The
                // snark always runs with `host` (the GPU snark prover is not usable yet). Each step
                // writes its own json so a failed snark cannot overwrite the stark result it reads.
                let args = vec![
                    "-c".to_string(),
                    format!(
                        "STARK=../rust-bitvmx-zk-proof/target/release/host-gpu; \
                        [ -x \"$STARK\" ] || STARK=../rust-bitvmx-zk-proof/target/release/host; \
                        \"$STARK\" prove-stark \
                        --input {input_file} \
                        --elf {elf} \
                        --output {stark_proof} \
                        --json {stark_json} \
                        && ../rust-bitvmx-zk-proof/target/release/host prove-snark \
                        --input {stark_proof} \
                        --json {json} \
                        --json-input {stark_json}"
                    ),
                ];
                Ok((cmd, args, json, "".to_string()))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn prove_script() -> (String, String) {
        let job = ProverJobType::Prove(vec![0; 4], "guest.bin".to_string(), "out".to_string());
        let (cmd, args, result_file, _) = job.command().unwrap();
        assert_eq!(cmd, "sh");
        assert_eq!(args[0], "-c");
        (args[1].clone(), result_file)
    }

    #[test]
    fn prove_runs_stark_with_gpu_fallback_and_snark_with_host() {
        let (script, _) = prove_script();
        assert!(script.contains("STARK=../rust-bitvmx-zk-proof/target/release/host-gpu;"));
        assert!(script
            .contains("[ -x \"$STARK\" ] || STARK=../rust-bitvmx-zk-proof/target/release/host;"));
        assert!(script.contains("\"$STARK\" prove-stark"));
        assert!(script.contains("&& ../rust-bitvmx-zk-proof/target/release/host prove-snark"));
        assert!(!script.contains("host-cpu"));
        assert!(script.contains("--elf guest.bin"));
    }

    #[test]
    fn prove_snark_never_reads_and_writes_the_same_json() {
        let (script, result_file) = prove_script();
        assert_eq!(result_file, "out/output.json");
        assert!(script.contains("--json out/stark.json"));
        assert!(script.contains("--json out/output.json --json-input out/stark.json"));
    }
}
