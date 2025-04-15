use std::{collections::HashMap, process::ExitStatus};

use tracing::error;

use crate::{
    errors::EmulatorError,
    messages::{EmulatorJob, EmulatorJobType},
};

#[derive(Clone)]
pub struct JobContext {
    pub job_id: String,
    pub command_file: String,
}

impl JobContext {
    pub fn new(job_id: String, command_file: String) -> Self {
        Self {
            job_id,
            command_file,
        }
    }
}

pub struct EmulatorDispatcher {
    jobs: HashMap<String, EmulatorJobType>,
}

//TODO: This part might be generalized
impl EmulatorDispatcher {
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
        }
    }

    pub fn process_msg(
        &mut self,
        msg: &str,
    ) -> Result<(String, Vec<String>, JobContext), EmulatorError> {
        let msg: EmulatorJob = serde_json::from_str(msg)?;

        //chec if id is already in jobs
        if self.jobs.contains_key(&msg.job_id) {
            error!("Job id already exists: {}", msg.job_id);
            return Err(EmulatorError::JobIdAlreadyExists);
        }

        let (cmd, args, command_file) = match &msg.job_type {
            EmulatorJobType::Execute(elf, command_file) => {
                let cmd = "../BitVMX-CPU/target/release/emulator";
                let args = vec![
                    "execute".to_string(),
                    "--elf".to_string(),
                    elf.clone(),
                    "--debug".to_string(),
                    "--limit".to_string(),
                    "20".to_string(),
                    "--command-file".to_string(),
                    command_file.clone(),
                ];
                (cmd, args, command_file)
            }
            EmulatorJobType::ProverExcecute(yaml, input, checkpoint_prover, command_file) => {
                let cmd = "../BitVMX-CPU/target/release/emulator";
                let mut args = vec![
                    "prover-excecute".to_string(),
                    "--pdf".to_string(),
                    yaml.clone(),
                    "--checkpoint-prover-path".to_string(),
                    checkpoint_prover.clone(),
                    "--force".to_string(),
                    "--command-file".to_string(),
                    command_file.clone(),
                ];

                // Extend with multiple --input flags
                for i in input {
                    args.push("--input".to_string());
                    args.push(i.to_string());
                }

                (cmd, args, command_file)
            }
            EmulatorJobType::VerifierCheckExecution(
                yaml,
                input,
                checkpoint_verifier,
                claim_last_step,
                claim_last_hash,
                command_file,
            ) => {
                let cmd = "../BitVMX-CPU/target/release/emulator";
                let mut args = vec![
                    "verifier-check-execution".to_string(),
                    "--pdf".to_string(),
                    yaml.clone(),
                    "--checkpoint-verifier-path".to_string(),
                    checkpoint_verifier.clone(),
                    "--force".to_string(),
                    "--claim-last-step".to_string(),
                    claim_last_step.to_string(),
                    "--claim-last-hash".to_string(),
                    claim_last_hash.clone(),
                    "--command-file".to_string(),
                    command_file.clone(),
                ];

                // Extend with multiple --input flags
                for i in input {
                    args.push("--input".to_string());
                    args.push(i.to_string());
                }

                (cmd, args, command_file)
            }
            EmulatorJobType::ProverGetHashesForRound(
                pdf,
                checkpoint_prover,
                round_number,
                v_decision,
                command_file,
            ) => {
                let cmd = "../BitVMX-CPU/target/release/emulator";
                let args = vec![
                    "prover-get-hashes-for-round".to_string(),
                    "--pdf".to_string(),
                    pdf.clone(),
                    "--checkpoint-prover-path".to_string(),
                    checkpoint_prover.clone(),
                    "--round-number".to_string(),
                    round_number.to_string(),
                    "--v-decision".to_string(),
                    v_decision.to_string(),
                    "--command-file".to_string(),
                    command_file.clone(),
                ];
                (cmd, args, command_file)
            }
            EmulatorJobType::VerifierChooseSegment(
                pdf,
                checkpoint_verifier,
                round_number,
                hashes,
                command_file,
            ) => {
                let cmd = "../BitVMX-CPU/target/release/emulator";
                let mut args = vec![
                    "verifier-choose-segment".to_string(),
                    "--pdf".to_string(),
                    pdf.clone(),
                    "--checkpoint-verifier-path".to_string(),
                    checkpoint_verifier.clone(),
                    "--round-number".to_string(),
                    round_number.to_string(),
                    "--command-file".to_string(),
                    command_file.clone(),
                ];

                // Extend with multiple --hashes flags
                for i in hashes {
                    args.push("--hashes".to_string());
                    args.push(i.to_string());
                }

                (cmd, args, command_file)
            }
            EmulatorJobType::ProverFinalTrace(pdf, checkpoint_prover, v_decision, command_file) => {
                let cmd = "../BitVMX-CPU/target/release/emulator";
                let args = vec![
                    "prover-final-trace".to_string(),
                    "--pdf".to_string(),
                    pdf.clone(),
                    "--checkpoint-prover-path".to_string(),
                    checkpoint_prover.clone(),
                    "--v-decision".to_string(),
                    v_decision.to_string(),
                    "--command-file".to_string(),
                    command_file.clone(),
                ];
                (cmd, args, command_file)
            }
            EmulatorJobType::VerifierChooseChallenge(
                pdf,
                checkpoint_verifier,
                prover_final_trace,
                command_file,
            ) => {
                let cmd = "../BitVMX-CPU/target/release/emulator";
                let args = vec![
                    "verifier-choose-challenge".to_string(),
                    "--pdf".to_string(),
                    pdf.clone(),
                    "--checkpoint-verifier-path".to_string(),
                    checkpoint_verifier.clone(),
                    "--prover-final-trace".to_string(),
                    serde_json::to_string(prover_final_trace)?,
                    "--command-file".to_string(),
                    command_file.clone(),
                ];

                (cmd, args, command_file)
            }
        };

        let job_context = JobContext::new(msg.job_id.clone(), command_file.clone());

        self.jobs.insert(msg.job_id.clone(), msg.job_type);

        Ok((cmd.to_string(), args, job_context))
    }

    pub fn discard_job(&mut self, id: &str) {
        self.jobs.remove(id);
    }

    pub fn process_result(
        &mut self,
        id: &str,
        result: String,
        status: ExitStatus,
    ) -> Option<String> {
        if let Some(msg_type) = self.jobs.remove(id) {
            if status.success() {
                let expected_type = match msg_type {
                    EmulatorJobType::Execute(_, _) => "ExecuteResult",
                    EmulatorJobType::ProverExcecute(_, _, _, _) => "ProverExecuteResult",
                    EmulatorJobType::VerifierCheckExecution(_, _, _, _, _, _) => {
                        "VerifierCheckExecutionResult"
                    }

                    EmulatorJobType::ProverGetHashesForRound(_, _, _, _, _) => {
                        "ProverGetHashesForRoundResult"
                    }
                    EmulatorJobType::VerifierChooseSegment(_, _, _, _, _) => {
                        "VerifierChooseSegmentResult"
                    }
                    EmulatorJobType::ProverFinalTrace(_, _, _, _) => "ProverFinalTraceResult",
                    EmulatorJobType::VerifierChooseChallenge(_, _, _, _) => {
                        "VerifierChooseChallengeResult"
                    }
                };

                if let Some(json_result) = extract_structured_json(expected_type, &result) {
                    return Some(json_result);
                }
                // No structured result found
                None
            } else {
                // Process exited with error
                Some("Error".to_string())
            }
        } else {
            None
        }
    }
}

fn extract_structured_json(expected_type: &str, result: &str) -> Option<String> {
    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(result) {
        if parsed.get("type") == Some(&serde_json::Value::String(expected_type.to_string())) {
            return Some(result.to_string());
        }
    }
    None
}
