use std::{
    fs, io,
    path::{Path, PathBuf},
};

use bitvmx_cpu_definitions::{
    //challenge::{ChallengeType, EmulatorResultType},
    trace::TraceRWStep,
};
use bitvmx_job_dispatcher::{
    dispatcher_error::DispatcherError, dispatcher_message::DispatcherMessage,
};
use emulator::{
    decision::{
        challenge::{ForceChallenge, ForceCondition},
        nary_search::NArySearchType,
    },
    executor::utils::FailConfiguration,
};
use serde::{Deserialize, Serialize};

//windows
#[cfg(target_os = "windows")]
const EMULATOR_PATH: &str = "../BitVMX-CPU/target/release/emulator.exe";
#[cfg(not(target_os = "windows"))]
const EMULATOR_PATH: &str = "../BitVMX-CPU/target/release/emulator";

fn copy_dir_all(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> io::Result<()> {
    fs::create_dir_all(&dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_dir_all(entry.path(), dst.as_ref().join(entry.file_name()))?;
        } else {
            fs::copy(entry.path(), dst.as_ref().join(entry.file_name()))?;
        }
    }
    Ok(())
}

fn prepare_current(base: &Path) -> io::Result<PathBuf> {
    let prev = base.join("prev");
    let current = base.join("current");

    if current.exists() {
        fs::remove_dir_all(&current)?;
    }

    if prev.exists() {
        copy_dir_all(&prev, &current)?;
    } else {
        fs::create_dir_all(&current)?;
    }

    Ok(current)
}

pub fn commit_checkpoint(base: &Path) -> io::Result<()> {
    let prev = base.join("prev");
    let current = base.join("current");

    if prev.exists() {
        fs::remove_dir_all(&prev)?;
    }

    fs::rename(current, prev)?;
    Ok(())
}

impl DispatcherMessage for EmulatorJobType {
    fn command(&self) -> Result<(String, Vec<String>, String), DispatcherError> {
        let checkpoint_path = self.prepare_checkpoint()?;
        match self {
            EmulatorJobType::Execute(elf, command_file) => {
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
                Ok((EMULATOR_PATH.to_string(), args, command_file.to_string()))
            }
            EmulatorJobType::ProverExecute(yaml, input, _, command_file, fail_config_prover) => {
                let hex_input = hex::encode(input);
                let mut args = vec![
                    "prover-execute".to_string(),
                    "--pdf".to_string(),
                    yaml.clone(),
                    "--checkpoint-prover-path".to_string(),
                    checkpoint_path.clone(),
                    "--force".to_string(),
                    "--command-file".to_string(),
                    command_file.clone(),
                    "--input".to_string(),
                    hex_input.clone(),
                ];

                if let Some(fcp) = fail_config_prover {
                    args.push("--fail-config-prover".to_string());
                    args.push(fcp.to_string());
                }

                Ok((EMULATOR_PATH.to_string(), args, command_file.to_string()))
            }
            EmulatorJobType::VerifierCheckExecution(
                yaml,
                input,
                _,
                claim_last_step,
                claim_last_hash,
                command_file,
                force,
                fail_config_verifier,
            ) => {
                let hex_input = hex::encode(input);
                let mut args = vec![
                    "verifier-check-execution".to_string(),
                    "--pdf".to_string(),
                    yaml.clone(),
                    "--checkpoint-verifier-path".to_string(),
                    checkpoint_path.clone(),
                    "--claim-last-step".to_string(),
                    claim_last_step.to_string(),
                    "--claim-last-hash".to_string(),
                    claim_last_hash.clone(),
                    "--command-file".to_string(),
                    command_file.clone(),
                    "--input".to_string(),
                    hex_input.clone(),
                ];

                if let Some(fcv) = fail_config_verifier {
                    args.push("--fail-config-verifier".to_string());
                    args.push(fcv.to_string());
                }

                args.push("--force".to_string());
                args.push(force.to_string());

                Ok((EMULATOR_PATH.to_string(), args, command_file.to_string()))
            }
            EmulatorJobType::ProverGetHashesForRound(
                pdf,
                _,
                round_number,
                v_decision,
                command_file,
                fail_config_prover,
                nary_search_type,
            ) => {
                let mut args = vec![
                    "prover-get-hashes-for-round".to_string(),
                    "--pdf".to_string(),
                    pdf.clone(),
                    "--checkpoint-prover-path".to_string(),
                    checkpoint_path.clone(),
                    "--round-number".to_string(),
                    round_number.to_string(),
                    "--v-decision".to_string(),
                    v_decision.to_string(),
                    "--command-file".to_string(),
                    command_file.clone(),
                    "--nary-type".to_string(),
                    nary_search_type.to_string(),
                ];

                if let Some(fcp) = fail_config_prover {
                    args.push("--fail-config-prover".to_string());
                    args.push(fcp.to_string());
                }

                Ok((EMULATOR_PATH.to_string(), args, command_file.to_string()))
            }
            EmulatorJobType::VerifierChooseSegment(
                pdf,
                _,
                round_number,
                hashes,
                command_file,
                fail_config_verifier,
                nary_search_type,
            ) => {
                let mut args = vec![
                    "verifier-choose-segment".to_string(),
                    "--pdf".to_string(),
                    pdf.clone(),
                    "--checkpoint-verifier-path".to_string(),
                    checkpoint_path.clone(),
                    "--round-number".to_string(),
                    round_number.to_string(),
                    "--command-file".to_string(),
                    command_file.clone(),
                    "--nary-type".to_string(),
                    nary_search_type.to_string(),
                ];

                if let Some(fcv) = fail_config_verifier {
                    args.push("--fail-config-verifier".to_string());
                    args.push(fcv.to_string());
                }

                // Extend with multiple --hashes flags
                for i in hashes {
                    args.push("--hashes".to_string());
                    args.push(i.to_string());
                }

                Ok((EMULATOR_PATH.to_string(), args, command_file.to_string()))
            }
            EmulatorJobType::ProverFinalTrace(
                pdf,
                _,
                v_decision,
                command_file,
                fail_config_prover,
            ) => {
                let mut args = vec![
                    "prover-final-trace".to_string(),
                    "--pdf".to_string(),
                    pdf.clone(),
                    "--checkpoint-prover-path".to_string(),
                    checkpoint_path.clone(),
                    "--v-decision".to_string(),
                    v_decision.to_string(),
                    "--command-file".to_string(),
                    command_file.clone(),
                ];

                if let Some(fcp) = fail_config_prover {
                    args.push("--fail-config-prover".to_string());
                    args.push(fcp.to_string());
                }

                Ok((EMULATOR_PATH.to_string(), args, command_file.to_string()))
            }
            EmulatorJobType::VerifierChooseChallenge(
                pdf,
                _,
                prover_final_trace,
                resigned_step_hash,
                resigned_next_hash,
                command_file,
                fail_config_verifier,
                force,
            ) => {
                let mut args = vec![
                    "verifier-choose-challenge".to_string(),
                    "--pdf".to_string(),
                    pdf.clone(),
                    "--checkpoint-verifier-path".to_string(),
                    checkpoint_path.clone(),
                    "--prover-final-trace".to_string(),
                    serde_json::to_string(&prover_final_trace)?,
                    "--resigned-step-hash".to_string(),
                    resigned_step_hash.clone(),
                    "--resigned-next-hash".to_string(),
                    resigned_next_hash.clone(),
                    "--command-file".to_string(),
                    command_file.clone(),
                    "--force".to_string(),
                    force.to_string(),
                ];

                if let Some(fcv) = fail_config_verifier {
                    args.push("--fail-config-verifier".to_string());
                    args.push(fcv.to_string());
                }

                Ok((EMULATOR_PATH.to_string(), args, command_file.to_string()))
            }
            EmulatorJobType::VerifierChooseChallengeForReadChallenge(
                pdf,
                _,
                command_file,
                resigned_step_hash,
                resigned_next_hash,
                fail_config_verifier,
                force,
            ) => {
                let mut args = vec![
                    "verifier-choose-challenge-for-read-challenge".to_string(),
                    "--pdf".to_string(),
                    pdf.clone(),
                    "--checkpoint-verifier-path".to_string(),
                    checkpoint_path.clone(),
                    "--resigned-step-hash".to_string(),
                    resigned_step_hash.clone(),
                    "--resigned-next-hash".to_string(),
                    resigned_next_hash.clone(),
                    "--command-file".to_string(),
                    command_file.clone(),
                    "--force".to_string(),
                    force.to_string(),
                ];

                if let Some(fcv) = fail_config_verifier {
                    args.push("--fail-config-verifier".to_string());
                    args.push(fcv.to_string());
                }

                Ok((EMULATOR_PATH.to_string(), args, command_file.to_string()))
            }
            EmulatorJobType::ProverGetHashesAndStep(
                pdf,
                _,
                v_decision,
                command_file,
                fail_config_prover,
            ) => {
                let mut args = vec![
                    "prover-get-hashes-and-step".to_string(),
                    "--pdf".to_string(),
                    pdf.clone(),
                    "--checkpoint-prover-path".to_string(),
                    checkpoint_path.clone(),
                    "--v-decision".to_string(),
                    v_decision.to_string(),
                    "--command-file".to_string(),
                    command_file.clone(),
                ];

                if let Some(fcp) = fail_config_prover {
                    args.push("--fail-config-prover".to_string());
                    args.push(fcp.to_string());
                }

                Ok((EMULATOR_PATH.to_string(), args, command_file.to_string()))
            }
        }
    }

    fn message_type(&self) -> String {
        let msg_type = match self {
            EmulatorJobType::Execute(_, _) => "ExecuteResult",
            EmulatorJobType::ProverExecute(_, _, _, _, _) => "ProverExecuteResult",
            EmulatorJobType::VerifierCheckExecution(_, _, _, _, _, _, _, _) => {
                "VerifierCheckExecutionResult"
            }
            EmulatorJobType::ProverGetHashesForRound(_, _, _, _, _, _, _) => {
                "ProverGetHashesForRoundResult"
            }
            EmulatorJobType::VerifierChooseSegment(_, _, _, _, _, _, _) => {
                "VerifierChooseSegmentResult"
            }
            EmulatorJobType::ProverFinalTrace(_, _, _, _, _) => "ProverFinalTraceResult",
            EmulatorJobType::VerifierChooseChallenge(_, _, _, _, _, _, _, _) => {
                "VerifierChooseChallengeResult"
            }
            EmulatorJobType::VerifierChooseChallengeForReadChallenge(_, _, _, _, _, _, _) => {
                "VerifierChooseChallengeResult"
            }
            EmulatorJobType::ProverGetHashesAndStep(_, _, _, _, _) => {
                "ProverGetHashesAndStepResult"
            }
        };
        msg_type.to_string()
    }

    fn commit_checkpoint(&self) -> Result<(), DispatcherError> {
        self.commit_checkpoint()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum EmulatorJobType {
    //TODO: add Force and FailConfiguration to each one
    Execute(String, String), // elf path, command_file
    ProverExecute(String, Vec<u8>, String, String, Option<FailConfiguration>), // yaml path, inputs, checkpoint path, command_file, fail_config_prover
    VerifierCheckExecution(
        String,
        Vec<u8>,
        String,
        u64,
        String,
        String,
        ForceCondition,
        Option<FailConfiguration>,
    ), // yaml path, inputs, checkpoint path, claim_last_step, claim_last_hash, command_file, fail_config_verifier
    ProverGetHashesForRound(
        String,
        String,
        u8,
        u32,
        String,
        Option<FailConfiguration>,
        NArySearchType,
    ), // pdf, checkpoint_prover_path, round_number, v_decision, command_file, fail_config_prover, nary_search_type
    VerifierChooseSegment(
        String,
        String,
        u8,
        Vec<String>,
        String,
        Option<FailConfiguration>,
        NArySearchType,
    ), // pdf, checkpoint_verifier_path, round_number, hashes, command_file, fail_config_verifier, nary_search_type
    ProverFinalTrace(String, String, u32, String, Option<FailConfiguration>), // pdf, checkpoint_prover_path, v_decision, command_file, fail_config_prover
    VerifierChooseChallenge(
        String,
        String,
        TraceRWStep,
        String,
        String,
        String,
        Option<FailConfiguration>,
        ForceChallenge,
    ), // pdf, checkpoint_verifier_path, prover_final_trace, resigned_step_hash, resigned_next_hash, command_file, fail_config_verifier, force
    VerifierChooseChallengeForReadChallenge(
        String,
        String,
        String,
        String,
        String,
        Option<FailConfiguration>,
        ForceChallenge,
    ), // pdf, checkpoint_verifier_path, command_file, resigned_step_hash, resigned_next_hash, fail_config_verifier, force
    ProverGetHashesAndStep(String, String, u32, String, Option<FailConfiguration>), // pdf, checkpoint_prover_path, v_decision, command_file, fail_config_prover
}

impl EmulatorJobType {
    pub fn to_string(&self) -> Result<String, DispatcherError> {
        Ok(serde_json::to_string(self)?)
    }
    pub fn checkpoint_base(&self) -> Result<String, DispatcherError> {
        match self {
            EmulatorJobType::ProverExecute(_, _, base, _, _) => Ok(base.clone()),
            EmulatorJobType::VerifierCheckExecution(_, _, base, _, _, _, _, _) => Ok(base.clone()),
            EmulatorJobType::ProverGetHashesForRound(_, base, _, _, _, _, _) => Ok(base.clone()),
            EmulatorJobType::VerifierChooseSegment(_, base, _, _, _, _, _) => Ok(base.clone()),
            EmulatorJobType::ProverFinalTrace(_, base, _, _, _) => Ok(base.clone()),
            EmulatorJobType::VerifierChooseChallenge(_, base, _, _, _, _, _, _) => Ok(base.clone()),
            EmulatorJobType::VerifierChooseChallengeForReadChallenge(_, base, _, _, _, _, _) => {
                Ok(base.clone())
            }
            EmulatorJobType::ProverGetHashesAndStep(_, base, _, _, _) => Ok(base.clone()),
            EmulatorJobType::Execute(_, _) => Err(DispatcherError::CheckpointPathError(
                "Execute job type does not have a checkpoint path".to_string(),
            )),
        }
    }
    pub fn prepare_checkpoint(&self) -> Result<String, DispatcherError> {
        let base = PathBuf::from(self.checkpoint_base()?);
        let current = prepare_current(&base)
            .map_err(|e| DispatcherError::CheckpointPathError(e.to_string()))?;

        current
            .to_str()
            .map(|s| s.to_string())
            .ok_or_else(|| DispatcherError::CheckpointPathError("Invalid checkpoint path".into()))
    }

    pub fn commit_checkpoint(&self) -> Result<(), DispatcherError> {
        let base = PathBuf::from(self.checkpoint_base()?);
        commit_checkpoint(&base).map_err(|e| DispatcherError::CheckpointPathError(e.to_string()))
    }
}
