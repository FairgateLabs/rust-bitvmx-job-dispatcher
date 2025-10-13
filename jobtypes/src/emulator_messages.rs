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

impl DispatcherMessage for EmulatorJobType {
    fn command(&self) -> Result<(String, Vec<String>, String), DispatcherError> {
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
            EmulatorJobType::ProverExecute(
                yaml,
                input,
                checkpoint_prover,
                command_file,
                fail_config_prover,
            ) => {
                let hex_input = hex::encode(input);
                let mut args = vec![
                    "prover-execute".to_string(),
                    "--pdf".to_string(),
                    yaml.clone(),
                    "--checkpoint-prover-path".to_string(),
                    checkpoint_prover.clone(),
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
                checkpoint_verifier,
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
                    checkpoint_verifier.clone(),
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
                checkpoint_prover,
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
                    checkpoint_prover.clone(),
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
                checkpoint_verifier,
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
                    checkpoint_verifier.clone(),
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
                checkpoint_prover,
                v_decision,
                command_file,
                fail_config_prover,
            ) => {
                let mut args = vec![
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

                if let Some(fcp) = fail_config_prover {
                    args.push("--fail-config-prover".to_string());
                    args.push(fcp.to_string());
                }

                Ok((EMULATOR_PATH.to_string(), args, command_file.to_string()))
            }
            EmulatorJobType::VerifierChooseChallenge(
                pdf,
                checkpoint_verifier,
                prover_final_trace,
                command_file,
                fail_config_verifier,
                force,
            ) => {
                let mut args = vec![
                    "verifier-choose-challenge".to_string(),
                    "--pdf".to_string(),
                    pdf.clone(),
                    "--checkpoint-verifier-path".to_string(),
                    checkpoint_verifier.clone(),
                    "--prover-final-trace".to_string(),
                    serde_json::to_string(prover_final_trace)?,
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
                checkpoint_verifier,
                command_file,
                fail_config_verifier,
                force,
            ) => {
                let mut args = vec![
                    "verifier-choose-challenge-for-read-challenge".to_string(),
                    "--pdf".to_string(),
                    pdf.clone(),
                    "--checkpoint-verifier-path".to_string(),
                    checkpoint_verifier.clone(),
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
            EmulatorJobType::VerifierChooseChallenge(_, _, _, _, _, _) => {
                "VerifierChooseChallengeResult"
            }
            EmulatorJobType::VerifierChooseChallengeForReadChallenge(_, _, _, _, _) => {
                "VerifierChooseChallengeResult"
            }
        };
        msg_type.to_string()
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
        Option<FailConfiguration>,
        ForceChallenge,
    ), // pdf, checkpoint_verifier_path, prover_final_trace, command_file, fail_config_verifier, force
    VerifierChooseChallengeForReadChallenge(
        String,
        String,
        String,
        Option<FailConfiguration>,
        ForceChallenge,
    ), // pdf, checkpoint_verifier_path, command_file, fail_config_verifier, force
}

impl EmulatorJobType {
    pub fn to_string(&self) -> Result<String, DispatcherError> {
        Ok(serde_json::to_string(self)?)
    }
}
