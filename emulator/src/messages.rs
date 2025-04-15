use crate::errors::EmulatorError;
use bitvmx_cpu_definitions::{challenge::ChallengeType, trace::TraceRWStep};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct EmulatorJob {
    pub job_id: String,
    pub job_type: EmulatorJobType,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum EmulatorJobType {
    //TODO: add Force and FailConfiguration to each one
    Execute(String, String),                         // elf path, command_file
    ProverExcecute(String, Vec<u8>, String, String), // yaml path, inputs, checkpoint path, command_file
    VerifierCheckExecution(String, Vec<u8>, String, u64, String, String), // yaml path, inputs, checkpoint path, claim_last_step, claim_last_hash, command_file
    ProverGetHashesForRound(String, String, u8, u32, String), // pdf, checkpoint_prover_path, round_number, v_decision, command_file
    VerifierChooseSegment(String, String, u8, Vec<String>, String), // pdf, checkpoint_verifier_path, round_number, hashes, command_file
    ProverFinalTrace(String, String, u32, String), // pdf, checkpoint_prover_path, v_decision, command_file
    VerifierChooseChallenge(String, String, TraceRWStep, String), // pdf, checkpoint_verifier_path, prover_final_trace, command_file
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum EmulatorResultType {
    ProverExecuteResult { last_step: u64, last_hash: String },
    VerifierCheckExecutionResult {},
    ProverGetHashesForRoundResult { hashes: Vec<String> },
    VerifierChooseSegmentResult { v_decision: u32 },
    ProverFinalTraceResult { final_trace: TraceRWStep },
    VerifierChooseChallengeResult { challenge: ChallengeType },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EmulatorResult {
    pub job_id: String,
    pub job_type: EmulatorResultType,
}

impl EmulatorResultType {
    pub fn from_value(value: serde_json::Value) -> Result<Self, EmulatorError> {
        serde_json::from_value(value).map_err(EmulatorError::SerializationError)
    }

    pub fn as_prover_execute(&self) -> Result<(u64, String), EmulatorError> {
        match self {
            EmulatorResultType::ProverExecuteResult {
                last_step,
                last_hash,
            } => Ok((*last_step, last_hash.clone())),
            _ => Err(EmulatorError::ResultTypeMismatch(
                "Expected ProverExecuteResult".to_string(),
            )),
        }
    }

    pub fn as_verifier_check(&self) -> Result<(), EmulatorError> {
        match self {
            EmulatorResultType::VerifierCheckExecutionResult {} => Ok(()),
            _ => Err(EmulatorError::ResultTypeMismatch(
                "Expected VerifierCheckExecutionResult".to_string(),
            )),
        }
    }

    pub fn as_prover_hashes(&self) -> Result<Vec<String>, EmulatorError> {
        match self {
            EmulatorResultType::ProverGetHashesForRoundResult { hashes } => Ok(hashes.clone()),
            _ => Err(EmulatorError::ResultTypeMismatch(
                "Expected ProverGetHashesForRoundResult".to_string(),
            )),
        }
    }

    pub fn as_v_decision(&self) -> Result<u32, EmulatorError> {
        match self {
            EmulatorResultType::VerifierChooseSegmentResult { v_decision } => Ok(*v_decision),
            _ => Err(EmulatorError::ResultTypeMismatch(
                "Expected VerifierChooseSegmentResult".to_string(),
            )),
        }
    }

    pub fn as_final_trace(&self) -> Result<TraceRWStep, EmulatorError> {
        match self {
            EmulatorResultType::ProverFinalTraceResult { final_trace } => Ok(final_trace.clone()),
            _ => Err(EmulatorError::ResultTypeMismatch(
                "Expected ProverFinalTraceResult".to_string(),
            )),
        }
    }

    pub fn as_challenge(&self) -> Result<ChallengeType, EmulatorError> {
        match self {
            EmulatorResultType::VerifierChooseChallengeResult { challenge } => {
                Ok(challenge.clone())
            }
            _ => Err(EmulatorError::ResultTypeMismatch(
                "Expected VerifierChooseChallengeResult".to_string(),
            )),
        }
    }
}
