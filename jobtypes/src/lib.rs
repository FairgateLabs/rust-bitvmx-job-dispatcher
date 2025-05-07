pub mod dispatcher_job;
pub mod dispatcher_message;
pub mod emulator_messages;
pub mod prover_messages;

use bitvmx_cpu_definitions::{challenge::ChallengeType, trace::TraceRWStep};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Serialize, Deserialize)]
pub enum EmulatorJobType {
    //TODO: add Force and FailConfiguration to each one
    Execute(String, String),                        // elf path, command_file
    ProverExecute(String, Vec<u8>, String, String), // yaml path, inputs, checkpoint path, command_file
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

impl EmulatorJobType {
    pub fn to_string(&self) -> Result<String, JobTypeError> {
        Ok(serde_json::to_string(self)?)
    }
}

#[derive(Error, Debug)]
pub enum JobTypeError {
    #[error("Serialization error {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Result type mismatch {0}")]
    ResultTypeMismatch(String),

    #[error("Job id already exists")]
    JobIdAlreadyExists,
}

impl EmulatorResultType {
    pub fn from_value(value: serde_json::Value) -> Result<Self, JobTypeError> {
        serde_json::from_value(value).map_err(JobTypeError::SerializationError)
    }

    pub fn as_prover_execute(&self) -> Result<(u64, String), JobTypeError> {
        match self {
            EmulatorResultType::ProverExecuteResult {
                last_step,
                last_hash,
            } => Ok((*last_step, last_hash.clone())),
            _ => Err(JobTypeError::ResultTypeMismatch(
                "Expected ProverExecuteResult".to_string(),
            )),
        }
    }

    pub fn as_verifier_check(&self) -> Result<(), JobTypeError> {
        match self {
            EmulatorResultType::VerifierCheckExecutionResult {} => Ok(()),
            _ => Err(JobTypeError::ResultTypeMismatch(
                "Expected VerifierCheckExecutionResult".to_string(),
            )),
        }
    }

    pub fn as_prover_hashes(&self) -> Result<Vec<String>, JobTypeError> {
        match self {
            EmulatorResultType::ProverGetHashesForRoundResult { hashes } => Ok(hashes.clone()),
            _ => Err(JobTypeError::ResultTypeMismatch(
                "Expected ProverGetHashesForRoundResult".to_string(),
            )),
        }
    }

    pub fn as_v_decision(&self) -> Result<u32, JobTypeError> {
        match self {
            EmulatorResultType::VerifierChooseSegmentResult { v_decision } => Ok(*v_decision),
            _ => Err(JobTypeError::ResultTypeMismatch(
                "Expected VerifierChooseSegmentResult".to_string(),
            )),
        }
    }

    pub fn as_final_trace(&self) -> Result<TraceRWStep, JobTypeError> {
        match self {
            EmulatorResultType::ProverFinalTraceResult { final_trace } => Ok(final_trace.clone()),
            _ => Err(JobTypeError::ResultTypeMismatch(
                "Expected ProverFinalTraceResult".to_string(),
            )),
        }
    }

    pub fn as_challenge(&self) -> Result<ChallengeType, JobTypeError> {
        match self {
            EmulatorResultType::VerifierChooseChallengeResult { challenge } => {
                Ok(challenge.clone())
            }
            _ => Err(JobTypeError::ResultTypeMismatch(
                "Expected VerifierChooseChallengeResult".to_string(),
            )),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ProverJobType {
    Prove(Vec<u8>, String, String),
}
