use bitvmx_job_dispatcher::{
    dispatcher_error::DispatcherError, dispatcher_message::DispatcherMessage,
};
use serde::{Deserialize, Serialize};

impl DispatcherMessage for GarbledJobType {
    fn command(&self) -> Result<(String, Vec<String>, String), DispatcherError> {
        match self {
            GarbledJobType::Prove(input_value, circuit_type, output_file_path) => {
                std::fs::create_dir_all(output_file_path)?;
                let json = format!("{output_file_path}/output.json");
                let input_file = format!("{output_file_path}/input.bin");
                std::fs::write(&input_file, input_value)?;

                let cmd =
                    std::env::var("GNOVA_BIN").unwrap_or_else(|_| "../../rust-bitvmx-gc/target/release/gnova".to_string());
                let args = vec![
                    "prove".to_string(),
                    "--circuit".to_string(),
                    circuit_type.clone(),
                    "--input".to_string(),
                    input_file,
                    "--output".to_string(),
                    output_file_path.clone(),
                    "--json".to_string(),
                    json.clone(),
                ];
                Ok((cmd, args, json))
            }
            GarbledJobType::Verify(proof_file_path, output_file_path) => {
                std::fs::create_dir_all(output_file_path)?;
                let json = format!("{output_file_path}/output.json");

                let cmd =
                    std::env::var("GNOVA_BIN").unwrap_or_else(|_| "../../rust-bitvmx-gc/target/release/gnova".to_string());
                let args = vec![
                    "verify".to_string(),
                    "--proof".to_string(),
                    proof_file_path.clone(),
                    "--json".to_string(),
                    json.clone(),
                ];
                Ok((cmd, args, json))
            }
            GarbledJobType::ProveLamport(labels_json, output_file_path) => {
                std::fs::create_dir_all(output_file_path)?;
                let json = format!("{output_file_path}/output.json");
                let labels_file = format!("{output_file_path}/labels.json");
                std::fs::write(&labels_file, labels_json)?;

                let cmd =
                    std::env::var("GNOVA_BIN").unwrap_or_else(|_| "../../rust-bitvmx-gc/target/release/gnova".to_string());
                let args = vec![
                    "prove-lamport".to_string(),
                    "--labels".to_string(),
                    labels_file,
                    "--output".to_string(),
                    output_file_path.clone(),
                    "--json".to_string(),
                    json.clone(),
                ];
                Ok((cmd, args, json))
            }
            GarbledJobType::VerifyLamport(proof_file_path, output_file_path) => {
                std::fs::create_dir_all(output_file_path)?;
                let json = format!("{output_file_path}/output.json");

                let cmd =
                    std::env::var("GNOVA_BIN").unwrap_or_else(|_| "../../rust-bitvmx-gc/target/release/gnova".to_string());
                let args = vec![
                    "verify-lamport".to_string(),
                    "--proof".to_string(),
                    proof_file_path.clone(),
                    "--json".to_string(),
                    json.clone(),
                ];
                Ok((cmd, args, json))
            }
        }
    }

    fn message_type(&self) -> String {
        match self {
            GarbledJobType::Prove(..) => "ProveResult".to_string(),
            GarbledJobType::Verify(..) => "VerifyResult".to_string(),
            GarbledJobType::ProveLamport(..) => "ProveLamportResult".to_string(),
            GarbledJobType::VerifyLamport(..) => "VerifyLamportResult".to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum GarbledJobType {
    /// Prove(input_bytes, circuit_type, output_dir)
    Prove(Vec<u8>, String, String),
    /// Verify(proof_file_path, output_dir)
    Verify(String, String),
    /// ProveLamport(serialized_labels_json, output_dir)
    ProveLamport(Vec<u8>, String),
    /// VerifyLamport(proof_file_path, output_dir)
    VerifyLamport(String, String),
}
