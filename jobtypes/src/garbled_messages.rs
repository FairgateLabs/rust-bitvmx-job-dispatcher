use bitvmx_job_dispatcher::{
    dispatcher_error::DispatcherError, dispatcher_message::DispatcherMessage,
};
use serde::{Deserialize, Serialize};

impl GarbledJobType {
    fn gnova_bin() -> String {
        #[cfg(target_os = "windows")]
        let gnova_bin = "gnova.exe";
        #[cfg(not(target_os = "windows"))]
        let gnova_bin = "gnova";
        std::env::var("GNOVA_BIN").unwrap_or_else(|_| {
            format!(
                "{}/../../rust-bitvmx-gc/target/release/{}",
                env!("CARGO_MANIFEST_DIR"),
                gnova_bin
            )
        })
    }
}

impl DispatcherMessage for GarbledJobType {
    fn command(&self) -> Result<(String, Vec<String>, String), DispatcherError> {
        match self {
            GarbledJobType::Prove(input_value, circuit_path, output_file_path) => {
                std::fs::create_dir_all(output_file_path)?;
                let json = format!("{output_file_path}/output.json");
                let input_file = format!("{output_file_path}/input.bin");
                std::fs::write(&input_file, input_value)?;

                let cmd = Self::gnova_bin();
                let args = vec![
                    "prove".to_string(),
                    "--circuit".to_string(),
                    circuit_path.clone(),
                    "--input".to_string(),
                    input_file,
                    "--output".to_string(),
                    output_file_path.clone(),
                    "--json".to_string(),
                    json.clone(),
                ];
                Ok((cmd, args, json))
            }
            GarbledJobType::Verify(
                proof_file_path,
                circuit_path,
                public_data,
                output_file_path,
            ) => {
                std::fs::create_dir_all(output_file_path)?;
                let json = format!("{output_file_path}/output.json");

                let cmd = Self::gnova_bin();
                let args = vec![
                    "verify".to_string(),
                    "--circuit".to_string(),
                    circuit_path.clone(),
                    "--proof".to_string(),
                    proof_file_path.clone(),
                    "--public-data".to_string(),
                    public_data.clone(),
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
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum GarbledJobType {
    /// Prove(input_bytes, circuit_file_path, output_dir)
    /// Generates both GC proof and Lamport proof in one command.
    Prove(Vec<u8>, String, String),
    /// Verify(proof_file_path, circuit_file_path, public_data, output_dir)
    /// Verifies both GC proof and Lamport proof (lamport_proof.bin expected in same dir as proof.bin).
    Verify(String, String, String, String),
}
