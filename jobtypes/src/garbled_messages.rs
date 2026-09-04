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
    fn command(&self) -> Result<(String, Vec<String>, String, String), DispatcherError> {
        match self {
            GarbledJobType::ImportProof(from_path, to_path) => {
                std::fs::create_dir_all(to_path)?;
                let json = format!("{to_path}/output.json");
                let cmd = Self::gnova_bin();
                let args = vec![
                    "import-proof".to_string(),
                    "--from".to_string(),
                    from_path.to_string(),
                    "--to".to_string(),
                    to_path.to_string(),
                ];

                Ok((cmd, args, json, "".to_string()))
            }
            GarbledJobType::Prove(circuit_path, output_file_path) => {
                std::fs::create_dir_all(output_file_path)?;
                let json = format!("{output_file_path}/output.json");
                let cmd = Self::gnova_bin();
                let args = vec![
                    "prove".to_string(),
                    "--circuit".to_string(),
                    circuit_path.clone(),
                    "--output".to_string(),
                    output_file_path.clone(),
                    "--json".to_string(),
                    json.clone(),
                ];
                Ok((cmd, args, json, "".to_string()))
            }
            GarbledJobType::Verify(proof_blob, circuit_path, output_file_path) => {
                std::fs::create_dir_all(output_file_path)?;
                let json = format!("{output_file_path}/output.json");

                let public_data = format!("{output_file_path}/public_data.json");
                std::fs::write(&public_data, &serde_json::to_vec(&proof_blob.prove_result)?)?;

                let gc_proof = format!("{output_file_path}/gc_proof.bin");
                std::fs::write(&gc_proof, &proof_blob.gc_proof)?;

                let lamport_proof = format!("{output_file_path}/lamport_proof.bin");
                std::fs::write(&lamport_proof, &proof_blob.lamport_proof)?;

                let commitments = format!("{output_file_path}/commitments.bin");
                std::fs::write(&commitments, &proof_blob.commitments)?;

                let cmd = Self::gnova_bin();
                let args = vec![
                    "verify".to_string(),
                    "--circuit".to_string(),
                    circuit_path.clone(),
                    "--proof".to_string(),
                    gc_proof.clone(),
                    "--lamport-proof".to_string(),
                    lamport_proof.clone(),
                    "--public-data".to_string(),
                    public_data.clone(),
                    "--commitments".to_string(),
                    commitments.clone(),
                    "--json".to_string(),
                    json.clone(),
                ];
                Ok((cmd, args, json, "".to_string()))
            }
            GarbledJobType::Evaluate(circuit_path, commitments, input_labels, output_file_path) => {
                std::fs::create_dir_all(output_file_path)?;
                let json = format!("{output_file_path}/output.json");
                let input_labels_file = format!("{output_file_path}/input_labels.bin");
                let mut input_labels_bytes = Vec::new();
                for (label, bit) in input_labels {
                    input_labels_bytes.extend_from_slice(label);
                    input_labels_bytes.push(*bit);
                }
                std::fs::write(&input_labels_file, input_labels_bytes)?;

                let commitments_path = format!("{output_file_path}/commitments.bin");
                std::fs::write(&commitments_path, commitments)?;

                let cmd = Self::gnova_bin();
                let args = vec![
                    "evaluate".to_string(),
                    "--circuit".to_string(),
                    circuit_path.clone(),
                    "--commitments".to_string(),
                    commitments_path.clone(),
                    "--input-labels".to_string(),
                    input_labels_file,
                    "--json".to_string(),
                    json.clone(),
                ];
                Ok((cmd, args, json, "".to_string()))
            }
        }
    }

    fn message_type(&self) -> String {
        match self {
            // ImportProof returns exacly the same struct as Prove, instead of generating a new one it uses an already saved one.
            GarbledJobType::ImportProof(..) => "ProveResult".to_string(),
            GarbledJobType::Prove(..) => "ProveResult".to_string(),
            GarbledJobType::Verify(..) => "VerifyResult".to_string(),
            GarbledJobType::Evaluate(..) => "EvaluateResult".to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum GarbledJobType {
    /// ImportProof(from_path, to_path)
    ImportProof(String, String),
    /// Prove(circuit_file_path, output_dir)
    /// Generates both GC proof and Lamport proof in one command.
    Prove(String, String),
    /// Verify(proof_blob, circuit_file_path, output_dir)
    /// Verifies both GC proof and Lamport proof (lamport_proof.bin expected in same dir as proof.bin).
    Verify(ProofBlob, String, String),
    /// Evaluate(circuit_file_path, public_data, input_labels, output_dir)
    /// Evaluates a circuit with given input labels.
    Evaluate(String, Vec<u8>, Vec<([u8; 32], u8)>, String),
}

// This struct is copied from the rust-bitvmx-gc repo, it will be here until that repo becomes public
/// Garbled gate in hex format for JSON serialization
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GarbledGateHex {
    And { ct: String },
    Noop,
}

// This struct is copied from the rust-bitvmx-gc repo, it will be here until that repo becomes public
/// Public garbling data for verifier (ct values only - no secrets)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GarblingPublicHex {
    /// Garbled gates (ct values for AND gates, Noop for XOR/INV)
    pub gates: Vec<GarbledGateHex>,
}

// This struct is copied from the rust-bitvmx-gc repo, it will be here until that repo becomes public
/// SHA256 commitment pair in hex format
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Sha256CommitmentHex {
    /// SHA256(x0) in hex
    pub h0: String,
    /// SHA256(x1) in hex
    pub h1: String,
}

// This struct is copied from the rust-bitvmx-gc repo, it will be here until that repo becomes public
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GCJobProveResult {
    pub status: String,
    pub r#type: String,
    pub circuit_file: String,
    pub num_gates: usize,
    pub num_inputs: usize,
    /// Path to GC proof
    pub proof_path: String,
    /// Path to Lamport proof
    pub lamport_proof_path: String,
    /// Path to input labels
    pub io_inputs_path: String,
    /// Path to gates and lamport commitments
    pub commitments_path: String,
    /// GC proof digests
    pub digest_circ: String,
    pub digest_ct: String,
    pub digest_io: String,
    /// Lamport proof digests
    pub digest_labels: String,
    pub digest_lamport: String,
}

// This struct is copied from the rust-bitvmx-gc repo, it will be here until that repo becomes public
#[derive(Debug, Serialize, Deserialize)]
pub struct ProofBlob {
    pub prove_result: GCJobProveResult,
    pub gc_proof: Vec<u8>,
    pub lamport_proof: Vec<u8>,
    pub commitments: Vec<u8>,
}

// This struct is copied from the rust-bitvmx-gc repo, it will be here until that repo becomes public
#[derive(Debug, Serialize, Deserialize)]
pub struct GCJobVerifyResult {
    pub status: String,
    pub r#type: String,
    /// All verifications passed (proofs valid + all digests match)
    pub valid: bool,
    /// GC proof digests (from proof)
    pub digest_circ: String,
    pub digest_ct: String,
    pub digest_io: String,
    /// Lamport proof digests (from proof)
    pub digest_labels: String,
    pub digest_lamport: String,
    /// Individual verification results
    pub gc_proof_valid: bool,
    pub lamport_proof_valid: bool,
    pub proofs_linked: bool,
    pub digest_circ_matches: bool,
    pub digest_ct_matches: bool,
    pub digest_lamport_matches: bool,
    pub valid_indices: bool,
    pub valid_num_inputs: bool,
}

// This struct is copied from the rust-bitvmx-gc repo, it will be here until that repo becomes public
#[derive(Debug, Serialize, Deserialize)]
pub struct GCJobEvaluationResult {
    pub output: Vec<Vec<u8>>,
}

// This struct is copied from the rust-bitvmx-gc repo, it will be here until that repo becomes public
/// Garbled circuit AND gate commitments, lamport commitments, and indices
#[derive(Serialize, Deserialize, Debug)]
pub struct GCCommitmentsFile {
    /// Public garbling data (ct values only, no secrets)
    pub garbling_public: GarblingPublicHex,
    /// SHA256 commitments to wire labels (public Lamport commitments)
    pub sha256_commitments: Vec<Sha256CommitmentHex>,
    pub input_commitment_indices: Vec<usize>,
}
