use std::{fs, thread::sleep, time::Duration};

use anyhow::Result;
use bitvmx_broker::RemoteChannel;
use bitvmx_job_dispatcher::dispatcher_job::{DispatcherJob, ResultMessage};
use bitvmx_job_dispatcher_types::garbled_messages::{GCJobProveResult, GarbledJobType, ProofBlob};
use test_helper::test_helper::{configure_example_broker, init_trace, Paths};
use tracing::{error, info};

// To make this example work, you need to:
// 1. Build the gnova binary:
//      cd ../rust-bitvmx-gc && cargo build --release --bin gnova
// 2. Run the broker server (from bitvmx-broker):
//      cargo run --release --example server -- --port 10000
// 3. Run the garbled-dispatcher:
//      cargo run --release --bin bitvmx-garbled-dispatcher -- --my-priv-key ../rust-bitvmx-client/config/keys/garbler.key --port 10000 --my-id 1
// 4. Run this client example:
//      cargo run --release --example garbled_client

fn main() {
    init_trace();
    if let Err(e) = run_garbled_job(10000) {
        error!("Error: {}", e);
    }
}

fn run_garbled_job(port: u16) -> Result<()> {
    info!("Starting garbled circuit client example...");

    let paths = Paths::new("", test_helper::test_helper::JobType::Garbled);
    let (channel, dest_id) = configure_example_broker(&paths, port)?;

    let output_dir = "/tmp/garbled_dispatcher_test";
    let _ = fs::remove_dir_all(output_dir);
    fs::create_dir_all(output_dir)?;

    let circuit_path = "../rust-bitvmx-gc/test-circuits/simple.circuit";

    // --- Step 1: Send Prove job ---
    info!("Sending Prove job for 'simple' circuit...");

    // Simple circuit: y = (a & b) ^ d
    let prove_job = DispatcherJob {
        job_id: "prove_simple_001".to_string(),
        job_type: GarbledJobType::Prove(circuit_path.to_string(), format!("{}/prove", output_dir)),
    };

    let msg = serde_json::to_string(&prove_job)?;
    channel.send(&dest_id, msg)?;

    info!("Waiting for Prove result...");
    let (prove_result, job_id) = wait_for_result(&channel, "ProveResult", 120, 2)?;

    info!("Prove completed (job_id: {})", job_id);
    info!("  status: {}", prove_result["status"]);
    info!("  circuit_type: {}", prove_result["circuit_type"]);
    info!("  num_gates: {}", prove_result["num_gates"]);
    info!("  num_inputs: {}", prove_result["num_inputs"]);
    info!("  digest_circ: {}", prove_result["digest_circ"]);
    info!("  digest_ct: {}", prove_result["digest_ct"]);
    info!("  digest_io: {}", prove_result["digest_io"]);

    // Save the whole prove_result as public data in json format
    let public_data = serde_json::to_string(&prove_result)?;
    let public_data_path = format!("{}/public_data.json", output_dir);

    fs::write(&public_data_path, public_data.clone())?;

    let prove_parsed: GCJobProveResult = serde_json::from_value(prove_result.clone())?;

    let gc_proof_path = &prove_parsed.proof_path;
    let lamport_proof_path = &prove_parsed.lamport_proof_path;

    let gc_proof = std::fs::read(gc_proof_path)?;
    let lamport_proof = std::fs::read(lamport_proof_path)?;

    let commitments_path = &prove_parsed.commitments_path;
    let encoded_commitments = std::fs::read(commitments_path)?;

    let proof_blob = ProofBlob {
        prove_result: prove_parsed,
        gc_proof,
        lamport_proof,
        commitments: encoded_commitments,
    };

    // --- Step 2: Send Verify job ---
    info!("Sending Verify job...");

    let verify_job = DispatcherJob {
        job_id: "verify_simple_001".to_string(),
        job_type: GarbledJobType::Verify(
            proof_blob,
            circuit_path.to_string(),
            format!("{}/verify", output_dir),
        ),
    };

    let msg = serde_json::to_string(&verify_job)?;
    channel.send(&dest_id, msg)?;

    info!("Waiting for Verify result...");
    let (verify_result, job_id) = wait_for_result(&channel, "VerifyResult", 60, 2)?;

    info!("Verify completed (job_id: {})", job_id);
    info!("  status: {}", verify_result["status"]);
    info!("  valid: {}", verify_result["valid"]);
    info!("  digest_circ: {}", verify_result["digest_circ"]);
    info!("  digest_ct: {}", verify_result["digest_ct"]);
    info!("  digest_io: {}", verify_result["digest_io"]);

    // --- Step 3: Verify digests match ---
    if prove_result["digest_circ"] == verify_result["digest_circ"]
        && prove_result["digest_ct"] == verify_result["digest_ct"]
        && prove_result["digest_io"] == verify_result["digest_io"]
    {
        info!("All digests match between Prove and Verify!");
    } else {
        error!("Digest mismatch!");
        return Err(anyhow::anyhow!("Digests do not match"));
    }

    info!("Garbled circuit dispatcher test completed successfully!");
    Ok(())
}

fn wait_for_result(
    channel: &RemoteChannel,
    expected_type: &str,
    max_attempts: usize,
    delay_secs: u64,
) -> Result<(serde_json::Value, String)> {
    for attempt in 1..=max_attempts {
        if let Some((msg, _from)) = channel.recv()? {
            info!("Received message: {}", msg);
            let result_msg: ResultMessage = serde_json::from_str(&msg)?;

            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&result_msg.result) {
                if json["type"] == expected_type {
                    return Ok((json, result_msg.job_id));
                } else {
                    info!(
                        "Received different type: {}, waiting for {}",
                        json["type"], expected_type
                    );
                }
            } else {
                info!("Received unstructured result: {}", result_msg.result);
            }
        } else {
            info!(
                "Attempt {}/{}: Waiting for result...",
                attempt, max_attempts
            );
            sleep(Duration::from_secs(delay_secs));
        }
    }

    Err(anyhow::anyhow!(
        "Timeout: did not receive '{}' result in {} attempts",
        expected_type,
        max_attempts
    ))
}
