use std::{thread::sleep, time::Duration};

use bitvmx_broker::channel::channel::DualChannel;
use bitvmx_broker::rpc::BrokerConfig;
use bitvmx_prover_job::messages::{ProverJobType, ProverJob};

// To make this example work, you need to:
// 1. Go to the `rust-bitvmx-zk-proof` folder and follow the instructions in README.md 
//    until the step "Template Setup"
// 3. run the server example first.
//      cargo run --release --example server -- --port 10000
// 4. Then run the job-dispatcher
//      cargo run --release
// 5. Then trigger one execution
//      cargo run --release --package bitvmx-emulator-job --example risczero

fn main() -> Result<(), anyhow::Error> {
    let channel = DualChannel::new(&BrokerConfig::new(10000, None), 2);
    let msg = serde_json::to_string(&ProverJob {
        job_id: "uid_job".to_string(),
        job_type: ProverJobType::ProveStark(
            "stark-proof.bin".to_string(),
        ),
    })?;
    channel.send(10, msg)?;

    for _ in 0..100 {
        if let Some((msg, _from)) = channel.recv()? {
            println!("Received: {}", msg);
            break;
        } else {
            println!("Waiting result execution");
            sleep(Duration::from_secs(1));
        }
    }

    Ok(())
}