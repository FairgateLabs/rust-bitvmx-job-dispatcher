use std::{thread::sleep, time::Duration};

use bitvmx_broker::channel::channel::DualChannel;
use bitvmx_broker::rpc::BrokerConfig;
use bitvmx_emulator_job::messages::{EmulatorJob, EmulatorJobType};

// To make this example work, you need to:
// 1. go to ../BitVMX-CPU and run cargo build --release
// 2. run the server example first (from bitvmx-broker).
//      cargo run --release --example server -- --port 10000
// 3. Then run the job-dispatcher
//      cargo run --release
// 4. Then trigger one execution
//      cargo run --release --package bitvmx-emulator-job --example client

fn main() -> Result<(), anyhow::Error> {
    let channel = DualChannel::new(&BrokerConfig::new(10000, None), 2);
    let msg = serde_json::to_string(&EmulatorJob {
        job_id: "uid_job".to_string(),
        job_type: EmulatorJobType::Execute(
            "../BitVMX-CPU/docker-riscv32/riscv32/build/hello-world.elf".to_string(),
        ),
    })?;
    channel.send(10, msg)?;

    for _ in 0..10 {
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
