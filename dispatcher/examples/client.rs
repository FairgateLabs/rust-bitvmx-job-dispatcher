#[cfg(feature = "emulator")]
use tracing::error;

#[cfg(feature = "emulator")]
mod emulator {

    use bitvmx_broker::channel::channel::DualChannel;
    use bitvmx_broker::rpc::BrokerConfig;
    use bitvmx_job_dispatcher_types::{dispatcher_job::DispatcherJob, EmulatorJobType};
    use std::{thread::sleep, time::Duration};

    // To make this example work, you need to:
    // 1. go to ../BitVMX-CPU and run cargo build --release
    // 2. run the server example first (from bitvmx-broker).
    //      cargo run --release --example server -- --port 10000
    // 3. Then run the job-dispatcher
    //      cargo run --release
    // 4. Then trigger one execution
    //      cargo run --release --example client

    pub(crate) fn run_job() -> Result<(), anyhow::Error> {
        let elf_path =
            "../BitVMX-CPU/bitvmx-docker-riscv32/riscv32/build/hello-world.elf".to_string();

        let commands_file =
            "../BitVMX-CPU/bitvmx-docker-riscv32/riscv32/build/temp-runs/commands.json".to_string();

        let channel = DualChannel::new(&BrokerConfig::new(10000, None), 2);
        let msg = serde_json::to_string(&DispatcherJob {
            job_id: "uid_job".to_string(),
            job_type: EmulatorJobType::Execute(elf_path.clone(), commands_file.clone()),
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
}

fn main() {
    #[cfg(feature = "emulator")]
    {
        if let Err(e) = emulator::run_job() {
            error!("{}", e);
        }
    }

    #[cfg(not(feature = "emulator"))]
    println!("Run with '--features emulator' to run this example");
}
