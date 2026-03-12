use tracing::error;

pub mod prover {
    use bitvmx_aws_job_dispatcher::dispatcher_job::{DispatcherJob, ProverJobType, ResultMessage};
    use test_helper::test_helper::{Paths, configure_example_broker, wait_for_result};
    use tracing::info;
    use zk_result::ResultType as ProverResultType;

    // To make this example work, you need to:
    // 1. Follow AWS Instance Setup in README.md
    // 2. Run the server example first.
    //      cargo run --release --bin bitvmx-aws-job-dispatcher -- --port 10000 --my-id 1
    // 3. Then run the job-dispatcher
    //      cargo run --release --features "prover"
    // 4. Then trigger one execution
    //      cargo run --release --example risczero --features "prover"

    pub(crate) fn run_proof(port: u16) -> Result<(), anyhow::Error> {
        let paths = Paths::new("");
        let (channel, emulator_id) = configure_example_broker(&paths, port)?;

        let msg = serde_json::to_string(&DispatcherJob {
            job_id: "uid_job".to_string(),
            job_type: ProverJobType::Prove(
                50_u32.to_be_bytes().to_vec(),
                "target/riscv-guest/methods/bitvmx/riscv32im-risc0-zkvm-elf/release/bitvmx.bin"
                    .to_string(),
                "./output.json".to_string(),
            ),
        })?;
        info!("Waiting for job...");
        channel.send(&emulator_id, msg)?;

        let result = wait_for_result(&channel, 10_000_000, 1, |msg| {
            let result_msg = ResultMessage::from_str(msg)?;
            let result = ProverResultType::from_json_string(result_msg.result)
                .map_err(|e| anyhow::anyhow!(e))?;

            Ok(Some(result))
        })?;

        info!("Result: {:?}", result);

        Ok(())
    }
}

#[allow(dead_code)]
fn main() {
    if let Err(e) = prover::run_proof(10000) {
        error!("Error: {}", e);
    }
}
