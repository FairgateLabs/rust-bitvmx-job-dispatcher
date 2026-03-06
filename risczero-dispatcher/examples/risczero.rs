mod prover {
    use bitvmx_job_dispatcher::dispatcher_job::DispatcherJob;
    use bitvmx_job_dispatcher_types::prover_messages::ProverJobType;
    use test_helper::test_helper::{configure_example_broker, wait_for_result, Paths};
    use zk_result::ResultType as ProverResultType;

    // To make this example work, you need to:
    // 1. Go to the `rust-bitvmx-zk-proof` folder and follow the instructions in README.md
    //    until the step "Template Setup"
    // 3. run the server example first.
    //      cargo run --release --bin bitvmx-emulator-dispatcher -- --port 10000 --my-id 1
    // 4. Then run the job-dispatcher
    //      cargo run --release --features "prover"
    // 5. Then trigger one execution
    //      cargo run --release --example risczero --features "prover"

    pub(crate) fn run_proof() -> Result<(), anyhow::Error> {
        let paths = Paths::new("");
        let (channel, emulator_id) = configure_example_broker(&paths, 10000)?;

        let msg = serde_json::to_string(&DispatcherJob {
            job_id: "uid_job".to_string(),
            job_type: ProverJobType::Prove(
                50_u32.to_be_bytes().to_vec(),
                "../rust-bitvmx-zk-proof/target/riscv-guest/methods/bitvmx/riscv32im-risc0-zkvm-elf/release/bitvmx.bin".to_string(),
                ".".to_string(),
            ),
        })?;
        channel.send(&emulator_id, msg)?;

        let result = wait_for_result(&channel, 1000, 1, |msg| {
            ProverResultType::from_json_string(msg.to_string())
                .map(Some)
                .map_err(anyhow::Error::msg)
        })?;

        println!("Received result: {:?}", result);

        Ok(())
    }
}

fn main() {
    if let Err(e) = prover::run_proof() {
        eprintln!("Error: {}", e);
    }
}
