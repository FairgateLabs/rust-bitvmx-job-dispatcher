mod prover {
    use std::fs;

    use bitvmx_broker::{identification::identifier::Identifier, rpc::tls_helper::Cert};
    use bitvmx_job_dispatcher::dispatcher_job::{DispatcherJob, ResultMessage};
    use bitvmx_job_dispatcher_types::prover_messages::ProverJobType;
    use test_helper::test_helper::{configure_example_broker, wait_for_result, Paths};
    use zk_result::ResultType as ProverResultType;

    // To make this example work, you need to:
    // 1. Go to the `rust-bitvmx-zk-proof` folder and follow the instructions in README.md
    //    until the step "Template Setup"
    // 2. run the server example first (from bitvmx-broker).
    //      cargo run --release --example server -- --port 10000
    // 3. Then run the job-dispatcher
    //      cargo run --release --bin bitvmx-risczero-dispatcher -- --my_priv_key ..\rust-bitvmx-client\config\keys\prover.key --port 10000 --my-id 1
    // 4. Then trigger one execution
    //      cargo run --release --example risczero
    pub(crate) fn run_proof() -> Result<(), anyhow::Error> {
        let paths = Paths::new("");
        let (channel, _) = configure_example_broker(&paths, 10000)?;

        let privk = fs::read_to_string("../rust-bitvmx-client/config/keys/prover.key")?;
        let dest_id = 1;
        let cert = Cert::new_with_privk(&privk)?;
        let dest_id = Identifier {
            pubkey_hash: cert.get_pubk_hash()?,
            id: dest_id,
        };

        let msg = serde_json::to_string(&DispatcherJob {
            job_id: "uid_job".to_string(),
            job_type: ProverJobType::Prove(
                50_u32.to_be_bytes().to_vec(),
                "../rust-bitvmx-zk-proof/target/riscv-guest/methods/bitvmx/riscv32im-risc0-zkvm-elf/release/bitvmx.bin".to_string(),
                ".".to_string(),
            ),
        })?;
        channel.send(&dest_id, msg)?;

        let result = wait_for_result(&channel, 1000, 1, |msg| {
            let parsed = ResultMessage::from_str(msg)?;
            println!(
                "Received message for jobid: {} restul: {}",
                parsed.job_id, parsed.is_error
            );

            ProverResultType::from_json_string(parsed.result)
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
