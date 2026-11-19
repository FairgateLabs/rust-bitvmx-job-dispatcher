mod prover {

    use bitvmx_job_dispatcher::dispatcher_job::{DispatcherJob, ResultMessage};
    use bitvmx_job_dispatcher_types::prover_messages::ProverJobType;
    use std::str::FromStr;
    use test_helper::test_helper::{configure_example_broker, wait_for_result, Paths};
    use zk_result::ResultType as ProverResultType;

    // To make this example work, you need to:
    // 1. Go to the `rust-bitvmx-zk-proof` folder and follow the instructions in README.md
    //    until the step "Template Setup"
    // 2. run the server example first (from bitvmx-broker).
    //      cargo run --release --example server -- --port 10000
    // 3. Then run the job-dispatcher
    //      cargo run --release --bin bitvmx-risczero-dispatcher -- --my-priv-key ..\rust-bitvmx-client\config\keys\prover.key --port 10000 --my-id 1
    //    To run the jobs on EC2 instead, build with `--features aws` and add `--mode aws --config-path <config.yaml>`.
    //    The aws credentials are read from AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
    // 4. Then trigger one execution
    //      cargo run --release --example risczero
    //
    // Environment variables (all optional, the defaults run the template guest with the input 50):
    // - RISCZERO_INPUT: path of a file whose bytes are the input of the guest.
    // - RISCZERO_ELF: path of the guest `.bin` on the machine that runs the job. On aws this is a path inside the
    //   image, for example /home/ubuntu/rust-bitvmx-zk-proof/guests/check-fork-guest.bin on the gpu image.
    // - RISCZERO_OUTPUT_DIR: directory where the job writes its files, relative to the repository of the prover.
    // - RISCZERO_TIMEOUT_SECS: how long to wait for the result.
    pub(crate) fn run_proof() -> Result<(), anyhow::Error> {
        let paths = Paths::new("", test_helper::test_helper::JobType::Risczero);
        let (channel, dest_id) = configure_example_broker(&paths, 10000)?;

        let input = match std::env::var("RISCZERO_INPUT") {
            Ok(path) => std::fs::read(path)?,
            Err(_) => 50_u32.to_be_bytes().to_vec(),
        };
        let elf = std::env::var("RISCZERO_ELF").unwrap_or_else(|_| "../rust-bitvmx-zk-proof/target/riscv-guest/methods/bitvmx/riscv32im-risc0-zkvm-elf/release/bitvmx.bin".to_string());
        let output_dir = std::env::var("RISCZERO_OUTPUT_DIR").unwrap_or_else(|_| ".".to_string());
        let timeout_secs: u64 = match std::env::var("RISCZERO_TIMEOUT_SECS") {
            Ok(value) => value.parse()?,
            Err(_) => 2000,
        };
        let delay_secs = 2;

        let msg = serde_json::to_string(&DispatcherJob {
            job_id: "uid_job".to_string(),
            job_type: ProverJobType::Prove(input, elf, output_dir),
        })?;
        channel.send(&dest_id, msg)?;

        let result = wait_for_result(
            &channel,
            (timeout_secs / delay_secs) as usize,
            delay_secs,
            |msg| {
                let parsed = ResultMessage::from_str(msg)?;
                println!(
                    "Received message for jobid: {} result is err: {}",
                    parsed.job_id, parsed.is_error
                );

                ProverResultType::from_json_string(parsed.result)
                    .map(Some)
                    .map_err(anyhow::Error::msg)
            },
        )?;

        println!("Received result: {:?}", result);

        Ok(())
    }
}

fn main() {
    if let Err(e) = prover::run_proof() {
        eprintln!("Error: {}", e);
    }
}
