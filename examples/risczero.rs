#[cfg(feature = "prover")]
mod prover {
    use bitvmx_broker::channel::channel::DualChannel;
    use bitvmx_broker::rpc::BrokerConfig;
    use bitvmx_job_dispatcher::{
        dispatcher::dispatcher_job::DispatcherJob,
        message_type::prover_messages::ProverJobType,
    };
    use std::{thread::sleep, time::Duration};
    use zk_result::ResultType as ProverResultType;

    // To make this example work, you need to:
    // 1. Go to the `rust-bitvmx-zk-proof` folder and follow the instructions in README.md
    //    until the step "Template Setup"
    // 3. run the server example first.
    //      cargo run --release --example server -- --port 10000
    // 4. Then run the job-dispatcher
    //      cargo run --release --features "prover"
    // 5. Then trigger one execution
    //      cargo run --release --example risczero --features "prover"

    pub(crate) fn run_proof() -> Result<(), anyhow::Error> {
        let channel = DualChannel::new(&BrokerConfig::new(10000, None), 2);
        let msg = serde_json::to_string(&DispatcherJob {
            job_id: "uid_job".to_string(),
            job_type: ProverJobType::Prove(
                50_u32.to_be_bytes().to_vec(),
                "./a.elf".to_string(),
                "./output.json".to_string(),
            ),
        })?;
        channel.send(10, msg)?;

        for _ in 0..1000 {
            if let Some((msg, _from)) = channel.recv()? {
                println!("Received: {}", msg);
                let result = ProverResultType::from_json_string(msg)
                    .map_err(|e| anyhow::anyhow!(e))?;
                println!("Result: {:?}", result);
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
    #[cfg(feature = "prover")]
    {
        if let Err(e) = prover::run_proof() {
            eprintln!("Error: {}", e);
        }
    }

    #[cfg(not(feature = "prover"))]
    println!("Run with '--features prover' to run this example");
}
