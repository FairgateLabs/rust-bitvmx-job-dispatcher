#[cfg(feature = "prover")]
mod prover{
    use std::{result, thread::sleep, time::Duration};
    use bitvmx_broker::channel::channel::DualChannel;
    use bitvmx_broker::rpc::BrokerConfig;
    use bitvmx_job_dispatcher::{
        dispatcher::dispatcher_job::DispatcherJob, 
        message_type::prover_messages::{
            ProverJobType, 
            ProverResultType
        },
    };

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
            job_type: ProverJobType::ProveStark(
                "./stark-proof.bin".to_string(),
                "./output.json".to_string(),
            ),
        })?;
        channel.send(10, msg)?;

        let mut stark_proved = false;

        for _ in 0..100 {
            if let Some((msg, _from)) = channel.recv()? {
                println!("Received: {}", msg);
                let result = ProverResultType::from_value(msg)?;
                stark_proved = result.is_prove_stark();
            } else {
                println!("Waiting result execution");
                sleep(Duration::from_secs(1));
            }
        }
        if stark_proved {
            let msg = serde_json::to_string(&DispatcherJob {
                job_id: "uid_job_2".to_string(),
                job_type: ProverJobType::ProveSnark(
                    "stark-proof.bin".to_string(),
                    "snark-seal.json".to_string(),
                    "output.json".to_string(),
                ),
            })?;
            channel.send(10, msg)?;

            for _ in 0..1000 {
                if let Some((msg, _from)) = channel.recv()? {
                    println!("Received: {}", msg);
                    let result = ProverResultType::from_value(msg)?;
                    if result.is_prove_snark() {
                        println!("✅ Prover finished successfully");
                    } else {
                        println!("❌ Error: Prover failed");
                    }
                    break;
                } else {
                    println!("Waiting result execution");
                    sleep(Duration::from_secs(1));
                }
            }
            
        } else {
            println!("❌ Error: Prover failed");
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
