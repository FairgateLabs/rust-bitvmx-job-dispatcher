mod prover {
    use bitvmx_broker::identification::allow_list::AllowList;
    use bitvmx_broker::identification::identifier::Identifier;
    use bitvmx_broker::rpc::BrokerConfig;
    use bitvmx_broker::{channel::channel::DualChannel, rpc::tls_helper::Cert};
    use bitvmx_job_dispatcher::dispatcher_job::DispatcherJob;
    use bitvmx_job_dispatcher_types::prover_messages::ProverJobType;
    use std::net::{IpAddr, Ipv4Addr};
    use std::{fs, thread::sleep, time::Duration};
    use zk_result::ResultType as ProverResultType;

    // To make this example work, you need to:
    // 1. Go to the `rust-bitvmx-zk-proof` folder and follow the instructions in README.md
    //    until the step "Template Setup"
    // 3. run the server example first.
    //      cargo run --release --bin bitvmx-emulator-dispatcher -- --port 10000 --my-id 1 --dest-id 2
    // 4. Then run the job-dispatcher
    //      cargo run --release --features "prover"
    // 5. Then trigger one execution
    //      cargo run --release --example risczero --features "prover"

    pub(crate) fn run_proof() -> Result<(), anyhow::Error> {
        let privk = fs::read_to_string("../rust-bitvmx-broker/certs/services.key")?;
        let my_id = 2;
        let dest_id = 1;
        let cert = Cert::new_with_privk(&privk)?;
        let allow_list =
            AllowList::from_certs(vec![cert.clone()], vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])?;
        let emulator_id = Identifier {
            pubkey_hash: cert.get_pubk_hash()?,
            id: dest_id,
        };

        let channel = DualChannel::new(
            &BrokerConfig::new(
                10000,
                Some(IpAddr::from([127, 0, 0, 1])),
                cert.get_pubk_hash()?,
            ),
            cert,
            Some(my_id),
            allow_list,
        )?;
        let msg = serde_json::to_string(&DispatcherJob {
            job_id: "uid_job".to_string(),
            job_type: ProverJobType::Prove(
                50_u32.to_be_bytes().to_vec(),
                "./a.elf".to_string(),
                ".".to_string(),
            ),
        })?;
        channel.send(emulator_id, msg)?;

        for _ in 0..1000 {
            if let Some((msg, _from)) = channel.recv()? {
                println!("Received: {}", msg);
                let result =
                    ProverResultType::from_json_string(msg).map_err(|e| anyhow::anyhow!(e))?;
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
    if let Err(e) = prover::run_proof() {
        eprintln!("Error: {}", e);
    }
}
