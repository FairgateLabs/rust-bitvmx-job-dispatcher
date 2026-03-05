pub mod prover {
    use bitvmx_aws_job_dispatcher::dispatcher_job::{DispatcherJob, ProverJobType, ResultMessage};
    use bitvmx_broker::identification::allow_list::AllowList;
    use bitvmx_broker::identification::identifier::Identifier;
    use bitvmx_broker::rpc::BrokerConfig;
    use bitvmx_broker::{channel::channel::DualChannel, rpc::tls_helper::Cert};
    use std::net::{IpAddr, Ipv4Addr};
    use std::{fs, thread::sleep, time::Duration};
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
        let privk = fs::read_to_string("../test_cert/services.key")?;
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
                port,
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
                "target/riscv-guest/methods/bitvmx/riscv32im-risc0-zkvm-elf/release/bitvmx.bin"
                    .to_string(),
                "./output.json".to_string(),
            ),
        })?;
        channel.send(&emulator_id, msg)?;

        for _ in 0..10000000 {
            if let Some((msg, _from)) = channel.recv()? {
                println!("Received: {}", msg);
                let result_msg = ResultMessage::from_str(&msg)?;
                let result = ProverResultType::from_json_string(result_msg.result)
                    .map_err(|e| anyhow::anyhow!(e))?;
                println!("Result: {:?}", result);
                break;
            } else {
                sleep(Duration::from_secs(1));
            }
        }

        Ok(())
    }
}

#[allow(dead_code)]
fn main() {
    if let Err(e) = prover::run_proof(10000) {
        eprintln!("Error: {}", e);
    }
}
