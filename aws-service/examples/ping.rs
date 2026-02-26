use tracing::error;
use tracing_subscriber::{
    EnvFilter, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt,
};

// To make this example work, you need to:
// 1. run the server example first (from bitvmx-broker).
//      cargo run --release --example server -- --port 10000
// 2. Then run the job-dispatcher
//      cargo run --release --bin bitvmx-emulator-dispatcher -- --port 10000 --my-id 1
// 3. Then trigger one execution
//      cargo run --release --example challenge --features "emulator"

pub mod prover {
    use bitvmx_broker::identification::identifier::Identifier;
    use bitvmx_broker::rpc::BrokerConfig;
    use bitvmx_broker::rpc::tls_helper::Cert;
    use bitvmx_broker::{channel::channel::DualChannel, identification::allow_list::AllowList};
    use dispatcher_utils::PingMessage;
    use std::net::Ipv4Addr;
    use std::{fs, net::IpAddr, thread::sleep, time::Duration};
    use tracing::info;

    pub(crate) fn run_job(port: u16) -> Result<(), anyhow::Error> {
        info!("Starting ping example...");
        let privk = fs::read_to_string("../../rust-bitvmx-broker/certs/services.key")?;
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

        let msg = serde_json::to_string(&PingMessage::Ping)?;
        channel.send(&emulator_id.clone(), msg)?;

        info!("Waiting Pong Response...");
        let msg = wait_for_result(&channel, 10, 1)?;

        match msg {
            PingMessage::Pong => {
                info!("Received Pong");
            }
            PingMessage::Ping => {
                return Err(anyhow::anyhow!("Unexpected Ping message received"));
            }
        }

        Ok(())
    }

    fn wait_for_result(
        channel: &DualChannel,
        max_attempts: usize,
        delay_secs: u64,
    ) -> Result<PingMessage, anyhow::Error> {
        for _ in 0..max_attempts {
            if let Some((msg, _from)) = channel.recv()? {
                info!("Received message: {}", msg);
                let msg = serde_json::from_str::<PingMessage>(&msg)?;
                return Ok(msg);
            } else {
                info!("Waiting result execution");
                sleep(Duration::from_secs(delay_secs));
            }
        }

        Err(anyhow::anyhow!(
            "Timeout: did not receive Pong Response in {} attempts",
            max_attempts
        ))
    }
}

#[allow(dead_code)]
fn main() {
    init_trace().unwrap();
    if let Err(e) = prover::run_job(10000) {
        error!("Error: {}", e);
    }
}

fn init_trace() -> Result<(), anyhow::Error> {
    let filter = EnvFilter::builder()
        .parse("info,tarpc=off") // Include everything at "info"
        .expect("Invalid filter");

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_span_events(FmtSpan::NEW | FmtSpan::CLOSE))
        .try_init()?;
    Ok(())
}
