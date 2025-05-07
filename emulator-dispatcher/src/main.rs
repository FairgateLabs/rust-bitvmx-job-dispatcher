use std::{
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::Result;
use bitvmx_broker::{channel::channel::DualChannel, rpc::BrokerConfig};

use bitvmx_job_dispatcher::dispatcher_loop;
use bitvmx_job_dispatcher_types::EmulatorJobType;
use tracing::info;
use tracing_subscriber::{
    fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

fn init_trace() -> Result<(), anyhow::Error> {
    let filter = EnvFilter::builder()
        .parse("info,tarpc=off") // Include everything at "info" except `libp2p`
        .expect("Invalid filter");

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_span_events(FmtSpan::NEW | FmtSpan::CLOSE))
        .try_init()?;

    Ok(())
}
fn main() -> Result<(), anyhow::Error> {
    init_trace()?;

    info!("Starting...");

    let config = BrokerConfig::new(10000, Some(IpAddr::from([127, 0, 0, 1])));
    let channel = DualChannel::new(&config, 10);
    let check_interval = std::time::Duration::from_secs(1);

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    dispatcher_loop::<EmulatorJobType>(channel, check_interval, running)?;

    info!("Shutting down...");

    Ok(())
}
