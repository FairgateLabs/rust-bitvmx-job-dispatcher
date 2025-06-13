use std::{
    net::{IpAddr, Ipv4Addr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::Result;
use bitvmx_broker::{channel::channel::DualChannel, rpc::BrokerConfig};

use bitvmx_job_dispatcher::dispatcher_loop;
use bitvmx_job_dispatcher_types::emulator_messages::EmulatorJobType;
use clap::{command, Parser};
use tracing::info;
use tracing_subscriber::{
    fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

#[derive(Parser)]
#[command(about = "Emulator Dispatcher CLI", long_about = None)]
#[command(arg_required_else_help = true)]
struct Command {
    #[arg(long, default_value = "127.0.0.1")]
    ip: String,
    #[arg(long)]
    port: u16,
}

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
const EMULATOR_ID: u32 = 1000;
fn main() -> Result<(), anyhow::Error> {
    init_trace()?;
    let args = Command::parse();

    info!("Starting...");

    let ip = args
        .ip
        .parse::<Ipv4Addr>()
        .map(|ip| ip.octets())
        .expect("Invalid IPv4 address");

    let config: BrokerConfig = BrokerConfig::new(args.port, Some(IpAddr::from(ip)));
    let channel = DualChannel::new(&config, EMULATOR_ID);
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
