use std::{
    fs,
    net::{IpAddr, Ipv4Addr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::Result;
use bitvmx_broker::{
    channel::channel::DualChannel,
    identification::allow_list::AllowList,
    rpc::{tls_helper::Cert, BrokerConfig},
};

use bitvmx_job_dispatcher::{dispatcher_loop, get_storage_with_path};
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

    #[arg(long, default_value = "1")]
    my_id: u8,

    /// Path to the private key file
    #[arg(long, default_value = "../rust-bitvmx-broker/certs/services.key")]
    privkey_path: String,

    /// Path to storage database
    #[arg(long, default_value = "temp-runs/storage_job.db")]
    storage_path: String,
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

fn main() -> Result<(), anyhow::Error> {
    init_trace()?;
    let args = Command::parse();

    info!("Starting...");

    let ip = args
        .ip
        .parse::<Ipv4Addr>()
        .map(|ip| ip.octets())
        .expect("Invalid IPv4 address");

    //TODO: obtain these values from a config file
    let my_id = args.my_id;
    let privk = fs::read_to_string(&args.privkey_path)?;

    let cert = Cert::new_with_privk(&privk)?;
    let allow_list =
        AllowList::from_certs(vec![cert.clone()], vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])?;

    let config: BrokerConfig =
        BrokerConfig::new(args.port, Some(IpAddr::from(ip)), cert.get_pubk_hash()?);
    let channel = DualChannel::new(&config, cert, Some(my_id), allow_list)?;
    let check_interval = std::time::Duration::from_secs(1);

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    let storage = get_storage_with_path(&args.storage_path)?;

    dispatcher_loop::<EmulatorJobType>(channel, check_interval, running, storage)?;

    info!("Shutting down...");

    Ok(())
}
