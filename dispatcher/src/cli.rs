use std::{
    fs,
    net::{IpAddr, Ipv4Addr},
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::Result;
use bitvmx_broker::{
    identification::allow_list::AllowList,
    rpc::{tls_helper::Cert, BrokerConfig},
    RemoteChannel,
};

use clap::Parser;
use storage_backend::storage::Storage;
use tracing::info;
use tracing_subscriber::{
    fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

use crate::{dispatcher_error::DispatcherError, get_storage_with_path};

#[derive(Parser)]
#[command(about = "Job Dispatcher CLI", long_about = None)]
#[command(arg_required_else_help = true)]
struct Command {
    #[arg(long, default_value = "127.0.0.1")]
    ip: String,
    #[arg(long)]
    port: u16,

    #[arg(long, default_value = "0")]
    my_id: u8,

    /// Path to the private key file
    #[arg(long, default_value = "../rust-bitvmx-client/config/keys/emulator.key")]
    my_priv_key: String,

    /// PubKeyHash of the broker service
    #[arg(
        long,
        default_value = "155c24337976116159e73e386bb721b8c5e219ae18696ef4fde6b7dedadfc570"
    )]
    broker_pubk_hash: String,

    /// Path to storage database
    #[arg(long, default_value = "temp-runs/storage_job.db")]
    storage_path: String,

    // Path to dispatcher config file (only used for aws dispatcher)
    #[arg(long, default_value = "./aws-helper/config/config.yaml")]
    config_path: Option<String>,

    // Specify the mode to run the dispatcher, either "local" or "aws"
    #[arg(long, default_value = "local")]
    mode: String,
}

fn init_trace() -> Result<(), DispatcherError> {
    let filter = EnvFilter::builder()
        .parse("info,tarpc=off") // Include everything at "info" except `libp2p`
        .expect("Invalid filter");

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_span_events(FmtSpan::NEW | FmtSpan::CLOSE))
        .try_init()?;

    Ok(())
}

pub fn init() -> Result<
    (
        RemoteChannel,
        Duration,
        Arc<AtomicBool>,
        Rc<Storage>,
        Option<String>,
        bool,
    ),
    DispatcherError,
> {
    init_trace()?;
    let args = Command::parse();

    info!("Starting...");

    let ip = args
        .ip
        .parse::<Ipv4Addr>()
        .map(|ip| ip.octets())
        .expect("Invalid IPv4 address");

    let my_id = args.my_id;
    let privk = fs::read_to_string(&args.my_priv_key)?;

    let cert = Cert::new_with_privk(&privk)?;

    let allow_list = AllowList::new();
    allow_list.lock().unwrap().set_allow_all(true);

    let config: BrokerConfig = BrokerConfig::new(
        args.port,
        Some(IpAddr::from(ip)),
        args.broker_pubk_hash,
        None, // Default config
    );
    let channel = RemoteChannel::new(&config, cert, Some(my_id), allow_list)?;
    let check_interval = std::time::Duration::from_secs(1);

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    let storage = get_storage_with_path(&args.storage_path)?;

    Ok((
        channel,
        check_interval,
        running,
        storage,
        args.config_path,
        args.mode == "local",
    ))
}
