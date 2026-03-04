use anyhow::Result;
use bitvmx_aws_job_dispatcher::{dispatcher_loop, init_trace};
use bitvmx_broker::{
    channel::channel::DualChannel,
    identification::allow_list::AllowList,
    rpc::{BrokerConfig, tls_helper::Cert},
};
use std::{
    fs,
    net::{IpAddr, Ipv4Addr},
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use storage_backend::{storage::Storage, storage_config::StorageConfig};

use clap::Parser;
use tracing::{error, info};

#[derive(Parser)]
#[command(about = "Emulator Dispatcher CLI", long_about = None)]
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
}

fn main() -> Result<(), anyhow::Error> {
    init_trace();
    let args = Command::parse();

    info!("Starting...");
    let rt = Arc::new(Mutex::new(tokio::runtime::Runtime::new()?));

    let ip = args
        .ip
        .parse::<Ipv4Addr>()
        .map(|ip| ip.octets())
        .expect("Invalid IPv4 address");

    //TODO: obtain these values from a config file
    let my_id = args.my_id;
    let privk = fs::read_to_string(&args.my_priv_key)?;

    let cert = Cert::new_with_privk(&privk)?;

    let allow_list = AllowList::new();
    allow_list.lock().unwrap().allow_all();

    let config: BrokerConfig =
        BrokerConfig::new(args.port, Some(IpAddr::from(ip)), args.broker_pubk_hash);
    let channel =
        DualChannel::new_with_runtime(&config, cert, Some(my_id), allow_list, rt.clone())?;

    let check_interval = std::time::Duration::from_secs(1);

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    let storage_config = StorageConfig::new(args.storage_path, None);
    let storage = Rc::new(Storage::new(&storage_config)?);

    dispatcher_loop(
        channel,
        check_interval,
        running,
        rt,
        storage,
        "./config.yaml".to_string(),
    )
    .map_err(|e| {
        error!("Dispatcher loop error: {}", e);
        e
    })?;

    info!("Shutting down...");

    Ok(())
}
