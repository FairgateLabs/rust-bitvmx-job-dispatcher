use std::{
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use anyhow::Result;
use bitvmx_broker::{
    channel::channel::DualChannel,
    identification::{allow_list::AllowList, routing::RoutingTable},
    rpc::{sync_server::BrokerSync, tls_helper::Cert, BrokerConfig},
};

use bitvmx_job_dispatcher::dispatcher_loop;
use bitvmx_job_dispatcher_types::emulator_messages::EmulatorJobType;
use tracing::{error, info};
use tracing_subscriber::{
    fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

use crate::challenge::Paths;

#[path = "../examples/challenge.rs"]
mod challenge;

const PORT: u16 = 10000;

#[test]
fn test_emulator_dispatcher() -> Result<(), anyhow::Error> {
    init_trace()?;
    let storage_path = get_storage_path();
    let mut server_handler = init_server(PORT)?;
    let running_dispatcher = Arc::new(AtomicBool::new(true));
    let _emulator_handler = start_emulator(running_dispatcher.clone(), storage_path.to_string())?;
    let challenge_handler = start_challenge()?;
    std::thread::sleep(std::time::Duration::from_secs(9));

    info!("⛔ Shutting down dispatcher...");
    running_dispatcher.store(false, Ordering::SeqCst);
    std::thread::sleep(std::time::Duration::from_secs(5));

    info!("🔄 Restarting emulator...");
    let running_dispatcher = Arc::new(AtomicBool::new(true));
    let emulator_handler = start_emulator(running_dispatcher.clone(), storage_path.clone())?;
    info!("✅ Emulator restarted");

    challenge_handler.join().unwrap();
    info!("Challenge finished, shutting everything down...");
    server_handler.close();
    running_dispatcher.store(false, Ordering::SeqCst);
    if let Err(msg) = emulator_handler.join().unwrap() {
        assert!(
            msg.contains("Expected"),
            "Emulator crashed unexpectedly: {msg}"
        );
    }
    remove_storage_file(&storage_path);
    Ok(())
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

fn get_storage_path() -> String {
    let storage_path = format!("../storage_job_{}.db", std::process::id());
    if path::Path::new(&storage_path).exists() {
        fs::remove_file(&storage_path)
            .unwrap_or_else(|e| error!("Warning: could not remove old storage file: {e}"));
    }
    storage_path
}

fn remove_storage_file(storage_path: &str) {
    // clean up the test’s storage file
    if path::Path::new(&storage_path).exists() {
        fs::remove_file(&storage_path)
            .unwrap_or_else(|e| error!("Warning: could not remove storage file: {e}"))
    }
}

fn start_emulator(
    running: Arc<AtomicBool>,
    storage_path: String,
) -> Result<thread::JoinHandle<Result<(), String>>, anyhow::Error> {
    let handle = thread::spawn(move || {
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let port = 10000;
        let my_id = 1;
        let privk = fs::read_to_string("../../rust-bitvmx-broker/certs/services.key").unwrap();

        let my_address = SocketAddr::new(IpAddr::from(ip), port);

        let cert = Cert::new_with_privk(&privk).unwrap();
        let allow_list =
            AllowList::from_certs(vec![cert.clone()], vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
                .unwrap();

        let config =
            BrokerConfig::new(port, Some(IpAddr::from(ip)), cert.get_pubk_hash().unwrap()).unwrap();
        let channel =
            DualChannel::new(&config, cert, Some(my_id), my_address, Some(allow_list)).unwrap();

        let check_interval = Duration::from_secs(1);

        if let Err(e) =
            dispatcher_loop::<EmulatorJobType>(channel, check_interval, running, storage_path)
        {
            return Err(format!("dispatcher error: {e}"));
        }

        Err("Expected abrupt end".to_string())
    });

    Ok(handle)
}

fn start_challenge() -> Result<thread::JoinHandle<()>, anyhow::Error> {
    let path = Paths::new("../");
    let handle = thread::spawn(move || {
        challenge::emulator::run_job(path).unwrap();
    });
    Ok(handle)
}

fn init_server(port: u16) -> Result<BrokerSync, anyhow::Error> {
    let privk = fs::read_to_string("../../rust-bitvmx-broker/certs/services.key").unwrap();
    let cert = Cert::new_with_privk(&privk).unwrap();
    let allow_list =
        AllowList::from_certs(vec![cert.clone()], vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]).unwrap();
    let routing = RoutingTable::new();
    routing.lock().unwrap().allow_all();
    let config = BrokerConfig::new(port, None, cert.get_pubk_hash().unwrap()).unwrap();

    let storage = Arc::new(Mutex::new(
        bitvmx_broker::broker_memstorage::MemStorage::new(),
    ));

    let server =
        BrokerSync::new(&config, storage.clone(), cert, allow_list.clone(), routing).unwrap();
    Ok(server)
}
