use std::{
    fs,
    net::{IpAddr, Ipv4Addr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use anyhow::Result;
use bitvmx_broker::{
    channel::channel::DualChannel,
    identification::{allow_list::AllowList, routing::RoutingTable},
    rpc::{BrokerConfig, sync_server::BrokerSync, tls_helper::Cert},
};

use bitvmx_aws_job_dispatcher::dispatcher_loop;
use tokio::runtime::Runtime;
use tracing::{debug, info};
use tracing_subscriber::{
    EnvFilter, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt,
};

#[path = "../examples/ping.rs"]
mod ping;

const PORT: u16 = 10300;

#[test]
fn test_dispatcher_ping() -> Result<(), anyhow::Error> {
    init_trace()?;
    let mut server_handler = init_server(PORT)?;
    let running_dispatcher = Arc::new(AtomicBool::new(true));
    let dispatcher_handler = start_dispatcher(running_dispatcher.clone())?;
    let ping_handler = start_ping(PORT);
    std::thread::sleep(std::time::Duration::from_secs(9));

    ping_handler.join().unwrap()?;
    info!("Ping finished, shutting everything down...");
    server_handler.close();
    running_dispatcher.store(false, Ordering::SeqCst);
    if let Err(msg) = dispatcher_handler.join().unwrap() {
        assert!(
            msg.contains("Expected"),
            "Dispatcher crashed unexpectedly: {msg}"
        );
    }
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

fn start_dispatcher(
    running: Arc<AtomicBool>,
) -> Result<thread::JoinHandle<Result<(), String>>, anyhow::Error> {
    let rt = Arc::new(Mutex::new(Runtime::new().unwrap()));
    let handle = thread::spawn(move || {
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let my_id = 1;
        let privk = fs::read_to_string("../../rust-bitvmx-broker/certs/services.key").unwrap();

        let cert = Cert::new_with_privk(&privk).unwrap();
        let allow_list =
            AllowList::from_certs(vec![cert.clone()], vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
                .unwrap();

        let config = BrokerConfig::new(PORT, Some(IpAddr::from(ip)), cert.get_pubk_hash().unwrap());
        let channel =
            DualChannel::new_with_runtime(&config, cert, Some(my_id), allow_list, rt.clone())
                .unwrap();

        let check_interval = Duration::from_secs(1);
        debug!("Starting dispatcher loop (Test)");
        if let Err(e) = dispatcher_loop(
            channel,
            check_interval,
            running,
            rt.clone(),
            "config/config.json".to_string(),
        ) {
            return Err(format!("dispatcher error: {e}"));
        }

        Err("Expected abrupt end".to_string())
    });

    Ok(handle)
}

fn start_ping(port: u16) -> thread::JoinHandle<Result<(), anyhow::Error>> {
    let handle = thread::spawn(move || ping::prover::run_job(port));
    handle
}

fn init_server(port: u16) -> Result<BrokerSync, anyhow::Error> {
    let privk = fs::read_to_string("../../rust-bitvmx-broker/certs/services.key").unwrap();
    let cert = Cert::new_with_privk(&privk).unwrap();
    let allow_list =
        AllowList::from_certs(vec![cert.clone()], vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]).unwrap();
    let routing = RoutingTable::new();
    routing.lock().unwrap().allow_all();
    let config = BrokerConfig::new(port, None, cert.get_pubk_hash().unwrap());

    let storage = Arc::new(Mutex::new(
        bitvmx_broker::broker_memstorage::MemStorage::new(),
    ));

    let server =
        BrokerSync::new(&config, storage.clone(), cert, allow_list.clone(), routing).unwrap();
    Ok(server)
}
