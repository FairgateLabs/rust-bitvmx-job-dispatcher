use std::{
    fs,
    net::{IpAddr, Ipv4Addr},
    path,
    sync::{Arc, Mutex, Once},
};

use bitvmx_broker::{
    identification::{allow_list::AllowList, routing::RoutingTable},
    rpc::{BrokerConfig, sync_server::BrokerSync, tls_helper::Cert},
};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

pub fn get_storage_path() -> String {
    let storage_path = format!("temp-runs/storage_job_{}.db", std::process::id());
    if path::Path::new(&storage_path).exists() {
        remove_storage_path(&storage_path);
    }
    storage_path
}

pub fn remove_storage_path(storage_path: &str) {
    // clean up the test’s storage file
    info!("Cleaning up storage file: {}", storage_path);
    if path::Path::new(&storage_path).exists() {
        fs::remove_dir_all(&storage_path)
            .unwrap_or_else(|e| error!("Warning: could not remove storage file: {e}"))
    }
}

pub fn init_server(port: u16) -> Result<BrokerSync, anyhow::Error> {
    let privk = fs::read_to_string("test_cert/services.key").unwrap();
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

static INIT: Once = Once::new();

pub fn init_trace() {
    INIT.call_once(|| {
        config_trace_aux();
    });
}

fn config_trace_aux() {
    let default_modules = ["info", "tarpc=off"];

    let filter = EnvFilter::builder()
        .parse(default_modules.join(","))
        .expect("Invalid filter");

    tracing_subscriber::fmt()
        .with_target(true)
        .with_env_filter(filter)
        .init();
}
