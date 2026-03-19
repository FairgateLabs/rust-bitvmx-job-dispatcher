use std::{
    fs,
    net::{IpAddr, Ipv4Addr},
    rc::Rc,
    sync::{Arc, Mutex, Once},
    thread::sleep,
    time::Duration,
};

use bitvmx_broker::{
    channel::channel::DualChannel,
    identification::{allow_list::AllowList, identifier::Identifier, routing::RoutingTable},
    rpc::{BrokerConfig, sync_server::BrokerSync, tls_helper::Cert},
};
use bitvmx_job_dispatcher::{get_storage_with_path, helper::remove_storage_path};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use storage_backend::storage::Storage;
use tokio::runtime::Runtime;
use tracing::info;
use tracing_subscriber::EnvFilter;

pub const PORT: u16 = 10300;

// ======================================================
// Tracing / Logging Initialization
// ======================================================

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

// ======================================================
// Broker / Server Setup
// ======================================================

pub fn init_server(port: u16) -> Result<BrokerSync, anyhow::Error> {
    let privk = fs::read_to_string("test-helper/cert/services.key").unwrap();
    let cert = Cert::new_with_privk(&privk).unwrap();
    let allow_list = AllowList::new();
    allow_list.lock().unwrap().allow_all();

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

pub fn config_broker(
    rt: Option<Arc<Mutex<Runtime>>>,
    storage_path: &str,
    paths: &Paths,
) -> (DualChannel, Duration, Rc<Storage>) {
    let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let my_id = 1;

    let privk = fs::read_to_string(&paths.job_dispatcher_key).unwrap();
    let cert = Cert::new_with_privk(&privk).unwrap();

    let allow_list = AllowList::new();
    allow_list.lock().unwrap().allow_all();

    let config = BrokerConfig::new(PORT, Some(IpAddr::from(ip)), cert.get_pubk_hash().unwrap());

    let channel = match rt {
        Some(rt) => {
            DualChannel::new_with_runtime(&config, cert, Some(my_id), allow_list, rt).unwrap()
        }
        None => DualChannel::new(&config, cert, Some(my_id), allow_list).unwrap(),
    };

    let check_interval = Duration::from_secs(1);

    let storage = get_storage_with_path(&storage_path).unwrap();

    (channel, check_interval, storage)
}

// ======================================================
// Paths Configuration
// ======================================================

#[derive(Clone)]
pub struct Paths {
    pub privk: String,
    pub job_dispatcher_key: String,
    pub yaml_path: String,
    pub checkpoint_prover_base: String,
    pub checkpoint_verifier_base: String,
    pub commands_file: String,
}

pub enum JobType {
    Emulator,
    Risczero,
    Garbled,
}

impl Paths {
    pub fn new(path_corrector: &str, job_type: JobType) -> Self {
        let my_privk = match job_type {
            JobType::Emulator => format!("{}test-helper/cert/emulator.key", path_corrector),
            JobType::Risczero => format!("{}test-helper/cert/prover.key", path_corrector),
            JobType::Garbled => format!("{}test-helper/cert/prover.key", path_corrector),
        };
        Self {
            privk: format!("{}test-helper/cert/services.key", path_corrector),
            job_dispatcher_key: my_privk,
            yaml_path: format!(
                "{}../BitVMX-CPU/docker-riscv32/riscv32/build/hello-world.yaml",
                path_corrector
            ),
            checkpoint_prover_base: format!("{}temp-runs/challenge/prover/", path_corrector),
            checkpoint_verifier_base: format!("{}temp-runs/challenge/verifier/", path_corrector),
            commands_file: format!("{}temp-runs/commands.json", path_corrector),
        }
    }
}

// ======================================================
// Example Utilities
// ======================================================

pub fn configure_example_broker(
    paths: &Paths,
    port: u16,
) -> Result<(DualChannel, Identifier), anyhow::Error> {
    let privk = fs::read_to_string(paths.privk.clone())?;

    let my_id = 2;
    let dest_id = 1;

    let cert = Cert::new_with_privk(&privk)?;

    let allow_list = AllowList::new();
    allow_list.lock().unwrap().allow_all();

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

    let privk = fs::read_to_string(paths.job_dispatcher_key.clone())?;
    let cert = Cert::new_with_privk(&privk)?;
    let job_dispatcher_id = Identifier {
        pubkey_hash: cert.get_pubk_hash()?,
        id: dest_id,
    };

    Ok((channel, job_dispatcher_id))
}

pub fn wait_for_result<T, F>(
    channel: &DualChannel,
    max_attempts: usize,
    delay_secs: u64,
    mut parser: F,
) -> Result<T, anyhow::Error>
where
    F: FnMut(&str) -> Result<Option<T>, anyhow::Error>,
{
    for _ in 0..max_attempts {
        if let Some((msg, _from)) = channel.recv()? {
            info!("Received message: {}", msg);

            if let Some(result) = parser(&msg)? {
                return Ok(result);
            }
        } else {
            info!("Waiting result execution");
            sleep(Duration::from_secs(delay_secs));
        }
    }

    Err(anyhow::anyhow!(
        "Timeout: did not receive expected result in {} attempts",
        max_attempts
    ))
}

// ======================================================
// Integration Test Flows
// ======================================================

pub fn run_flow<S, C, CR>(
    storage_path: String,
    start_worker: S,
    start_challenge: C,
    paths: Paths,
) -> Result<(), anyhow::Error>
where
    S: Fn(Arc<AtomicBool>, String, Paths) -> Result<JoinHandle<Result<(), String>>, anyhow::Error>,
    C: Fn(u16) -> std::thread::JoinHandle<CR>,
{
    std::env::set_current_dir("..").unwrap();
    init_trace();

    let mut server_handler = init_server(PORT)?;
    let running = Arc::new(AtomicBool::new(true));

    let worker_handler = start_worker(running.clone(), storage_path.clone(), paths.clone())?;
    let challenge_handler = start_challenge(PORT);

    std::thread::sleep(Duration::from_secs(12));

    info!("⛔ Shutting down worker...");
    running.store(false, Ordering::SeqCst);

    if let Err(msg) = worker_handler.join().unwrap() {
        assert!(
            msg.contains("Expected"),
            "Worker crashed unexpectedly: {msg}"
        );
    }

    std::thread::sleep(Duration::from_secs(4));

    info!("🔄 Restarting worker...");
    let running = Arc::new(AtomicBool::new(true));
    let worker_handler = start_worker(running.clone(), storage_path.clone(), paths.clone())?;
    info!("✅ Worker restarted");

    challenge_handler.join().unwrap();

    info!("Challenge finished, shutting everything down...");
    server_handler.close();
    running.store(false, Ordering::SeqCst);

    if let Err(msg) = worker_handler.join().unwrap() {
        assert!(
            msg.contains("Expected"),
            "Worker crashed unexpectedly: {msg}"
        );
    }

    remove_storage_path(&storage_path);

    Ok(())
}

pub fn run_ping_flow<S, P>(
    storage_path: String,
    start_worker: S,
    start_ping: P,
    paths: Paths,
) -> Result<(), anyhow::Error>
where
    S: Fn(
        Arc<AtomicBool>,
        String,
        Paths,
    ) -> Result<std::thread::JoinHandle<Result<(), String>>, anyhow::Error>,
    P: Fn(u16) -> std::thread::JoinHandle<Result<(), anyhow::Error>>,
{
    let mut server_handler = init_server(PORT)?;
    let running = Arc::new(AtomicBool::new(true));

    let worker_handler = start_worker(running.clone(), storage_path.clone(), paths.clone())?;
    let ping_handler = start_ping(PORT);

    ping_handler.join().unwrap()?;

    info!("Ping finished, shutting everything down...");
    server_handler.close();
    running.store(false, Ordering::SeqCst);

    if let Err(msg) = worker_handler.join().unwrap() {
        assert!(
            msg.contains("Expected"),
            "Worker crashed unexpectedly: {msg}"
        );
    }

    remove_storage_path(&storage_path);

    Ok(())
}
