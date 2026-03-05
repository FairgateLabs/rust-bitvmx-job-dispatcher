use std::{
    fs,
    net::{IpAddr, Ipv4Addr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use anyhow::Result;
use bitvmx_broker::{
    channel::channel::DualChannel,
    identification::allow_list::AllowList,
    rpc::{tls_helper::Cert, BrokerConfig},
};

use bitvmx_job_dispatcher::{dispatcher_loop, get_storage_with_path};
use bitvmx_job_dispatcher_types::emulator_messages::EmulatorJobType;
use test_helper::test_helper::{get_storage_path, init_server, init_trace, remove_storage_path};
use tracing::info;

use crate::challenge::Paths;

#[path = "../examples/challenge.rs"]
mod challenge;

const PORT: u16 = 10300;

#[test]
fn test_emulator_dispatcher() -> Result<(), anyhow::Error> {
    std::env::set_current_dir("..").unwrap();
    init_trace();
    let storage_path = get_storage_path();
    {
        let mut server_handler = init_server(PORT)?;
        let running_dispatcher = Arc::new(AtomicBool::new(true));
        let emulator_handler =
            start_emulator(running_dispatcher.clone(), storage_path.to_string())?;
        let challenge_handler = start_challenge(PORT)?;
        std::thread::sleep(std::time::Duration::from_secs(9));

        info!("⛔ Shutting down dispatcher...");
        running_dispatcher.store(false, Ordering::SeqCst);
        if let Err(msg) = emulator_handler.join().unwrap() {
            assert!(
                msg.contains("Expected"),
                "Emulator crashed unexpectedly: {msg}"
            );
        }

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
    }
    remove_storage_path(&storage_path);
    Ok(())
}

fn start_emulator(
    running: Arc<AtomicBool>,
    storage_path: String,
) -> Result<thread::JoinHandle<Result<(), String>>, anyhow::Error> {
    let handle = thread::spawn(move || {
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let my_id = 1;
        let privk = fs::read_to_string("test_cert/services.key").unwrap();

        let cert = Cert::new_with_privk(&privk).unwrap();
        let allow_list =
            AllowList::from_certs(vec![cert.clone()], vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
                .unwrap();

        let config = BrokerConfig::new(PORT, Some(IpAddr::from(ip)), cert.get_pubk_hash().unwrap());
        let channel = DualChannel::new(&config, cert, Some(my_id), allow_list).unwrap();

        let check_interval = Duration::from_secs(1);

        let storage = get_storage_with_path(&storage_path).unwrap();
        if let Err(e) =
            dispatcher_loop::<EmulatorJobType>(channel, check_interval, running, storage)
        {
            return Err(format!("dispatcher error: {e}"));
        }
        info!("Dispatcher loop exited normally");

        Err("Expected abrupt end".to_string())
    });

    Ok(handle)
}

fn start_challenge(port: u16) -> Result<thread::JoinHandle<()>, anyhow::Error> {
    let path = Paths::new("");
    let handle = thread::spawn(move || {
        challenge::emulator::run_job(path, port).unwrap();
    });
    Ok(handle)
}
