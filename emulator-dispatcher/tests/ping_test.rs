use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
};

use anyhow::Result;

use test_helper::test_helper::{get_storage_path, init_server, init_trace, remove_storage_path};
use tracing::info;

use crate::{common::start_emulator, ping::Paths};
#[path = "../examples/ping.rs"]
mod ping;

mod common;

const PORT: u16 = 10300;

#[test]
fn test_dispatcher_ping() -> Result<(), anyhow::Error> {
    init_trace();
    let storage_path = get_storage_path();
    {
        let mut server_handler = init_server(PORT)?;
        let running_dispatcher = Arc::new(AtomicBool::new(true));
        let emulator_handler =
            start_emulator(running_dispatcher.clone(), storage_path.to_string())?;
        let challenge_handler = start_ping(PORT)?;
        std::thread::sleep(std::time::Duration::from_secs(9));

        challenge_handler.join().unwrap();
        info!("Ping finished, shutting everything down...");
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

fn start_ping(port: u16) -> Result<thread::JoinHandle<()>, anyhow::Error> {
    let path = Paths::new("../");
    let handle = thread::spawn(move || {
        ping::emulator::run_job(path, port).unwrap();
    });
    Ok(handle)
}
