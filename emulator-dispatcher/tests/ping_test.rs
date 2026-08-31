use crate::common::start_emulator;
use anyhow::Result;
use bitvmx_job_dispatcher::helper::get_storage_path;
use std::thread;
use test_helper::test_helper::{init_trace, run_ping_flow, Paths};
use tracing::error;
#[path = "../examples/ping.rs"]
mod ping;

mod common;

#[test]
fn test_dispatcher_ping() -> Result<(), anyhow::Error> {
    std::env::set_current_dir("..").unwrap();
    init_trace();

    let storage_path = get_storage_path();
    let path = Paths::new("", test_helper::test_helper::JobType::Emulator);

    run_ping_flow(storage_path, start_emulator, start_ping, path)
}

fn start_ping(port: u16) -> thread::JoinHandle<Result<(), anyhow::Error>> {
    let path = Paths::new("", test_helper::test_helper::JobType::Emulator);

    thread::spawn(move || {
        let result = ping::emulator::run_job(path, port);
        if let Err(err) = result {
            error!("Error in ping job: {err}");
        }
        Ok(())
    })
}
