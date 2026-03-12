use crate::common::start_emulator;
use anyhow::Result;
use bitvmx_job_dispatcher::helper::get_storage_path;
use std::thread;
use test_helper::test_helper::{init_trace, run_ping_flow, Paths};
#[path = "../examples/ping.rs"]
mod ping;

mod common;

#[test]
fn test_dispatcher_ping() -> Result<(), anyhow::Error> {
    std::env::set_current_dir("..").unwrap();
    init_trace();

    let storage_path = get_storage_path();

    run_ping_flow(
        storage_path,
        |running, storage| start_emulator(running, storage),
        |port| start_ping(port),
    )
}

fn start_ping(port: u16) -> thread::JoinHandle<Result<(), anyhow::Error>> {
    let path = Paths::new("");

    thread::spawn(move || {
        ping::emulator::run_job(path, port)?;
        Ok(())
    })
}
