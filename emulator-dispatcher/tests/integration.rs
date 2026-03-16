use crate::common::start_emulator;
use anyhow::Result;
use bitvmx_job_dispatcher::helper::get_storage_path;
use std::thread;
use test_helper::test_helper::{run_flow, Paths};

#[path = "../examples/challenge.rs"]
mod challenge;

mod common;

#[test]
fn test_emulator_dispatcher() -> Result<(), anyhow::Error> {
    let path = Paths::new("", test_helper::test_helper::JobType::Emulator);
    let storage_path = get_storage_path();
    run_flow(
        storage_path,
        |running, storage, paths| start_emulator(running, storage, paths),
        |port| start_challenge(port).unwrap(),
        path,
    )
}

fn start_challenge(port: u16) -> Result<thread::JoinHandle<()>, anyhow::Error> {
    let path = Paths::new("", test_helper::test_helper::JobType::Emulator);
    let handle = thread::spawn(move || {
        challenge::emulator::run_job(path, port).unwrap();
    });
    Ok(handle)
}
