use crate::common::start_emulator;
use anyhow::Result;
use std::thread;
use test_helper::test_helper::{get_storage_path, run_flow, Paths};

#[path = "../examples/challenge.rs"]
mod challenge;

mod common;

#[test]
fn test_emulator_dispatcher() -> Result<(), anyhow::Error> {
    let storage_path = get_storage_path();
    run_flow(
        storage_path,
        |running, storage| start_emulator(running, storage),
        |port| start_challenge(port).unwrap(),
    )
}

fn start_challenge(port: u16) -> Result<thread::JoinHandle<()>, anyhow::Error> {
    let path = Paths::new("");
    let handle = thread::spawn(move || {
        challenge::emulator::run_job(path, port).unwrap();
    });
    Ok(handle)
}
