use crate::common::start_dispatcher;
use anyhow::Result;
use std::thread;
use test_helper::test_helper::{get_storage_path, init_trace, run_ping_flow};

mod common;
#[path = "../examples/aws_ping.rs"]
mod ping;

#[test]
fn test_aws_ping() -> Result<(), anyhow::Error> {
    init_trace();
    std::env::set_current_dir("..").unwrap();

    let storage_path = get_storage_path();
    let config_path = "aws-service/config/config.yaml".to_string();

    run_ping_flow(
        storage_path,
        move |running, storage| start_dispatcher(running, storage, config_path.clone()),
        |port| start_ping(port),
    )
}

fn start_ping(port: u16) -> thread::JoinHandle<Result<(), anyhow::Error>> {
    let handle = thread::spawn(move || ping::prover::run_job(port));
    handle
}
