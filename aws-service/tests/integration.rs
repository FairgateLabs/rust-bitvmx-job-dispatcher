use crate::common::start_dispatcher;
use anyhow::Result;
use std::thread;
use test_helper::test_helper::{get_storage_path, run_flow};
#[path = "../examples/aws.rs"]
mod aws;
mod common;

#[ignore]
#[test]
fn test_aws_dispatcher() -> Result<(), anyhow::Error> {
    let storage_path = get_storage_path();
    let config_path = "aws-service/config/config.yaml".to_string();
    run_flow(
        storage_path,
        move |running, storage| start_dispatcher(running, storage, config_path.clone()),
        |port| start_zkp(port),
    )
}

fn start_zkp(port: u16) -> thread::JoinHandle<Result<(), anyhow::Error>> {
    let handle = thread::spawn(move || aws::prover::run_proof(port));
    handle
}
