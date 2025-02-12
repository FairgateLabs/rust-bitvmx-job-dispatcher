use std::{
    io::{BufReader, Read},
    net::IpAddr,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::Result;
use bitvmx_broker::{channel::channel::DualChannel, rpc::BrokerConfig};
//use bitvmx_workers_messages::EmulatorExecute;
use tracing::{error, info};
use tracing_subscriber::{
    fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

pub fn process_msg(msg: &str) -> Option<(Child, BufReader<std::process::ChildStdout>)> {
    //let msg: EmulatorExecute = serde_json::from_str(msg).unwrap();
    info!("Received: {:?}", msg);

    let mut child = Command::new("../BitVMX-CPU/target/release/emulator")
        //.stdin(Stdio::piped())
        .args([
            "execute",
            "--elf",
            "../BitVMX-CPU/docker-riscv32/riscv32/build/hello-world.elf",
            "--debug",
            "--limit",
            "20",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to start worker");

    // If necessary to send through stdin
    /*if let Some(mut stdin) = child.stdin.take() {
        writeln!(stdin, "{}", task).expect("Failed to write to worker");
    }*/

    let stdout = child.stdout.take().expect("Failed to capture stdout");
    let reader = BufReader::new(stdout);

    Some((child, reader))
}

fn init_trace() -> Result<(), anyhow::Error> {
    let filter = EnvFilter::builder()
        .parse("info,tarpc=off") // Include everything at "info" except `libp2p`
        .expect("Invalid filter");

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_span_events(FmtSpan::NEW | FmtSpan::CLOSE))
        .try_init()?;

    Ok(())
}

fn dispatcher_loop(
    channel: DualChannel,
    check_interval: Duration,
    running: Arc<AtomicBool>,
) -> Result<(), anyhow::Error> {
    let mut workers: Vec<(Child, BufReader<std::process::ChildStdout>, u32)> = Vec::new();
    while running.load(Ordering::SeqCst) {
        let msg = channel.recv();
        match msg {
            Ok(msg) => {
                if let Some(msg) = msg {
                    if let Some((child, reader)) = process_msg(&msg.0) {
                        workers.push((child, reader, msg.1));
                    }
                }
            }
            Err(e) => error!("Error: {:?}", e),
        }

        if !workers.is_empty() {
            workers.retain_mut(|(child, reader, id)| {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        let mut buf = String::new();
                        let _ = reader.read_to_string(&mut buf);
                        //TODO: process message before sending out
                        info!("Worker output: {}", buf);
                        info!("Worker exited with status: {:?}", status);
                        channel
                            .send(
                                *id,
                                format!("Worker exited with status: {}\nResult: {}", status, buf),
                            )
                            .unwrap();
                        false // Remove from the list
                    }
                    Ok(None) => true, // Still running keep it
                    Err(e) => {
                        error!("Error checking worker: {}", e);
                        channel
                            .send(*id, "Error executiong worker".to_string())
                            .unwrap();
                        false // Remove from the list
                    }
                }
            });
        }

        std::thread::sleep(check_interval);
    }
    Ok(())
}

fn main() -> Result<(), anyhow::Error> {
    init_trace()?;

    info!("Starting...");

    let config = BrokerConfig::new(10000, Some(IpAddr::from([127, 0, 0, 1])));
    let channel = DualChannel::new(&config, 10);
    let check_interval = std::time::Duration::from_secs(1);

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    dispatcher_loop(channel, check_interval, running)?;

    info!("Shutting down...");

    Ok(())
}
