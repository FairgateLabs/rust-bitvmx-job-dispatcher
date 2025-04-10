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
// use bitvmx_emulator_job::handler::EmulatorDispatcher;
use bitvmx_prover_job::handler::ProverDispatcher;
use tracing::{error, info};
use tracing_subscriber::{
    fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

pub fn process_msg(
    emulator_dispatcher: &mut ProverDispatcher,
    msg: &str,
) -> Option<(Child, BufReader<std::process::ChildStdout>, String)> {
    info!("Received: {:?}", msg);

    let (cmd, args, job_id) = emulator_dispatcher.process_msg(msg).ok()?;

    let child = Command::new(cmd).args(args).stdout(Stdio::piped()).spawn();

    if let Err(e) = child {
        error!("Error executing command: {}", e);
        emulator_dispatcher.discard_job(&job_id);
        return None;
    }
    let mut child = child.unwrap();

    let stdout = child.stdout.take().expect("Failed to capture stdout");
    let reader = BufReader::new(stdout);

    Some((child, reader, job_id))
}

fn dispatcher_loop(
    channel: DualChannel,
    check_interval: Duration,
    running: Arc<AtomicBool>,
) -> Result<(), anyhow::Error> {
    let mut workers: Vec<(Child, BufReader<std::process::ChildStdout>, u32, String)> = Vec::new();
    let mut dispatcher = ProverDispatcher::new();

    while running.load(Ordering::SeqCst) {
        let msg = channel.recv();
        match msg {
            Ok(msg) => {
                if let Some(msg) = msg {
                    if let Some((child, reader, context)) =
                        process_msg(&mut dispatcher, &msg.0)
                    {
                        workers.push((child, reader, msg.1, context));
                    } else {
                        error!("Error processing message: {:?}", msg);
                    }
                }
            }
            Err(e) => error!("Error: {:?}", e),
        }

        if !workers.is_empty() {
            workers.retain_mut(|(child, reader, id, context)| {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        let mut buf = String::new();
                        let _ = reader.read_to_string(&mut buf);

                        info!("Worker output: {}", buf);
                        info!("Worker exited with status: {:?}", status);

                        if let Some(result) =
                            dispatcher.process_result(&context, buf, status)
                        {
                            channel.send(*id, result).unwrap();
                        }

                        false // Remove from the list
                    }
                    Ok(None) => true, // Still running keep it  //TODO: Tick the dispatcher for timeout handling?
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
