use std::{
    fs,
    net::IpAddr,
    process::{Child, Command},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::Result;
use bitvmx_broker::{channel::channel::DualChannel, rpc::BrokerConfig};
use bitvmx_emulator_job::handler::{EmulatorDispatcher, JobContext};
use tracing::{error, info};
use tracing_subscriber::{
    fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

pub fn process_msg(
    emulator_dispatcher: &mut EmulatorDispatcher,
    msg: &str,
) -> Option<(Child, JobContext)> {
    info!("Received: {:?}", msg);

    let (cmd, args, job_context) = emulator_dispatcher.process_msg(msg).ok()?;

    let child = Command::new(cmd).args(args).spawn();

    if let Err(e) = child {
        error!("Error executing command: {}", e);
        emulator_dispatcher.discard_job(&job_context.job_id);
        return None;
    }
    let child = child.unwrap();

    Some((child, job_context))
}

fn dispatcher_loop(
    channel: DualChannel,
    check_interval: Duration,
    running: Arc<AtomicBool>,
) -> Result<(), anyhow::Error> {
    let mut workers: Vec<(Child, u32, JobContext)> = Vec::new();
    let mut emulator_dispatcher = EmulatorDispatcher::new();

    while running.load(Ordering::SeqCst) {
        let msg = channel.recv();
        match msg {
            Ok(msg) => {
                if let Some(msg) = msg {
                    if let Some((child, context)) = process_msg(&mut emulator_dispatcher, &msg.0) {
                        workers.push((child, msg.1, context));
                    } else {
                        error!("Error processing message: {:?}", msg);
                    }
                }
            }
            Err(e) => error!("Error: {:?}", e),
        }

        if !workers.is_empty() {
            workers.retain_mut(|(child, id, context)| match child.try_wait() {
                Ok(Some(status)) => match fs::read_to_string(&context.command_file) {
                    Ok(buf) => {
                        info!("Worker output from file: {}", buf);
                        info!("Worker exited with status: {:?}", status);

                        if let Some(result) =
                            emulator_dispatcher.process_result(&context.job_id, buf, status)
                        {
                            if let Err(e) = channel.send(*id, result) {
                                error!("Failed to send result: {}", e);
                            }
                        }
                        false
                    }
                    Err(e) => {
                        error!("Failed to read file {}: {}", context.command_file, e);
                        if let Err(e) = channel.send(*id, "Failed to read file".to_string()) {
                            error!("Failed to send error message: {}", e);
                        }
                        false
                    }
                },
                Ok(None) => true,
                Err(e) => {
                    error!("Error checking worker: {}", e);
                    if let Err(e) = channel.send(*id, "Error checking worker status".to_string()) {
                        error!("Failed to send error message: {}", e);
                    }
                    false
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
