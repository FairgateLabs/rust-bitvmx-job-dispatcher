use std::{
    fs,
    process::{Child, Command},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use bitvmx_broker::channel::channel::DualChannel;
use serde::de::DeserializeOwned;
use tracing::{error, info};

pub fn process_msg<V>(dispatcher: &mut Dispatcher<V>, msg: &str) -> Option<(Child, JobContext)>
where
    V: DispatcherMessage + DeserializeOwned,
{
    info!("Received: {:?}", msg);

    let (cmd, args, job_context) = dispatcher.process_msg(msg).ok()?;
    info!("Command: {:?}", cmd);
    info!("Args: {:?}", args);

    let child = Command::new(cmd).args(args).spawn();

    if let Err(e) = child {
        error!("Error executing command: {}", e);
        dispatcher.discard_job(&job_context.job_id);
        return None;
    }
    let child = child.unwrap();

    Some((child, job_context))
}
pub struct DispatcherHandler<T: DispatcherMessage + DeserializeOwned> {
    channel: DualChannel,
    workers: Vec<(Child, u32, JobContext)>,
    dispatcher: Dispatcher<T>,
}

impl<T> DispatcherHandler<T>
where
    T: DispatcherMessage + DeserializeOwned,
{
    pub fn new(channel: DualChannel) -> Self {
        let dispatcher = Dispatcher::<T>::new();

        Self {
            channel,
            workers: Vec::new(),
            dispatcher,
        }
    }

    pub fn tick(&mut self) -> bool {
        let msg = self.channel.recv();
        let mut job_completed = false;
        match msg {
            Ok(msg) => {
                if let Some(msg) = msg {
                    if let Some((child, context)) = process_msg(&mut self.dispatcher, &msg.0) {
                        self.workers.push((child, msg.1, context));
                    } else {
                        error!("Error processing message: {:?}", msg);
                    }
                }
            }
            Err(e) => error!("Error: {:?}", e),
        }

        if !self.workers.is_empty() {
            self.workers
                .retain_mut(|(child, id, context)| match child.try_wait() {
                    Ok(Some(status)) => {
                        job_completed = true;
                        match fs::read_to_string(&context.command_file) {
                            Ok(buf) => {
                                info!("Worker output from file: {}", buf);
                                info!("Worker exited with status: {:?}", status);

                                if let Some(result) =
                                    self.dispatcher.process_result(&context.job_id, buf, status)
                                {
                                    if let Err(e) = self.channel.send(
                                        *id,
                                        ResultMessage::new(context.job_id.clone(), result)
                                            .to_string(),
                                    ) {
                                        error!("Failed to send result: {}", e);
                                    }
                                }
                                false
                            }
                            Err(e) => {
                                error!("Failed to read file {}: {}", context.command_file, e);
                                if let Err(e) =
                                    self.channel.send(*id, "Failed to read file".to_string())
                                {
                                    error!("Failed to send error message: {}", e);
                                }
                                false
                            }
                        }
                    }
                    Ok(None) => true,
                    Err(e) => {
                        job_completed = true;
                        error!("Error checking worker: {}", e);
                        if let Err(e) = self
                            .channel
                            .send(*id, "Error checking worker status".to_string())
                        {
                            error!("Failed to send error message: {}", e);
                        }
                        false
                    }
                });
        }
        job_completed
    }
}

pub fn dispatcher_loop<T: DispatcherMessage + DeserializeOwned>(
    channel: DualChannel,
    check_interval: Duration,
    running: Arc<AtomicBool>,
) -> Result<(), anyhow::Error> {
    let mut dispacher_handler: DispatcherHandler<T> = DispatcherHandler::<T>::new(channel);

    while running.load(Ordering::SeqCst) {
        dispacher_handler.tick();
        std::thread::sleep(check_interval);
    }

    Ok(())
}