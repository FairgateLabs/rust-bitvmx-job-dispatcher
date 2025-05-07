use std::{
    fs,
    process::{Child, Command},
};

use bitvmx_broker::channel::channel::DualChannel;

use bitvmx_job_dispatcher::dispatcher_module::{Dispatcher, JobContext};
use bitvmx_job_dispatcher_types::{dispatcher_message::DispatcherMessage, EmulatorJobType};
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
pub struct DispatcherHandler {
    channel: DualChannel,
    workers: Vec<(Child, u32, JobContext)>,
    dispatcher: Dispatcher<EmulatorJobType>,
}

impl DispatcherHandler {
    pub fn new(channel: DualChannel) -> Self {
        let dispatcher = Dispatcher::<EmulatorJobType>::new();

        Self {
            channel,
            workers: Vec::new(),
            dispatcher,
        }
    }

    pub fn tick(&mut self) {
        let msg = self.channel.recv();
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
                    Ok(Some(status)) => match fs::read_to_string(&context.command_file) {
                        Ok(buf) => {
                            info!("Worker output from file: {}", buf);
                            info!("Worker exited with status: {:?}", status);

                            if let Some(result) =
                                self.dispatcher.process_result(&context.job_id, buf, status)
                            {
                                if let Err(e) = self.channel.send(*id, result) {
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
                    },
                    Ok(None) => true,
                    Err(e) => {
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
    }
}
