use std::{
    process::{Child, Command},
    sync::{Arc, Mutex},
};

use serde::de::DeserializeOwned;
use tracing::info;
use dispatcher_utils::Msg;

use crate::{
    dispatcher_error::DispatcherError,
    dispatcher_message::DispatcherMessage,
    dispatcher_module::{Dispatcher, JobContext},
    dispatcher_storage::DispatcherStorage,
};
use std::path::PathBuf;

fn resolve_command_path(cmd: &str) -> Result<PathBuf, DispatcherError> {
    if cmd != "sh" {
        let cwd: PathBuf = env::current_dir()?;
        info!("Current working dir: {}", cwd.display());
        Ok(cwd.join(cmd))
    } else {
        Ok(PathBuf::from(cmd))
    }
}

pub fn job_key(job_id: &str) -> String {
    format!("job_{}", job_id)
}

pub fn process_msg<V>(
    dispatcher: &mut Dispatcher<V>,
    msg: &str,
) -> Result<(Child, JobContext), DispatcherError>
where
    V: DispatcherMessage + DeserializeOwned,
{
    info!("Received: {:?}", msg);

    let (cmd, args, job_context) = dispatcher.process_msg(&msg)?;
    let cmd = resolve_command_path(&cmd)?;
    info!("Command: {:?}", cmd);
    info!("Args: {:?}", args);

    let child = Command::new(cmd).args(args).spawn().map_err(|e| {
        dispatcher.discard_job(&job_context.job_id);
        DispatcherError::IoError(e)
    })?;

    Ok((child, job_context))
}

pub fn persist_job(
    job_context: &JobContext,
    msg: &Msg,
    storage: Arc<Mutex<DispatcherStorage>>,
) -> Result<(), DispatcherError> {
    let key: String = job_key(&job_context.job_id);
    storage
        .lock()
        .map_err(|_| DispatcherError::MutexPoisoned)?
        .persist_job(&key, &msg.to_string())?;
    Ok(())
}
