use std::{
    env, fs, path,
    process::{Child, Command},
    sync::{Arc, Mutex},
};

use bitvmx_dispatcher_utils::Msg;
use serde::de::DeserializeOwned;
use tracing::{error, info};

use crate::{
    dispatcher_error::DispatcherError,
    dispatcher_message::DispatcherMessage,
    dispatcher_module::{parse_job_msg, JobContext},
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

pub fn process_msg<V>(msg: &str) -> Result<(Child, JobContext), DispatcherError>
where
    V: DispatcherMessage + DeserializeOwned,
{
    info!("Received: {:?}", msg);

    let (cmd, args, job_context) = parse_job_msg::<V>(msg)?;
    let cmd = resolve_command_path(&cmd)?;
    info!("Command: {:?}", cmd);
    info!("Args: {:?}", args);

    let child = Command::new(cmd)
        .args(args)
        .spawn()
        .map_err(DispatcherError::IoError)?;

    Ok((child, job_context))
}

pub fn persist_job(
    job_context: &JobContext,
    msg: &Msg,
    storage: Arc<Mutex<DispatcherStorage>>,
) -> Result<(), DispatcherError> {
    storage
        .lock()
        .map_err(|_| DispatcherError::MutexPoisoned)?
        .persist_job(&job_context.job_id, &msg.to_string())?;
    Ok(())
}

// ======================================================
// Storage Utilities
// ======================================================

pub fn get_storage_path() -> String {
    let storage_path = format!("temp-runs/storage_job_{}.db", std::process::id());
    if path::Path::new(&storage_path).exists() {
        remove_storage_path(&storage_path);
    }
    storage_path
}

pub fn remove_storage_path(storage_path: &str) {
    // clean up the test’s storage file
    info!("Cleaning up storage file: {}", storage_path);
    if path::Path::new(&storage_path).exists() {
        fs::remove_dir_all(&storage_path)
            .unwrap_or_else(|e| error!("Warning: could not remove storage file: {e}"))
    }
}
