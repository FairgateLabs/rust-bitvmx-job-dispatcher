use std::{fs, path};

use tracing::{error, info};

use crate::dispatcher_error::DispatcherError;
use std::path::PathBuf;

pub fn resolve_command_path(cmd: &str) -> Result<PathBuf, DispatcherError> {
    if cmd != "sh" {
        let cwd: PathBuf = env::current_dir()?;
        info!("Current working dir: {}", cwd.display());
        Ok(cwd.join(cmd))
    } else {
        Ok(PathBuf::from(cmd))
    }
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
        fs::remove_dir_all(storage_path)
            .unwrap_or_else(|e| error!("Warning: could not remove storage file: {e}"))
    }
}
