use std::{fs, path};

use tracing::{error, info};

pub fn get_storage_path() -> String {
    let storage_path = format!("../temp-runs/storage_job_{}.db", std::process::id());
    if path::Path::new(&storage_path).exists() {
        remove_storage_file(&storage_path);
    }
    storage_path
}

pub fn remove_storage_file(storage_path: &str) {
    // clean up the test’s storage file
    info!("Cleaning up storage file: {}", storage_path);
    if path::Path::new(&storage_path).exists() {
        fs::remove_dir(&storage_path)
            .unwrap_or_else(|e| error!("Warning: could not remove storage file: {e}"))
    }
}
