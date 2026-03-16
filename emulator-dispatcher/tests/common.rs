use std::{
    sync::{atomic::AtomicBool, Arc},
    thread,
};

use bitvmx_job_dispatcher::dispatcher_loop;
use bitvmx_job_dispatcher_types::emulator_messages::EmulatorJobType;
use test_helper::test_helper::{config_broker, Paths};
use tracing::info;

pub fn start_emulator(
    running: Arc<AtomicBool>,
    storage_path: String,
    paths: Paths,
) -> Result<thread::JoinHandle<Result<(), String>>, anyhow::Error> {
    let handle = thread::spawn(move || {
        let (channel, check_interval, storage) = config_broker(None, &storage_path, &paths);

        if let Err(e) =
            dispatcher_loop::<EmulatorJobType>(channel, check_interval, running, storage, None)
        {
            tracing::error!("Error in dispatcher loop: {e}");
            return Err(format!("dispatcher error: {e}"));
        }
        info!("Dispatcher loop exited normally");

        Err("Expected abrupt end".to_string())
    });

    Ok(handle)
}
