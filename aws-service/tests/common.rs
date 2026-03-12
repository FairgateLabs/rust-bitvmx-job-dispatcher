use anyhow::Result;
use bitvmx_aws_job_dispatcher::dispatcher_loop;
use std::{
    sync::{Arc, Mutex, atomic::AtomicBool},
    thread,
};
use test_helper::test_helper::config_broker;
use tokio::runtime::Runtime;

pub fn start_dispatcher(
    running: Arc<AtomicBool>,
    storage_path: String,
    config_path: String,
) -> Result<thread::JoinHandle<Result<(), String>>, anyhow::Error> {
    let rt = Arc::new(Mutex::new(Runtime::new().unwrap()));
    let handle = thread::spawn(move || {
        let (channel, check_interval, storage) = config_broker(Some(rt.clone()), &storage_path);

        if let Err(e) = dispatcher_loop(
            channel,
            check_interval,
            running,
            rt.clone(),
            storage,
            config_path.clone(),
        ) {
            return Err(format!("dispatcher error: {e}"));
        }

        Err("Expected abrupt end".to_string())
    });

    Ok(handle)
}
