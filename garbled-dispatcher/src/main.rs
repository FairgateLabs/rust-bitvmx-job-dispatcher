use bitvmx_job_dispatcher::{cli::init, dispatcher_loop};
use bitvmx_job_dispatcher_types::garbled_messages::GarbledJobType;
use tracing::info;

fn main() -> Result<(), anyhow::Error> {
    let (channel, check_interval, running, storage, config, local_mode) = init()?;

    #[cfg(feature = "aws")]
    info!("Running in AWS mode");
    #[cfg(not(feature = "aws"))]
    info!("Running in local mode");

    dispatcher_loop::<GarbledJobType>(
        channel,
        check_interval,
        running,
        storage,
        config,
        local_mode,
    )?;

    info!("Shutting down...");

    Ok(())
}
