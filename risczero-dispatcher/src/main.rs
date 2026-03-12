use bitvmx_job_dispatcher::{cli::init, dispatcher_loop};
use bitvmx_job_dispatcher_types::prover_messages::ProverJobType;
use tracing::info;

fn main() -> Result<(), anyhow::Error> {
    let (channel, check_interval, running, storage) = init()?;
    dispatcher_loop::<ProverJobType>(channel, check_interval, running, storage)?;

    info!("Shutting down...");

    Ok(())
}
