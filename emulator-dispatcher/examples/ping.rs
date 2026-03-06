use test_helper::test_helper::{init_trace, Paths};
use tracing::error;

#[path = "./challenge.rs"]
mod challenge;

// To make this example work, you need to:
// 1. run the server example first (from bitvmx-broker).
//      cargo run --release --example server -- --port 10000
// 2. Then run the job-dispatcher
//      cargo run --release --bin bitvmx-emulator-dispatcher -- --port 10000 --my-id 1
// 3. Then trigger one execution
//      cargo run --release --example challenge --features "emulator"

pub mod emulator {
    use bitvmx_broker::channel::channel::DualChannel;
    use bitvmx_dispatcher_utils::PingMessage;
    use test_helper::test_helper::{configure_example_broker, wait_for_result, Paths};
    use tracing::info;

    pub(crate) fn run_job(paths: Paths, port: u16) -> Result<(), anyhow::Error> {
        let (channel, emulator_id) = configure_example_broker(&paths, port)?;

        let msg = serde_json::to_string(&PingMessage::Ping)?;
        channel.send(&emulator_id.clone(), msg)?;

        info!("Waiting Pong Response...");
        let msg = wait_for_ping(&channel, 10, 1)?;

        match msg {
            PingMessage::Pong => {
                info!("Received Pong");
            }
            PingMessage::Ping => {
                return Err(anyhow::anyhow!("Unexpected Ping message received"));
            }
        }

        Ok(())
    }

    fn wait_for_ping(
        channel: &DualChannel,
        max_attempts: usize,
        delay_secs: u64,
    ) -> Result<PingMessage, anyhow::Error> {
        wait_for_result(channel, max_attempts, delay_secs, |msg| {
            let msg = serde_json::from_str::<PingMessage>(msg)?;
            Ok(Some(msg))
        })
    }
}

#[allow(dead_code)]
fn main() {
    init_trace();
    let paths = Paths::new("");
    if let Err(e) = emulator::run_job(paths, 10000) {
        error!("Error: {}", e);
    }
}
