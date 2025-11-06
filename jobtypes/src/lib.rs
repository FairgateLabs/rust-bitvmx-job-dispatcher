use serde::{Deserialize, Serialize};

#[cfg(feature = "emulator")]
pub mod emulator_messages;
#[cfg(feature = "prover")]
pub mod prover_messages;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ResultType {
    Pong { value: u64 },
}
