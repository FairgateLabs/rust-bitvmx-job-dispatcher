use crate::dispatcher::dispatcher_message::DispatcherMessage;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum EmulatorJobType {
    Execute(String), // elf path
}

impl DispatcherMessage for EmulatorJobType {
    fn command(&self) -> (String, Vec<String>) {
        match self {
            EmulatorJobType::Execute(elf) => {
                let cmd = "../BitVMX-CPU/target/release/emulator".to_string();
                let args = vec![
                    "execute".to_string(),
                    "--elf".to_string(),
                    elf.clone(),
                    "--debug".to_string(),
                    "--limit".to_string(),
                    "20".to_string(),
                ];
                (cmd, args)
            }
        }
    }
}
