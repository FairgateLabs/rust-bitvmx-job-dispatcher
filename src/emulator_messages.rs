use std::{collections::HashMap, process::ExitStatus};

use serde::{Deserialize, Serialize};
use tracing::error;

use crate::errors::JobDispatcherError;

#[derive(Debug, Serialize, Deserialize)]
pub struct EmulatorExecute {
    pub id: String,
    pub elf: String,
}

pub enum MessageType {
    Execute,
}

pub struct EmulatorDispatcher {
    jobs: HashMap<String, (MessageType, String)>,
}

impl EmulatorDispatcher {
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
        }
    }

    pub fn process_msg(
        &mut self,
        msg: &str,
    ) -> Result<(String, Vec<String>, String), JobDispatcherError> {
        let msg: EmulatorExecute = serde_json::from_str(msg)?;

        //chec if id is already in jobs
        if self.jobs.contains_key(&msg.id) {
            error!("Job id already exists: {}", msg.id);
            return Err(JobDispatcherError::JobIdAlreadyExists);
        }

        self.jobs.insert(
            msg.id.clone(),
            (MessageType::Execute, "someting else?".to_string()),
        );

        let cmd = "../BitVMX-CPU/target/release/emulator";
        let args = vec![
            "execute".to_string(),
            "--elf".to_string(),
            msg.elf,
            "--debug".to_string(),
            "--limit".to_string(),
            "20".to_string(),
        ];

        Ok((cmd.to_string(), args, msg.id))
    }

    pub fn discard_job(&mut self, id: &str) {
        self.jobs.remove(id);
    }

    pub fn process_result(
        &mut self,
        id: &str,
        result: String,
        status: ExitStatus,
    ) -> Option<String> {
        if let Some((msg_type, _)) = self.jobs.get(id) {
            match msg_type {
                MessageType::Execute => {
                    self.jobs.remove(id);

                    //TODO: Parse result if necessary
                    if status.success() {
                        Some(result)
                    } else {
                        Some("Error".to_string())
                    }
                }
            }
        } else {
            None
        }
    }
}
