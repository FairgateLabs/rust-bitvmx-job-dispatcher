use std::{collections::HashMap, process::ExitStatus};

use tracing::error;

use crate::{
    errors::EmulatorError,
    messages::{EmulatorJob, EmulatorJobType},
};

pub struct EmulatorDispatcher {
    jobs: HashMap<String, EmulatorJobType>,
}

//TODO: This part might be generalized
impl EmulatorDispatcher {
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
        }
    }

    pub fn process_msg(
        &mut self,
        msg: &str,
    ) -> Result<(String, Vec<String>, String), EmulatorError> {
        let msg: EmulatorJob = serde_json::from_str(msg)?;

        //chec if id is already in jobs
        if self.jobs.contains_key(&msg.job_id) {
            error!("Job id already exists: {}", msg.job_id);
            return Err(EmulatorError::JobIdAlreadyExists);
        }

        let (cmd, args) = match &msg.job_type {
            EmulatorJobType::Execute(elf) => {
                let cmd = "../BitVMX-CPU/target/release/emulator";
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
        };

        self.jobs.insert(msg.job_id.clone(), msg.job_type);

        Ok((cmd.to_string(), args, msg.job_id))
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
        if let Some(msg_type) = self.jobs.get(id) {
            match msg_type {
                EmulatorJobType::Execute(_) => {
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
