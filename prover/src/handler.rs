use std::{collections::HashMap, process::ExitStatus};

use tracing::error;

use crate::{
    errors::ProverError,
    messages::{ProverJob, ProverJobType},
};

pub struct ProverDispatcher {
    jobs: HashMap<String, ProverJobType>,
}

//TODO: This part might be generalized
impl ProverDispatcher {
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
        }
    }

    pub fn process_msg(
        &mut self,
        msg: &str,
    ) -> Result<(String, Vec<String>, String), ProverError> {
        let msg: ProverJob = serde_json::from_str(msg)?;

        //chec if id is already in jobs
        if self.jobs.contains_key(&msg.job_id) {
            error!("Job id already exists: {}", msg.job_id);
            return Err(ProverError::JobIdAlreadyExists);
        }

        let (cmd, args) = match &msg.job_type {
            ProverJobType::ProveStark(output_file) => {
                let cmd = "../rust-bitvmx-zk-proof/target/release/host";
                let args = vec![
                    "prove-stark".to_string(),
                    "--input".to_string(),
                    "50".to_string(),
                    "--output".to_string(),
                    output_file.clone(),
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
                ProverJobType::ProveStark(_) => {
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
