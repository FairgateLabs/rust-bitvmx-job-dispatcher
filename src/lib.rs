use std::{collections::HashMap, process::ExitStatus};

use errors::JobDispatcherError;
use serde::{de::DeserializeOwned, Deserialize};
use tracing::error;

pub mod errors;


#[derive(Deserialize)]
pub struct DispatcherJob<V> 
where V: DispatcherMessage{
    pub job_id: String,
    pub job_type: V,
}

impl <V> DispatcherJob<V>
where V: DispatcherMessage {
    pub fn job_id(&self) -> &String {
        &self.job_id
    }
    pub fn job_type(&self) -> &V {
        &self.job_type
    }
}

pub struct Dispatcher<V>
where V: DeserializeOwned {
    jobs: HashMap<String, V>,
}

impl<V> Dispatcher<V>
where
    V: DispatcherMessage + DeserializeOwned
{

    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
        }
    }

    pub fn process_msg(
        &mut self,
        msg: &str,
    ) -> Result<(String, Vec<String>, String), JobDispatcherError> {
        let msg: DispatcherJob<V> = serde_json::from_str(msg)?;

        //chec if id is already in jobs
        if self.jobs.contains_key(msg.job_id()) {
            error!("Job id already exists: {}", msg.job_id());
            return Err(JobDispatcherError::JobIdAlreadyExists);
        }

        let (cmd, args) = msg.job_type.command();

        self.jobs.insert(msg.job_id().clone(), msg.job_type);

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
        if self.jobs.remove(id).is_some() {
            if status.success() {
                Some(result)
            } else {
                Some("Error".to_string())
            }
        } else {
            None
        }
    }

}

pub trait DispatcherMessage {
    fn command(&self) -> (String, Vec<String>);
}
