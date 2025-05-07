use serde::{Deserialize, Serialize};

use crate::dispatcher_message::DispatcherMessage;

#[derive(Debug, Serialize, Deserialize)]
pub struct DispatcherJob<P>
where
    P: DispatcherMessage,
{
    pub job_id: String,
    pub job_type: P,
}

impl<P> DispatcherJob<P>
where
    P: DispatcherMessage,
{
    pub fn job_id(&self) -> &String {
        &self.job_id
    }
    pub fn job_type(&self) -> &P {
        &self.job_type
    }
}
