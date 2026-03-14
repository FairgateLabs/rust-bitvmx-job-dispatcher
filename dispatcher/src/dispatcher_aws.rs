use std::rc::Rc;

use bitvmx_dispatcher_aws::aws_handler::AwsHandler;
use bitvmx_dispatcher_utils::Msg;
use serde::de::DeserializeOwned;

use crate::{
    dispatcher_error::DispatcherError, dispatcher_job::DispatcherJob,
    dispatcher_message::DispatcherMessage, dispatcher_storage::DispatcherStorage,
};

pub struct DispatcherAws {
    pub handler: AwsHandler,
    pub storage: DispatcherAwsStorage,
}

impl DispatcherAws {
    pub fn new(
        config_path: String,
        storage: Rc<DispatcherStorage>,
    ) -> Result<Self, DispatcherError> {
        let handler = AwsHandler::new(config_path)?;
        let storage = DispatcherAwsStorage::new(storage);
        Ok(Self { handler, storage })
    }

    pub fn spawn_aws_job<V>(&self, job: &DispatcherJob<V>, msg: Msg) -> Result<(), DispatcherError>
    where
        V: DispatcherMessage + DeserializeOwned,
    {
        Ok(())
    }

    pub fn tick<V>(&self) -> Result<bool, DispatcherError>
    where
        V: DispatcherMessage + DeserializeOwned,
    {
        //let job_context = self.handler.process_msg(msg);
        Ok(false)
    }
}

pub struct DispatcherAwsStorage {
    storage: Rc<DispatcherStorage>,
}

impl DispatcherAwsStorage {
    pub fn new(storage: Rc<DispatcherStorage>) -> Self {
        Self { storage }
    }
}
