use crate::dispatcher_error::DispatcherError;

pub trait DispatcherMessage {
    fn prepare_local_input(&self) -> Result<(), DispatcherError> {
        Ok(())
    }
    fn prepare_remote_input(&self) -> Result<Vec<(Vec<u8>, String)>, DispatcherError> {
        Ok(vec![(vec![], String::new())])
    }
    fn command(&self) -> Result<(String, Vec<String>, String), DispatcherError>;
    fn message_type(&self) -> String;
    fn commit_checkpoint(&self) -> Result<(), DispatcherError> {
        Ok(())
    }
}
