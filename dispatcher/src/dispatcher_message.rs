use crate::dispatcher_error::DispatcherError;

pub trait DispatcherMessage {
    fn command(&self) -> Result<(String, Vec<String>, String), DispatcherError>;
    fn message_type(&self) -> String;
    fn commit_checkpoint(&self) -> Result<(), DispatcherError> {
        Ok(())
    }
}
