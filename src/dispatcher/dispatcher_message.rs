use crate::dispatcher::dispatcher_error::DispatcherError;

pub trait DispatcherMessage {
    fn command(&self) -> Result<(String, Vec<String>, String), DispatcherError>;
    fn message_type(&self) -> String;
}
