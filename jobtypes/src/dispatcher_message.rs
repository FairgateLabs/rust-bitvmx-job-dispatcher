use crate::JobTypeError;

pub trait DispatcherMessage {
    fn command(&self) -> Result<(String, Vec<String>, String), JobTypeError>;
    fn message_type(&self) -> String;
}
