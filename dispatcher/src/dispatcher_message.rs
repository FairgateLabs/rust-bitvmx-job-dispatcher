pub trait DispatcherMessage {
    fn command(&self) -> (String, Vec<String>);
}