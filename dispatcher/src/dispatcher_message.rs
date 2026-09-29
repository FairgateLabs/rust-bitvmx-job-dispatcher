use crate::dispatcher_error::DispatcherError;

/// What a job runs. The program and its arguments stay separate all the way down to the OS.
#[derive(Clone, Debug)]
pub struct JobCommand {
    pub program: String,
    pub args: Vec<String>,
    pub result_file: String,
    pub checkpoint_output_path: String,
}

impl JobCommand {
    pub fn new(
        program: impl Into<String>,
        args: Vec<String>,
        result_file: String,
        checkpoint_output_path: String,
    ) -> Self {
        Self {
            program: program.into(),
            args,
            result_file,
            checkpoint_output_path,
        }
    }
}

pub trait DispatcherMessage {
    fn prepare_local_input(&self) -> Result<(), DispatcherError> {
        Ok(())
    }
    fn prepare_remote_input(&self) -> Result<Vec<(Vec<u8>, String, String)>, DispatcherError> {
        Ok(vec![(vec![], String::new(), String::new())])
    }
    fn command(&self) -> Result<JobCommand, DispatcherError>;
    fn message_type(&self) -> String;
    fn commit_checkpoint(&self, _output_temp_path: String) -> Result<(), DispatcherError> {
        Ok(())
    }
}
