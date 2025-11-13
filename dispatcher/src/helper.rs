use std::{
    fmt,
    process::{Child, Command},
    str::FromStr,
    sync::{Arc, Mutex},
};

use bitvmx_broker::identification::identifier::Identifier;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tracing::info;

use crate::{
    dispatcher_error::DispatcherError,
    dispatcher_message::DispatcherMessage,
    dispatcher_module::{Dispatcher, JobContext},
    dispatcher_storage::DispatcherStorage,
};
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum PingMessage {
    Ping,
    Pong,
}


#[derive(Serialize, Deserialize)]
pub struct Msg {
    pub raw: String,
    pub id: Identifier,
}

impl Msg {
    pub fn new(raw: String, id: Identifier) -> Self {
        Self { raw, id }
    }
    pub fn from_msg(msg: (String, Identifier)) -> Self {
        Self {
            raw: msg.0,
            id: msg.1,
        }
    }
}

impl fmt::Display for Msg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}|{}", self.id, self.raw)
    }
}

impl FromStr for Msg {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.splitn(2, '|');
        let id = parts.next().ok_or(())?;
        let raw = parts.next().ok_or(())?;
        let id = Identifier::from_str(id).map_err(|_| ())?;
        Ok(Msg::new(raw.to_string(), id))
    }
}

impl Msg {
    pub fn to_string(&self) -> String {
        format!("{}", self)
    }

    pub fn from_string(s: &str) -> Result<Self, DispatcherError> {
        s.parse().map_err(|_| DispatcherError::ParseError)
    }
}

fn resolve_command_path(cmd: &str) -> Result<PathBuf, DispatcherError> {
    if cmd != "sh" {
        let cwd: PathBuf = env::current_dir()?;
        info!("Current working dir: {}", cwd.display());
        Ok(cwd.join(cmd))
    } else {
        Ok(PathBuf::from(cmd))
    }  
}

pub fn job_key(job_id: &str) -> String {
    format!("job_{}", job_id)
}

pub fn process_msg<V>(
    dispatcher: &mut Dispatcher<V>,
    msg: &str,
) -> Result<(Child, JobContext), DispatcherError>
where
    V: DispatcherMessage + DeserializeOwned,
{
    info!("Received: {:?}", msg);

    let (cmd, args, job_context) = dispatcher.process_msg(&msg)?;
    let cmd = resolve_command_path(&cmd)?;
    info!("Command: {:?}", cmd);
    info!("Args: {:?}", args);

    let child = Command::new(cmd).args(args).spawn().map_err(|e| {
        dispatcher.discard_job(&job_context.job_id);
        DispatcherError::IoError(e)
    })?;

    Ok((child, job_context))
}

pub fn persist_job(
    job_context: &JobContext,
    msg: &Msg,
    storage: Arc<Mutex<DispatcherStorage>>,
) -> Result<(), DispatcherError> {
    let key: String = job_key(&job_context.job_id);
    storage
        .lock()
        .map_err(|_| DispatcherError::MutexPoisoned)?
        .persist_job(&key, &msg.to_string())?;
    Ok(())
}
