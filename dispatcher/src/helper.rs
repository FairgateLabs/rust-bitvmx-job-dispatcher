use std::{
    fmt,
    process::{Child, Command},
    str::FromStr,
    sync::{Arc, Mutex},
};

use bitvmx_broker::identification::identifier::Identifier;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tracing::{error, info};

use crate::{
    dispatcher_message::DispatcherMessage,
    dispatcher_module::{Dispatcher, JobContext},
    dispatcher_storage::DispatcherStorage,
};
use std::path::{Path, PathBuf};

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

    pub fn from_string(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

fn resolve_command_path(cmd: &str) -> PathBuf {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    workspace_root.join(cmd)
}

pub fn job_key(job_id: &str) -> String {
    format!("job_{}", job_id)
}

pub fn process_msg<V>(
    dispatcher: &mut Dispatcher<V>,
    msg: &Msg,
    store: Option<Arc<Mutex<DispatcherStorage>>>,
) -> Option<(Child, JobContext)>
where
    V: DispatcherMessage + DeserializeOwned,
{
    info!("Received: {:?}", msg.raw);

    let (cmd, args, job_context) = dispatcher.process_msg(&msg.raw).ok()?;
    let cmd = resolve_command_path(&cmd);
    info!("Command: {:?}", cmd);
    info!("Args: {:?}", args);

    if let Some(storage) = store {
        let key = job_key(&job_context.job_id);
        storage.lock().unwrap().persist_job(&key, &msg.to_string());
    }

    let child = Command::new(cmd).args(args).spawn();

    if let Err(e) = child {
        error!("Error executing command: {}", e);
        dispatcher.discard_job(&job_context.job_id);
        return None;
    }
    let child = child.unwrap();

    Some((child, job_context))
}
