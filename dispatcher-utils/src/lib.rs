use std::{fmt, str::FromStr};

use bitvmx_broker::identification::identifier::Identifier;
use serde::{Deserialize, Serialize};

use crate::error::UtilsError;

pub mod error;

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
    pub fn from_string(s: &str) -> Result<Self, UtilsError> {
        s.parse().map_err(|_| UtilsError::ParseError)
    }
}
