use bytes::Bytes;
use paxos::{ReplicatedState, Slot};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    convert::TryFrom,
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvCommand {
    Get { request_id: u64, key: Bytes },
    Set { request_id: u64, key: Bytes, value: Bytes },
}

impl From<KvCommand> for Bytes {
    fn from(command: KvCommand) -> Bytes {
        bincode::serialize(&command).unwrap().into()
    }
}

impl TryFrom<Bytes> for KvCommand {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        bincode::deserialize(&value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvEvent {
    Get { request_id: u64, slot: Slot, value: Option<Bytes> },
    Set { request_id: u64, slot: Slot },
}

#[derive(Default)]
struct Inner {
    values: HashMap<Bytes, Bytes>,
    events: Vec<KvEvent>,
}

#[derive(Clone, Default)]
pub struct KvState {
    inner: Arc<Mutex<Inner>>,
}

impl KvState {
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.inner.lock().unwrap().values.get(key).cloned()
    }

    pub fn events(&self) -> Vec<KvEvent> {
        self.inner.lock().unwrap().events.clone()
    }

    pub fn take_events(&self) -> Vec<KvEvent> {
        std::mem::take(&mut self.inner.lock().unwrap().events)
    }
}

impl ReplicatedState for KvState {
    fn execute(&mut self, slot: Slot, cmd: Bytes) {
        let command = match KvCommand::try_from(cmd) {
            Ok(command) => command,
            Err(_) => return,
        };

        let mut inner = self.inner.lock().unwrap();
        match command {
            KvCommand::Get { request_id, key } => {
                let value = inner.values.get(&key).cloned();
                inner.events.push(KvEvent::Get { request_id, slot, value });
            }
            KvCommand::Set { request_id, key, value } => {
                inner.values.insert(key, value);
                inner.events.push(KvEvent::Set { request_id, slot });
            }
        }
    }
}
