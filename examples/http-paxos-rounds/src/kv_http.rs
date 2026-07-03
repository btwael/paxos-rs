use bytes::Bytes;
use paxos::Slot;
use paxos_example_support::kvstore::{KvEvent, KvState};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::oneshot::{Receiver, Sender, channel};

struct Inner {
    pending_set: HashMap<u64, Sender<Slot>>,
    pending_get: HashMap<u64, Sender<Option<(Slot, Bytes)>>>,
}

#[derive(Clone)]
pub struct HttpKv {
    state: KvState,
    inner: Arc<Mutex<Inner>>,
}

impl HttpKv {
    pub fn new(state: KvState) -> HttpKv {
        HttpKv {
            state,
            inner: Arc::new(Mutex::new(Inner {
                pending_set: HashMap::new(),
                pending_get: HashMap::new(),
            })),
        }
    }

    pub fn state(&self) -> KvState {
        self.state.clone()
    }

    pub fn register_get(&self, id: u64) -> Receiver<Option<(Slot, Bytes)>> {
        let (snd, recv) = channel();
        self.inner.lock().unwrap().pending_get.insert(id, snd);
        recv
    }

    pub fn register_set(&self, id: u64) -> Receiver<Slot> {
        let (snd, recv) = channel();
        self.inner.lock().unwrap().pending_set.insert(id, snd);
        recv
    }

    pub fn prune_listeners(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.pending_get.retain(|_, val| !val.is_closed());
        inner.pending_set.retain(|_, val| !val.is_closed());
    }

    pub fn notify_new_events(&self) {
        let events = self.state.take_events();
        let mut inner = self.inner.lock().unwrap();
        for event in events {
            match event {
                KvEvent::Get { request_id, slot, value } => {
                    if let Some(sender) = inner.pending_get.remove(&request_id) {
                        sender.send(value.map(|value| (slot, value))).unwrap_or(());
                    }
                }
                KvEvent::Set { request_id, slot } => {
                    if let Some(sender) = inner.pending_set.remove(&request_id) {
                        sender.send(slot).unwrap_or(());
                    }
                }
            }
        }
    }
}
