use bytes::Bytes;
use hyper::{Body, Client, Request, client::HttpConnector};
use paxos::{Command, PaxosKey, PaxosRound};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    convert::Infallible,
    sync::{Arc, Mutex},
};
use traceforge_rounds::set::{SetEnvelope, SetTransport};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WireEnvelope {
    pub key: PaxosKey,
    pub round: PaxosRound,
    pub command: Command,
}

#[derive(Clone)]
pub struct HttpTransport {
    client: Client<HttpConnector, Body>,
    inbound: Arc<Mutex<VecDeque<SetEnvelope<PaxosKey, PaxosRound, Command>>>>,
}

impl Default for HttpTransport {
    fn default() -> HttpTransport {
        HttpTransport { client: Client::new(), inbound: Arc::new(Mutex::new(VecDeque::new())) }
    }
}

impl HttpTransport {
    pub fn push_bytes(&self, bytes: Bytes) {
        let Ok(wire) = bincode::deserialize::<WireEnvelope>(&bytes) else {
            return;
        };
        self.inbound.lock().unwrap().push_back(SetEnvelope::new(wire.key, wire.round, wire.command));
    }

    fn pop_matching<F>(
        &mut self,
        key: &PaxosKey,
        filter: &F,
        current: &PaxosRound,
    ) -> Option<SetEnvelope<PaxosKey, PaxosRound, Command>>
    where
        F: Fn(&PaxosRound, &PaxosRound) -> bool,
    {
        let mut inbound = self.inbound.lock().unwrap();
        let index = inbound
            .iter()
            .position(|envelope| envelope.key() == key && filter(current, envelope.stamp()))?;
        inbound.remove(index)
    }
}

impl SetTransport<PaxosKey, PaxosRound, Command> for HttpTransport {
    type Node = String;
    type Error = Infallible;

    fn send(
        &mut self,
        dst: Self::Node,
        envelope: SetEnvelope<PaxosKey, PaxosRound, Command>,
    ) -> Result<(), Self::Error> {
        let (key, round, command) = envelope.into_parts();
        let Ok(bytes) = bincode::serialize(&WireEnvelope { key, round, command }) else {
            return Ok(());
        };
        let request = Request::builder().method("POST").uri(dst).body(bytes.into()).unwrap();
        tokio::spawn(self.client.request(request));
        Ok(())
    }

    fn recv<F>(
        &mut self,
        key: &PaxosKey,
        current: &PaxosRound,
        filter: F,
    ) -> Result<Option<SetEnvelope<PaxosKey, PaxosRound, Command>>, Self::Error>
    where
        F: Fn(&PaxosRound, &PaxosRound) -> bool + Send + Sync + 'static,
    {
        Ok(self.pop_matching(key, &filter, current))
    }

    fn recv_block<F>(
        &mut self,
        key: &PaxosKey,
        current: &PaxosRound,
        filter: F,
    ) -> Result<SetEnvelope<PaxosKey, PaxosRound, Command>, Self::Error>
    where
        F: Fn(&PaxosRound, &PaxosRound) -> bool + Send + Sync + 'static,
    {
        loop {
            if let Some(envelope) = self.pop_matching(key, &filter, current) {
                return Ok(envelope);
            }
            std::thread::yield_now();
        }
    }

    fn inbox<F>(
        &mut self,
        key: &PaxosKey,
        current: &PaxosRound,
        filter: F,
        _min: usize,
        max: Option<usize>,
    ) -> Result<Vec<Option<SetEnvelope<PaxosKey, PaxosRound, Command>>>, Self::Error>
    where
        F: Fn(&PaxosRound, &PaxosRound) -> bool + Send + Sync + 'static,
    {
        let limit = max.unwrap_or(usize::MAX);
        let mut out = Vec::new();
        while out.len() < limit {
            match self.pop_matching(key, &filter, current) {
                Some(envelope) => out.push(Some(envelope)),
                None => break,
            }
        }
        Ok(out)
    }
}
