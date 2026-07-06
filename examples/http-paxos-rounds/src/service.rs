use crate::{kv_http::HttpKv, transport::HttpTransport};
use bytes::Bytes;
use hyper::{Body, Method, Request, Response, StatusCode};
use paxos::{Command, Configuration, Node, Receiver};
use paxos_example_support::{
    driver::{self, DecisionCursor},
    kvstore::{KvCommand, KvState},
};
use rand::random;
use std::{sync::Arc, time::Duration};
use tokio::{sync::Mutex, task::JoinHandle, time::interval};

type PaxosNode = Node<HttpTransport>;
const DRAIN_MAX_STEPS: usize = 8;
const DRAIN_MAX_MESSAGES_PER_STEP: usize = 64;

#[derive(Clone)]
pub struct Handler {
    replica: Arc<Mutex<PaxosNode>>,
    transport: HttpTransport,
    kv: HttpKv,
    cursor: Arc<Mutex<DecisionCursor>>,
}

impl Handler {
    pub fn new(config: Configuration<String>) -> Handler {
        let transport = HttpTransport::default();
        let kv_state = KvState::default();
        let replica = Node::new(transport.clone(), config);
        Handler {
            replica: Arc::new(Mutex::new(replica)),
            transport,
            kv: HttpKv::new(kv_state),
            cursor: Arc::new(Mutex::new(DecisionCursor::new())),
        }
    }

    pub fn spawn_tasks(&self) -> Vec<JoinHandle<()>> {
        let kv_cleanup = self.kv.clone();
        let cleanup = tokio::spawn(async move {
            let mut ticks = interval(Duration::new(30, 0));
            loop {
                ticks.tick().await;
                kv_cleanup.prune_listeners();
            }
        });

        // Fallback only: normal protocol progress is driven immediately from
        // the HTTP handlers so request latency is not quantized by this timer.
        let replica = self.replica.clone();
        let cursor = self.cursor.clone();
        let kv = self.kv.clone();
        let drain = tokio::spawn(async move {
            let mut ticks = interval(Duration::from_millis(10));
            loop {
                ticks.tick().await;
                let mut node = replica.lock().await;
                let mut cursor = cursor.lock().await;
                let mut state = kv.state();
                let _ = driver::drain_until_idle(
                    &mut node,
                    &mut state,
                    &mut cursor,
                    DRAIN_MAX_STEPS,
                    DRAIN_MAX_MESSAGES_PER_STEP,
                );
                kv.notify_new_events();
            }
        });

        vec![cleanup, drain]
    }

    async fn drain_pending(&self) {
        let mut node = self.replica.lock().await;
        let mut cursor = self.cursor.lock().await;
        let mut state = self.kv.state();
        let _ = driver::drain_until_idle(
            &mut node,
            &mut state,
            &mut cursor,
            DRAIN_MAX_STEPS,
            DRAIN_MAX_MESSAGES_PER_STEP,
        );
        self.kv.notify_new_events();
    }

    pub async fn handle(&self, req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
        let path = Bytes::from(req.uri().path()[1..].to_string());
        match (req.method(), path) {
            (&Method::POST, key) if key == "paxos" => {
                let bytes = hyper::body::to_bytes(req.into_body()).await?;
                self.transport.push_bytes(bytes);
                self.drain_pending().await;
                respond(StatusCode::ACCEPTED)
            }
            (&Method::POST, key) => {
                let value = hyper::body::to_bytes(req.into_body()).await?;
                let request_id = random();
                let receiver = self.kv.register_set(request_id);
                self.replica
                    .lock()
                    .await
                    .receive(Command::Proposal(KvCommand::Set { request_id, key, value }.into()));
                self.drain_pending().await;

                match receiver.await {
                    Ok(slot) => Ok(Response::builder()
                        .status(StatusCode::NO_CONTENT)
                        .header("X-Paxos-Slot", slot)
                        .body(Body::empty())
                        .unwrap()),
                    Err(_) => respond(StatusCode::INTERNAL_SERVER_ERROR),
                }
            }
            (&Method::GET, key) => {
                let request_id = random::<u64>();
                let receiver = self.kv.register_get(request_id);
                self.replica
                    .lock()
                    .await
                    .receive(Command::Proposal(KvCommand::Get { request_id, key }.into()));
                self.drain_pending().await;

                match receiver.await {
                    Ok(Some((slot, value))) => Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header("X-Paxos-Slot", slot)
                        .body(value.into())
                        .unwrap()),
                    Ok(None) => respond(StatusCode::NOT_FOUND),
                    Err(_) => respond(StatusCode::INTERNAL_SERVER_ERROR),
                }
            }
            (_, key) if key == "paxos" => respond(StatusCode::METHOD_NOT_ALLOWED),
            _ => respond(StatusCode::NOT_FOUND),
        }
    }
}

fn respond(code: StatusCode) -> Result<Response<Body>, hyper::Error> {
    let mut resp = Response::default();
    *resp.status_mut() = code;
    Ok(resp)
}
