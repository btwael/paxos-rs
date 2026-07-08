use crate::{kv_http::HttpKv, transport::HttpTransport};
use bytes::Bytes;
use hyper::{Body, Method, Request, Response, StatusCode};
use paxos::{Command, Configuration, Node, Receiver, Replica, StampedReceiver, liveness::Liveness};
use paxos_example_support::{
    driver::DecisionCursor,
    kvstore::{KvCommand, KvState},
};
use rand::random;
use std::{sync::Arc, time::Duration};
use tokio::{sync::Mutex, task::JoinHandle, time::interval};

type PaxosNode = Liveness<Node<HttpTransport>>;

#[derive(Clone)]
pub struct Handler {
    replica: Arc<Mutex<PaxosNode>>,
    kv: HttpKv,
    cursor: Arc<Mutex<DecisionCursor>>,
}

impl Handler {
    pub fn new(config: Configuration<String>) -> Handler {
        let transport = HttpTransport::default();
        let kv_state = KvState::default();
        let replica = Node::new(transport.clone(), config).liveness();
        Handler {
            replica: Arc::new(Mutex::new(replica)),
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

        let replica_tick = self.replica.clone();
        let liveness_tick = tokio::spawn(async move {
            let mut ticks = interval(Duration::from_millis(100));
            loop {
                ticks.tick().await;
                replica_tick.lock().await.tick_liveness();
            }
        });

        vec![cleanup, liveness_tick]
    }

    async fn apply_decisions(&self, node: &PaxosNode) {
        let mut cursor = self.cursor.lock().await;
        let mut state = self.kv.state();
        cursor.apply(node, &mut state);
        self.kv.notify_new_events();
    }

    pub async fn handle(&self, req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
        let path = Bytes::from(req.uri().path()[1..].to_string());
        match (req.method(), path) {
            (&Method::POST, key) if key == "paxos" => {
                let bytes = hyper::body::to_bytes(req.into_body()).await?;
                let mut node = self.replica.lock().await;
                for wire in HttpTransport::decode_bytes(bytes) {
                    node.try_receive_stamped(wire.key, wire.round, wire.command);
                }
                self.apply_decisions(&node).await;
                respond(StatusCode::ACCEPTED)
            }
            (&Method::POST, key) => {
                let value = hyper::body::to_bytes(req.into_body()).await?;
                let request_id = random();
                let receiver = self.kv.register_set(request_id);
                {
                    let mut node = self.replica.lock().await;
                    node.receive(Command::Proposal(
                        KvCommand::Set { request_id, key, value }.into(),
                    ));
                    self.apply_decisions(&node).await;
                }

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
                {
                    let mut node = self.replica.lock().await;
                    node.receive(Command::Proposal(KvCommand::Get { request_id, key }.into()));
                    self.apply_decisions(&node).await;
                }

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
