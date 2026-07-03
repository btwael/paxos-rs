use bytes::Bytes;
use paxos::{Configuration, Node, NodeId, Replica, Slot};
use paxos_example_support::{
    driver::{self, DecisionCursor},
    kvstore::{KvCommand, KvState},
};
use std::{collections::HashMap, env};
use traceforge::{
    Config,
    comm_close::{TraceForgeTransport, TraceForgeTransportMode},
    thread,
    thread::ThreadId,
};

const CLIENT_REQUEST_TAG: u32 = u32::MAX;
const PAXOS_PHASES_PER_REQUEST: usize = 5;

#[derive(Clone, Copy, Debug)]
struct ModelOptions {
    nodes: usize,
    requests: usize,
    workers: usize,
}

impl Default for ModelOptions {
    fn default() -> Self {
        Self { nodes: 3, requests: 1, workers: 4 }
    }
}

impl ModelOptions {
    fn delivery_passes(self) -> usize {
        self.requests * self.nodes * PAXOS_PHASES_PER_REQUEST
    }

    fn max_quorum_messages(self) -> usize {
        self.nodes.saturating_sub(1)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Init {
    nodes: Vec<ThreadId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ClientRequest {
    request_id: u64,
    key: Bytes,
    value: Bytes,
}

type ModelNode = Node<TraceForgeTransport>;

fn node_config(nodes: &[ThreadId], me: ThreadId) -> Configuration<ThreadId> {
    let current = nodes
        .iter()
        .position(|node| *node == me)
        .expect("current thread must be in participant set") as NodeId;

    Configuration::new(
        current,
        nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| **node != me)
            .map(|(id, node)| (id as NodeId, *node)),
    )
}

fn run_node(options: ModelOptions) -> Vec<(Slot, Bytes)> {
    let init: Init = traceforge::recv_tagged_msg_block(|_, tag| tag.is_none());
    let me = thread::current_id();
    let config = node_config(&init.nodes, me);
    let mut node: ModelNode = Node::new(
        TraceForgeTransport::with_mode(TraceForgeTransportMode::TaggedNativeInbox),
        config,
    );
    let mut state = KvState::default();
    let mut cursor = DecisionCursor::new();

    (0..options.delivery_passes()).for_each(|_| {
        let request: Option<ClientRequest> =
            traceforge::recv_tagged_msg(|_, tag| tag == Some(CLIENT_REQUEST_TAG));
        if let Some(request) = request {
            driver::submit(
                &mut node,
                KvCommand::Set {
                    request_id: request.request_id,
                    key: request.key,
                    value: request.value,
                }
                .into(),
            );
        }

        let _ =
            driver::model_step(&mut node, &mut state, &mut cursor, options.max_quorum_messages())
                .expect("TraceForge transport is infallible");
    });

    node.decisions().iter().collect()
}

fn run_client(client_id: usize, nodes: Vec<ThreadId>) {
    let target = client_id % nodes.len();
    let value =
        if client_id % 2 == 0 { Bytes::from_static(b"0") } else { Bytes::from_static(b"1") };

    traceforge::send_tagged_msg(
        nodes[target],
        CLIENT_REQUEST_TAG,
        ClientRequest { request_id: client_id as u64, key: Bytes::from_static(b"k"), value },
    );
}

fn assert_no_conflicting_decisions(logs: &[Vec<(Slot, Bytes)>]) {
    let mut decisions: HashMap<Slot, Bytes> = HashMap::new();
    for log in logs {
        for (slot, value) in log {
            if value.is_empty() {
                continue;
            }
            match decisions.get(slot) {
                Some(previous) => assert_eq!(
                    previous, value,
                    "conflicting decision for slot {}: {:?} vs {:?}",
                    slot, previous, value
                ),
                None => {
                    decisions.insert(*slot, value.clone());
                }
            }
        }
    }
}

fn model(options: ModelOptions) {
    let mut handles = Vec::new();
    for _ in 0..options.nodes {
        handles.push(thread::spawn(move || run_node(options)));
    }

    let init = Init { nodes: handles.iter().map(|handle| handle.thread().id()).collect() };
    for handle in &handles {
        traceforge::send_msg(handle.thread().id(), init.clone());
    }

    let clients = (0..options.requests)
        .map(|client| {
            let nodes = init.nodes.clone();
            thread::spawn(move || run_client(client, nodes))
        })
        .collect::<Vec<_>>();
    for client in clients {
        client.join().unwrap();
    }

    let logs = handles.into_iter().map(|handle| handle.join().unwrap()).collect::<Vec<_>>();
    assert_no_conflicting_decisions(&logs);
}

fn parse_args() -> ModelOptions {
    let mut options = ModelOptions::default();
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--nodes" => {
                options.nodes = parse_positive_usize(&mut args, "--nodes");
            }
            "--requests" => {
                options.requests = parse_usize(&mut args, "--requests");
            }
            "--workers" => {
                options.workers = parse_positive_usize(&mut args, "--workers");
            }
            "--help" | "-h" => {
                print_help_and_exit();
            }
            _ => {
                panic!(
                    "unknown argument: {} (expected --nodes <n>, --requests <n>, --workers <n>)",
                    arg
                );
            }
        }
    }

    if options.nodes < 2 {
        panic!("--nodes must be >= 2");
    }
    options
}

fn parse_usize(args: &mut impl Iterator<Item = String>, flag: &str) -> usize {
    let value = args.next().unwrap_or_else(|| panic!("{} requires a value", flag));
    value.parse::<usize>().unwrap_or_else(|_| panic!("{} requires an integer, got {}", flag, value))
}

fn parse_positive_usize(args: &mut impl Iterator<Item = String>, flag: &str) -> usize {
    let value = parse_usize(args, flag);
    if value == 0 {
        panic!("{} must be >= 1", flag);
    }
    value
}

fn print_help_and_exit() -> ! {
    println!(
        "Usage: traceforge-paxos [--nodes <n>] [--requests <n>] [--workers <n>]\n\
         Defaults: --nodes 3 --requests 1 --workers 4"
    );
    std::process::exit(0);
}

fn main() {
    let options = parse_args();
    println!(
        "Config = nodes {}, requests {}, workers {}",
        options.nodes, options.requests, options.workers
    );
    let stats = traceforge::verify(
        Config::builder()
            .with_parallel(true)
            .with_parallel_workers(options.workers)
            .with_iterations_until_split(5000)
            .build(),
        move || model(options),
    );
    println!("Stats = {}, {}", stats.execs, stats.block);
}
