use bytes::Bytes;
use paxos::{Configuration, Node, NodeId, Replica, Slot};
use paxos_example_support::{
    driver::{self, DecisionCursor},
    kvstore::{KvCommand, KvState},
};
use std::{
    collections::BTreeMap,
    convert::TryFrom,
    env,
};
use traceforge::{
    Config,
    comm_close::{TraceForgeSetTransport, TraceForgeTransportMode},
    thread,
    thread::ThreadId,
};

const CLIENT_REQUEST_TAG: u32 = u32::MAX;

#[derive(Clone, Copy, Debug)]
struct ModelOptions {
    nodes: usize,
    requests: usize,
    max_slots: usize,
    max_ballots: usize,
    workers: usize,
    print_decisions: bool,
}

impl Default for ModelOptions {
    fn default() -> Self {
        Self { nodes: 3, requests: 3, max_slots: 2, max_ballots: 1, workers: 4, print_decisions: false }
    }
}

impl ModelOptions {
    fn peer_quorum_messages(self) -> usize {
        self.nodes.saturating_sub(1) / 2
    }

    fn requests_for_node(self, node: NodeId) -> usize {
        (0..self.requests).filter(|request| request % self.nodes == node as usize).count()
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

type ModelNode = Node<TraceForgeSetTransport>;

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
    let current_node = config.current();
    let mut node: ModelNode = Node::new(
        TraceForgeSetTransport::with_mode(TraceForgeTransportMode::TaggedNativeInbox),
        config,
    );
    let mut state = KvState::default();
    let mut cursor = DecisionCursor::new();

    for _ in 0..options.requests_for_node(current_node) {
        let request: ClientRequest =
            traceforge::recv_tagged_msg_block(|_, tag| tag == Some(CLIENT_REQUEST_TAG));
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

    for _ in 0..options.max_ballots {
        if cursor.next_slot() >= options.max_slots as Slot {
            break;
        }
        let _ = driver::model_step(
            &mut node,
            &mut state,
            &mut cursor,
            options.max_slots,
            options.nodes,
            options.peer_quorum_messages(),
        )
        .expect("TraceForge transport is infallible");
    }

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
    let mut decisions: BTreeMap<Slot, Bytes> = BTreeMap::new();
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

fn decision_summary(logs: &[Vec<(Slot, Bytes)>]) -> String {
    let mut decisions: BTreeMap<Slot, String> = BTreeMap::new();
    for log in logs {
        for (slot, value) in log {
            if value.is_empty() {
                continue;
            }
            decisions.entry(*slot).or_insert_with(|| describe_decision(value));
        }
    }

    if decisions.is_empty() {
        return "none".to_string();
    }

    decisions
        .into_iter()
        .map(|(slot, decision)| format!("slot{}={}", slot, decision))
        .collect::<Vec<_>>()
        .join(",")
}

fn describe_decision(value: &Bytes) -> String {
    match KvCommand::try_from(value.clone()) {
        Ok(KvCommand::Set { request_id, value, .. }) => {
            format!("set{}:{}", request_id, String::from_utf8_lossy(&value))
        }
        Ok(KvCommand::Get { request_id, .. }) => format!("get{}", request_id),
        Err(_) => format!("raw:{}b", value.len()),
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
    if options.print_decisions {
        println!(
            "TRACEFORGE_PAXOS_EXEC_DECISIONS nodes={} requests={} max_slots={} max_ballots={} {}",
            options.nodes,
            options.requests,
            options.max_slots,
            options.max_ballots,
            decision_summary(&logs)
        );
    }
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
            "--max-slots" => {
                options.max_slots = parse_usize(&mut args, "--max-slots");
            }
            "--max-ballots" => {
                options.max_ballots = parse_usize(&mut args, "--max-ballots");
            }
            "--workers" => {
                options.workers = parse_positive_usize(&mut args, "--workers");
            }
            "--print-decisions" => {
                options.print_decisions = true;
            }
            "--help" | "-h" => {
                print_help_and_exit();
            }
            _ => {
                panic!(
                    "unknown argument: {} (expected --nodes <n>, --requests <n>, --max-slots <n>, --max-ballots <n>, --workers <n>, --print-decisions)",
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
        "Usage: traceforge-paxos [--nodes <n>] [--requests <n>] [--max-slots <n>] [--max-ballots <n>] [--workers <n>] [--print-decisions]\n\
         Defaults: --nodes 3 --requests 2 --max-slots 2 --max-ballots 3 --workers 4"
    );
    std::process::exit(0);
}

fn main() {
    let options = parse_args();
    println!(
        "Config = nodes {}, requests {}, max_slots {}, max_ballots {}, workers {}",
        options.nodes, options.requests, options.max_slots, options.max_ballots, options.workers
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
