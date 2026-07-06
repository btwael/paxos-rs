use bytes::Bytes;
use paxos::{
    Command, Node, NodeId, PaxosKey, PaxosRound, Phase, Receiver, Replica, ReplicatedState, Slot,
};
use traceforge_rounds::set::{SetCommError, SetTransport};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StepStats {
    pub received: usize,
    pub applied: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DecisionCursor {
    next_slot: Slot,
}

impl DecisionCursor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn next_slot(&self) -> Slot {
        self.next_slot
    }

    pub fn apply<T, S>(&mut self, node: &Node<T>, state: &mut S) -> usize
    where
        T: SetTransport<PaxosKey, PaxosRound, Command>,
        T::Node: Clone,
        T::Error: std::fmt::Debug,
        S: ReplicatedState,
    {
        let mut applied = 0;
        let decided = node.decisions().range(self.next_slot..).collect::<Vec<_>>();
        for (slot, command) in decided {
            if !command.is_empty() {
                state.execute(slot, command);
                applied += 1;
            }
            self.next_slot = slot + 1;
        }
        applied
    }
}

pub fn submit<T>(node: &mut Node<T>, value: Bytes)
where
    T: SetTransport<PaxosKey, PaxosRound, Command>,
    T::Node: Clone,
    T::Error: std::fmt::Debug,
{
    node.receive(Command::Proposal(value));
}

pub fn drain_once<T, S>(
    node: &mut Node<T>,
    state: &mut S,
    cursor: &mut DecisionCursor,
    max_messages: usize,
) -> Result<StepStats, SetCommError<PaxosKey, PaxosRound, Command, T::Error>>
where
    T: SetTransport<PaxosKey, PaxosRound, Command>,
    T::Node: Clone,
    T::Error: std::fmt::Debug,
    Command: 'static,
    S: ReplicatedState,
{
    let mut stats = StepStats::default();
    for _ in 0..max_messages {
        let Some((key, round, command)) = receive_any_known_key(node)? else {
            break;
        };
        node.receive_stamped(key, round, command);
        stats.received += 1;
        stats.applied += cursor.apply(node, state);
    }
    Ok(stats)
}

pub fn model_step<T, S>(
    node: &mut Node<T>,
    state: &mut S,
    cursor: &mut DecisionCursor,
    max_slots: usize,
    nodes: usize,
    max_quorum_messages: usize,
) -> Result<StepStats, SetCommError<PaxosKey, PaxosRound, Command, T::Error>>
where
    T: SetTransport<PaxosKey, PaxosRound, Command>,
    T::Node: Clone,
    T::Error: std::fmt::Debug,
    Command: 'static,
    S: ReplicatedState,
{
    let mut total = StepStats::default();
    let current = node.current_node_id();

    merge(&mut total, receive_proposal(node, state, cursor)?);
    for slot in bounded_slots(max_slots) {
        for proposer in bounded_nodes(nodes) {
            merge(
                &mut total,
                receive_slot_phase(node, state, cursor, slot, proposer, Phase::Prepare)?,
            );
            if proposer == current {
                merge(
                    &mut total,
                    collect_slot_quorum(
                        node,
                        state,
                        cursor,
                        slot,
                        Phase::Promise,
                        max_quorum_messages,
                    )?,
                );
            }
            merge(
                &mut total,
                receive_slot_phase(node, state, cursor, slot, proposer, Phase::Accept)?,
            );
            if proposer == current {
                merge(
                    &mut total,
                    collect_slot_quorum(
                        node,
                        state,
                        cursor,
                        slot,
                        Phase::Accepted,
                        max_quorum_messages,
                    )?,
                );
            }
            merge(
                &mut total,
                receive_slot_phase(node, state, cursor, slot, proposer, Phase::Resolution)?,
            );
            merge(
                &mut total,
                receive_slot_phase(node, state, cursor, slot, proposer, Phase::Reject)?,
            );
            merge(&mut total, receive_proposal(node, state, cursor)?);
        }
        merge(
            &mut total,
            receive_slot_phase(node, state, cursor, slot, current, Phase::Catchup)?,
        );
        merge(&mut total, receive_proposal(node, state, cursor)?);
    }

    Ok(total)
}

fn receive_proposal<T, S>(
    node: &mut Node<T>,
    state: &mut S,
    cursor: &mut DecisionCursor,
) -> Result<StepStats, SetCommError<PaxosKey, PaxosRound, Command, T::Error>>
where
    T: SetTransport<PaxosKey, PaxosRound, Command>,
    T::Node: Clone,
    T::Error: std::fmt::Debug,
    Command: 'static,
    S: ReplicatedState,
{
    let key = PaxosKey::ProposalTo(node.current_node_id());
    receive_key(node, state, cursor, key, |_| true)
}

fn receive_slot_phase<T, S>(
    node: &mut Node<T>,
    state: &mut S,
    cursor: &mut DecisionCursor,
    slot: Slot,
    proposer: NodeId,
    phase: Phase,
) -> Result<StepStats, SetCommError<PaxosKey, PaxosRound, Command, T::Error>>
where
    T: SetTransport<PaxosKey, PaxosRound, Command>,
    T::Node: Clone,
    T::Error: std::fmt::Debug,
    Command: 'static,
    S: ReplicatedState,
{
    let key = if phase == Phase::Catchup {
        PaxosKey::Catchup { slot, leader: proposer }
    } else {
        PaxosKey::Slot { slot, proposer }
    };
    receive_key(node, state, cursor, key, move |remote| remote.phase() == phase)
}

fn receive_key<T, S, F>(
    node: &mut Node<T>,
    state: &mut S,
    cursor: &mut DecisionCursor,
    key: PaxosKey,
    filter: F,
) -> Result<StepStats, SetCommError<PaxosKey, PaxosRound, Command, T::Error>>
where
    T: SetTransport<PaxosKey, PaxosRound, Command>,
    T::Node: Clone,
    T::Error: std::fmt::Debug,
    Command: 'static,
    S: ReplicatedState,
    F: Fn(&PaxosRound) -> bool + Send + Sync + 'static,
{
    let mut stats = StepStats::default();
    let received = node
        .comm_mut()
        .on(key)
        .recv_stamped_with::<Command, _>(move |_, remote| filter(remote))?;
    let Some((round, command)) = received else {
        return Ok(stats);
    };

    node.receive_stamped(key, round, command);
    stats.received += 1;
    stats.applied += cursor.apply(node, state);
    Ok(stats)
}

fn collect_slot_quorum<T, S>(
    node: &mut Node<T>,
    state: &mut S,
    cursor: &mut DecisionCursor,
    slot: Slot,
    phase: Phase,
    quorum_messages: usize,
) -> Result<StepStats, SetCommError<PaxosKey, PaxosRound, Command, T::Error>>
where
    T: SetTransport<PaxosKey, PaxosRound, Command>,
    T::Node: Clone,
    T::Error: std::fmt::Debug,
    Command: 'static,
    S: ReplicatedState,
{
    let Some(ballot) = node.highest_observed_ballot() else {
        return Ok(StepStats::default());
    };
    if ballot.1 != node.current_node_id() {
        return Ok(StepStats::default());
    }
    let round = node.current_round(PaxosKey::Slot { slot, proposer: ballot.1 });
    let received = match phase {
        Phase::Promise if matches!(round.phase(), Phase::Prepare | Phase::Promise) => {
            node.collect_promise_inbox(slot, ballot, 0, quorum_messages)?
        }
        Phase::Accepted if matches!(round.phase(), Phase::Accept | Phase::Accepted) => {
            node.collect_accepted_inbox(slot, ballot, 0, quorum_messages)?
        }
        _ => 0,
    };

    Ok(StepStats { received, applied: cursor.apply(node, state) })
}

fn bounded_slots(max_slots: usize) -> impl Iterator<Item = Slot> {
    0..max_slots as Slot
}

fn bounded_nodes(nodes: usize) -> impl Iterator<Item = NodeId> {
    (0..nodes).map(|node| node as NodeId)
}

fn receive_any_known_key<T>(
    node: &mut Node<T>,
) -> Result<Option<(PaxosKey, PaxosRound, Command)>, SetCommError<PaxosKey, PaxosRound, Command, T::Error>>
where
    T: SetTransport<PaxosKey, PaxosRound, Command>,
    T::Node: Clone,
    T::Error: std::fmt::Debug,
    Command: 'static,
{
    let proposal_key = PaxosKey::ProposalTo(node.current_node_id());
    if let Some((round, command)) = node.comm_mut().on(proposal_key).recv_stamped::<Command>()? {
        return Ok(Some((proposal_key, round, command)));
    }
    for slot in node.open_slots() {
        for proposer in node.participant_node_ids() {
            let key = PaxosKey::Slot { slot, proposer };
            if let Some((round, command)) = node.comm_mut().on(key).recv_stamped::<Command>()? {
                return Ok(Some((key, round, command)));
            }
        }
        let key = PaxosKey::Catchup { slot, leader: node.current_node_id() };
        if let Some((round, command)) = node.comm_mut().on(key).recv_stamped::<Command>()? {
            return Ok(Some((key, round, command)));
        }
    }
    Ok(None)
}

fn merge(total: &mut StepStats, step: StepStats) {
    total.received += step.received;
    total.applied += step.applied;
}

pub fn drain_until_idle<T, S>(
    node: &mut Node<T>,
    state: &mut S,
    cursor: &mut DecisionCursor,
    max_steps: usize,
    max_messages_per_step: usize,
) -> Result<StepStats, SetCommError<PaxosKey, PaxosRound, Command, T::Error>>
where
    T: SetTransport<PaxosKey, PaxosRound, Command>,
    T::Node: Clone,
    T::Error: std::fmt::Debug,
    Command: 'static,
    S: ReplicatedState,
{
    let mut total = StepStats::default();
    for _ in 0..max_steps {
        let step = drain_once(node, state, cursor, max_messages_per_step)?;
        total.received += step.received;
        total.applied += step.applied;
        if step.received == 0 {
            break;
        }
    }
    Ok(total)
}
