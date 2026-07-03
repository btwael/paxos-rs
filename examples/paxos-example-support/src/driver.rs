use bytes::Bytes;
use paxos::{Command, Node, PaxosRound, Phase, Receiver, Replica, ReplicatedState, Slot};
use traceforge_rounds::{CommError, Transport};

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

    pub fn apply<T, S>(&mut self, node: &Node<T>, state: &mut S) -> usize
    where
        T: Transport<PaxosRound, Command>,
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
    T: Transport<PaxosRound, Command>,
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
) -> Result<StepStats, CommError<PaxosRound, Command, T::Error>>
where
    T: Transport<PaxosRound, Command>,
    T::Node: Clone,
    T::Error: std::fmt::Debug,
    Command: 'static,
    S: ReplicatedState,
{
    let mut stats = StepStats::default();
    for _ in 0..max_messages {
        let received = node.comm_mut().recv_stamped::<Command>()?;
        let Some((round, command)) = received else {
            break;
        };
        node.receive_stamped(round, command);
        stats.received += 1;
        stats.applied += cursor.apply(node, state);
    }
    Ok(stats)
}

pub fn model_step<T, S>(
    node: &mut Node<T>,
    state: &mut S,
    cursor: &mut DecisionCursor,
    max_quorum_messages: usize,
) -> Result<StepStats, CommError<PaxosRound, Command, T::Error>>
where
    T: Transport<PaxosRound, Command>,
    T::Node: Clone,
    T::Error: std::fmt::Debug,
    Command: 'static,
    S: ReplicatedState,
{
    let mut total = StepStats::default();

    merge(&mut total, receive_expected_phase(node, state, cursor, Phase::Prepare)?);
    merge(
        &mut total,
        collect_expected_quorum(node, state, cursor, Phase::Promise, max_quorum_messages)?,
    );
    merge(&mut total, receive_expected_phase(node, state, cursor, Phase::Accept)?);
    merge(
        &mut total,
        collect_expected_quorum(node, state, cursor, Phase::Accepted, max_quorum_messages)?,
    );
    merge(&mut total, receive_expected_phase(node, state, cursor, Phase::Resolution)?);
    merge(&mut total, receive_expected_phase(node, state, cursor, Phase::Catchup)?);
    merge(&mut total, receive_expected_phase(node, state, cursor, Phase::Reject)?);

    Ok(total)
}

fn receive_expected_phase<T, S>(
    node: &mut Node<T>,
    state: &mut S,
    cursor: &mut DecisionCursor,
    phase: Phase,
) -> Result<StepStats, CommError<PaxosRound, Command, T::Error>>
where
    T: Transport<PaxosRound, Command>,
    T::Node: Clone,
    T::Error: std::fmt::Debug,
    Command: 'static,
    S: ReplicatedState,
{
    let mut stats = StepStats::default();
    let received = node
        .comm_mut()
        .recv_stamped_with::<Command, _>(move |_, remote| remote.phase() == phase)?;
    let Some((round, command)) = received else {
        return Ok(stats);
    };

    node.receive_stamped(round, command);
    stats.received += 1;
    stats.applied += cursor.apply(node, state);
    Ok(stats)
}

fn collect_expected_quorum<T, S>(
    node: &mut Node<T>,
    state: &mut S,
    cursor: &mut DecisionCursor,
    phase: Phase,
    max_quorum_messages: usize,
) -> Result<StepStats, CommError<PaxosRound, Command, T::Error>>
where
    T: Transport<PaxosRound, Command>,
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

    let round = node.current_round();
    let slot = round.paxos_slot();
    let received = match phase {
        Phase::Promise if matches!(round.phase(), Phase::Prepare | Phase::Promise) => {
            node.collect_promise_inbox(slot, ballot, 0, max_quorum_messages)?
        }
        Phase::Accepted if matches!(round.phase(), Phase::Accept | Phase::Accepted) => {
            node.collect_accepted_inbox(slot, ballot, 0, max_quorum_messages)?
        }
        _ => 0,
    };

    Ok(StepStats { received, applied: cursor.apply(node, state) })
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
) -> Result<StepStats, CommError<PaxosRound, Command, T::Error>>
where
    T: Transport<PaxosRound, Command>,
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
