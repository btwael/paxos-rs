use crate::round::PaxosRound;
use crate::{Ballot, NodeId, Slot, round::Phase};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Receiver of Paxos commands.
pub trait Receiver {
    /// Receives a command and reacts accordingly
    fn receive(&mut self, command: Command);
}

/// Receiver of Paxos commands.
///
/// This is a convenience trait that breaks out reactors for each command.
pub trait Commander {
    /// Receive a proposal
    fn proposal(&mut self, val: Bytes);

    /// Receive a Phase 1a PREPARE message containing the proposed ballot
    fn prepare(&mut self, slot: Slot, bal: Ballot);

    /// Receive a Phase 1b PROMISE message containing the node
    /// that generated the promise, the ballot promised and the accepted
    /// value for this slot, if one exists.
    fn promise(&mut self, node: NodeId, slot: Slot, bal: Ballot, accepted: Option<(Ballot, Bytes)>);

    /// Receive a Phase 2a ACCEPT message that contains the the slot, proposed
    /// ballot and value of the proposal. The ballot contains the node of
    /// the leader of the slot.
    fn accept(&mut self, slot: Slot, bal: Ballot, value: Bytes);

    /// Receives a REJECT message from a peer containing a higher ballot that
    /// preempts either a Phase 1a (PREPARE) for Phase 2a (ACCEPT) message.
    fn reject(
        &mut self,
        node: NodeId,
        slot: Slot,
        proposed: Ballot,
        preempted: Ballot,
        phase: Phase,
    );

    /// Receives a Phase 2b ACCEPTED message containing the acceptor that has
    /// accepted the slot's proposal along with the ballot that generated
    /// the slot.
    fn accepted(&mut self, node: NodeId, slot: Slot, bal: Ballot);

    /// Receives a final resolution of a slot that has been accepted by a
    /// majority of acceptors.
    ///
    /// NOTE: Resolutions may arrive out-of-order. No guarantees are made on
    /// slot order.
    fn resolution(&mut self, slot: Slot, bal: Ballot, value: Bytes);

    /// Request sent to a distinguished learner to catch up to latest slot
    /// values.
    fn catchup(&mut self, node: NodeId, slot: Slot);
}

impl<T: Commander> Receiver for T {
    fn receive(&mut self, command: Command) {
        match command {
            Command::Proposal(val) => {
                self.proposal(val);
            }
            Command::Prepare { slot, ballot } => {
                self.prepare(slot, ballot);
            }
            Command::Promise { from, slot, ballot, accepted } => {
                self.promise(from, slot, ballot, accepted);
            }
            Command::Accept { slot, ballot, value } => {
                self.accept(slot, ballot, value);
            }
            Command::Reject { from, slot, proposed, preempted, phase } => {
                self.reject(from, slot, proposed, preempted, phase);
            }
            Command::Accepted { from, slot, ballot } => {
                self.accepted(from, slot, ballot);
            }
            Command::Resolution { slot, ballot, value } => {
                self.resolution(slot, ballot, value);
            }
            Command::Catchup { from, slot } => {
                self.catchup(from, slot);
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
/// RPC commands sent between replicas
pub enum Command {
    /// Propose a value
    Proposal(Bytes),

    /// Phase 1a PREPARE message containing the proposed ballot
    Prepare { slot: Slot, ballot: Ballot },

    /// Phase 1b PROMISE message containing the node
    /// that generated the promise, the ballot promised and the slot's
    /// previously accepted value, if one exists.
    Promise { from: NodeId, slot: Slot, ballot: Ballot, accepted: Option<(Ballot, Bytes)> },

    /// Phase 2a ACCEPT message that contains the the slot, proposed
    /// ballot and value of the proposal. The ballot contains the node of
    /// the leader of the slot.
    Accept { slot: Slot, ballot: Ballot, value: Bytes },

    /// REJECT a peer's previous message containing a higher ballot that
    /// preempts either a Phase 1a (PREPARE) for Phase 2a (ACCEPT) message.
    Reject { from: NodeId, slot: Slot, proposed: Ballot, preempted: Ballot, phase: Phase },

    /// Phase 2b ACCEPTED message containing the acceptor that has
    /// accepted the slot's proposal along with the ballot that generated
    /// the slot.
    Accepted { from: NodeId, slot: Slot, ballot: Ballot },

    /// Resolution of a slot that has been accepted by a
    /// majority of acceptors.
    ///
    /// NOTE: Resolutions may arrive out-of-order. No guarantees are made on
    /// slot order.
    Resolution { slot: Slot, ballot: Ballot, value: Bytes },

    /// Request sent to a distinguished learner to catch up to latest slot
    /// values.
    Catchup { from: NodeId, slot: Slot },
}

impl Command {
    /// Returns the Paxos round that is part of the command's protocol payload.
    ///
    /// Proposal and catchup messages are intentionally excluded: proposals are
    /// client work forwarded between replicas, and catchup requests do not carry
    /// a ballot in the original protocol.
    pub fn protocol_round(&self) -> Option<PaxosRound> {
        match self {
            Command::Proposal(_) | Command::Catchup { .. } => None,
            Command::Prepare { slot, ballot } => {
                Some(PaxosRound::new(*slot, ballot.0, Phase::Prepare))
            }
            Command::Promise { slot, ballot, .. } => {
                Some(PaxosRound::new(*slot, ballot.0, Phase::Promise))
            }
            Command::Accept { slot, ballot, .. } => {
                Some(PaxosRound::new(*slot, ballot.0, Phase::Accept))
            }
            Command::Reject { slot, proposed, .. } => {
                Some(PaxosRound::new(*slot, proposed.0, Phase::Reject))
            }
            Command::Accepted { slot, ballot, .. } => {
                Some(PaxosRound::new(*slot, ballot.0, Phase::Accepted))
            }
            Command::Resolution { slot, ballot, .. } => {
                Some(PaxosRound::new(*slot, ballot.0, Phase::Resolution))
            }
        }
    }
}
