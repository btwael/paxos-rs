use serde::{Deserialize, Serialize};
use traceforge_rounds::{Dim, Round, set::KeyScheme};

use crate::{NodeId, Slot};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Dim)]
pub enum Phase {
    Prepare,
    Promise,
    Accept,
    Accepted,
    Resolution,
    Catchup,
    Reject,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum PaxosKey {
    ProposalTo(NodeId),
    Slot { slot: Slot, proposer: NodeId },
    Catchup { slot: Slot, leader: NodeId },
}

impl KeyScheme for PaxosKey {
    const LEN: usize = 4;

    fn encode(&self, out: &mut Vec<u32>) {
        match self {
            Self::ProposalTo(node) => {
                out.push(0);
                out.push(*node);
                encode_slot(0, out);
            }
            Self::Slot { slot, proposer } => {
                out.push(1);
                out.push(*proposer);
                encode_slot(*slot, out);
            }
            Self::Catchup { slot, leader } => {
                out.push(2);
                out.push(*leader);
                encode_slot(*slot, out);
            }
        }
    }

    fn decode(raw: &[u32]) -> Option<Self> {
        if raw.len() != Self::LEN {
            return None;
        }
        let node = raw[1];
        let slot = ((raw[2] as u64) << 32) | raw[3] as u64;
        match raw[0] {
            0 if slot == 0 => Some(Self::ProposalTo(node)),
            1 => Some(Self::Slot { slot, proposer: node }),
            2 => Some(Self::Catchup { slot, leader: node }),
            _ => None,
        }
    }
}

fn encode_slot(slot: Slot, out: &mut Vec<u32>) {
    out.push((slot >> 32) as u32);
    out.push(slot as u32);
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Round)]
pub struct PaxosRound {
    ballot: u32,
    phase: Phase,
}

impl PaxosRound {
    pub fn new(ballot: u32, phase: Phase) -> PaxosRound {
        PaxosRound { ballot, phase }
    }
}
