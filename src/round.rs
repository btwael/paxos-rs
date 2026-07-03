use crate::Slot;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use traceforge_rounds::{Dim, Round};

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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Round)]
pub struct PaxosRound {
    slot: u32,
    ballot: u32,
    phase: Phase,
}

impl PaxosRound {
    pub fn new(slot: Slot, ballot: u32, phase: Phase) -> PaxosRound {
        let slot =
            u32::try_from(slot).expect("TraceForge Paxos rounds support slots up to u32::MAX");
        PaxosRound { slot, ballot, phase }
    }

    pub fn paxos_slot(&self) -> Slot {
        self.slot as Slot
    }
}
