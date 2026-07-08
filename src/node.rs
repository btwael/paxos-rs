use crate::{
    Ballot, Configuration, NodeId, Replica, Slot, StampedReceiver,
    acceptor::{AcceptResponse, PrepareResponse},
    commands::*,
    proposer::{Proposer, ProposerState},
    round::{PaxosKey, PaxosRound, Phase},
    window::{DecisionSet, SlotMutRef, SlotWindow},
};
use bytes::Bytes;
use traceforge_rounds::set::{SetComm, SetCommError, SetTransport};

/// State manager for multi-paxos group
pub struct Node<T: SetTransport<PaxosKey, PaxosRound, Command>> {
    comm: SetComm<PaxosKey, PaxosRound, T>,
    config: Configuration<T::Node>,
    proposer: Proposer,
    window: SlotWindow,
}

impl<T> Node<T>
where
    T: SetTransport<PaxosKey, PaxosRound, Command>,
    T::Node: Clone,
    T::Error: std::fmt::Debug,
{
    /// Node creation from a sender and starting configuration
    pub fn with_comm(
        comm: SetComm<PaxosKey, PaxosRound, T>,
        config: Configuration<T::Node>,
    ) -> Node<T> {
        let (p1_quorum, p2_quorum) = config.quorum_size();
        let node = config.current();
        Node {
            comm,
            config,
            proposer: Proposer::new(node, p1_quorum),
            window: SlotWindow::new(p2_quorum),
        }
    }

    /// Node creation from a transport and starting configuration.
    pub fn new(transport: T, config: Configuration<T::Node>) -> Node<T> {
        Self::with_comm(SetComm::new(transport), config)
    }

    pub fn comm(&self) -> &SetComm<PaxosKey, PaxosRound, T> {
        &self.comm
    }

    pub fn comm_mut(&mut self) -> &mut SetComm<PaxosKey, PaxosRound, T> {
        &mut self.comm
    }

    pub fn current_node_id(&self) -> NodeId {
        self.config.current()
    }

    pub fn current_round(&mut self, key: PaxosKey) -> PaxosRound {
        self.comm.on(key).rounds().current().clone()
    }

    pub fn open_slots(&self) -> Vec<Slot> {
        self.window.open_range().collect()
    }

    fn unresolved_open_slots(&mut self) -> Vec<Slot> {
        self.window
            .open_range()
            .collect::<Vec<_>>()
            .into_iter()
            .filter(|slot| matches!(self.window.slot_mut(*slot), SlotMutRef::Open(_)))
            .collect()
    }

    pub fn participant_node_ids(&self) -> Vec<NodeId> {
        let mut nodes = self.config.peer_node_ids().collect::<Vec<_>>();
        nodes.push(self.config.current());
        nodes.sort_unstable();
        nodes
    }

    pub fn highest_observed_ballot(&self) -> Option<Ballot> {
        self.proposer.highest_observed_ballot()
    }

    fn enter_round(&mut self, key: PaxosKey, ballot: Ballot, phase: Phase) {
        self.comm
            .on(key)
            .rounds()
            .jump(PaxosRound::new(ballot.0, phase))
            .expect("round movement must not move to the past");
    }

    /// Broadcast ACCEPT messages once the proposer has phase 1 quorum
    fn drive_accept(&mut self) {
        if !self.proposer.state().is_leader() {
            return;
        }

        let bal = self.proposer.highest_observed_ballot().unwrap();
        assert!(bal.1 == self.config.current());

        // add queued proposals to new slots
        for value in self.proposer.take_proposals() {
            let mut slot = self.window.next_slot();
            slot.acceptor().notice_value(bal, value.clone());
        }

        // queue up all accepts
        let accepts = self
            .window
            .open_range()
            .filter_map(|slot| {
                match self.window.slot_mut(slot) {
                    SlotMutRef::Open(ref mut open_slot) => {
                        if let Some((_, val)) = open_slot.acceptor().highest_value() {
                            // have the acceptor update the highest ballot to this one
                            open_slot.acceptor().notice_value(bal, val.clone());
                            Some((slot, val))
                        } else {
                            open_slot.acceptor().notice_value(bal, Bytes::default());
                            Some((slot, Bytes::default()))
                        }
                    }
                    SlotMutRef::Empty(empty_slot) => {
                        // fill the hole with an empty slot
                        let mut slot = empty_slot.fill();
                        slot.acceptor().notice_value(bal, Bytes::default());
                        Some((slot.slot(), Bytes::default()))
                    }
                    _ => None,
                }
            })
            .collect::<Vec<_>>();

        // send out the accepts
        for (slot, value) in accepts {
            self.broadcast_at(
                slot,
                bal,
                Phase::Accept,
                Command::Accept { slot, ballot: bal, value },
            );
        }
    }

    /// Forwards pending proposals to the new leader
    fn forward(&mut self) {
        if !self.proposer.state().is_follower() || self.proposer.is_proposal_queue_empty() {
            return;
        }

        let proposals = self.proposer.take_proposals();
        if let Some(Ballot(_, node)) = self.proposer.highest_observed_ballot() {
            if node == self.config.current() {
                for proposal in proposals {
                    self.proposer.push_proposal(proposal);
                }
                return;
            }
            for proposal in proposals.into_iter() {
                self.send(node, Command::Proposal(proposal));
            }
        }
    }

    fn observe_ballot_and_forward(&mut self, ballot: Ballot) {
        self.proposer.observe_ballot(ballot);
        self.forward();
    }

    #[inline(always)]
    fn send(&mut self, node: NodeId, cmd: Command) {
        assert_ne!(
            node,
            self.config.current(),
            "attempted to send command to self through peer transport: {:?}",
            cmd
        );
        let dst = self.config[node].clone();
        let key = cmd.key_for(node);
        self.comm.on(key).send(dst, cmd).expect("transport send failed");
    }

    #[inline(always)]
    fn send_at(&mut self, node: NodeId, slot: Slot, ballot: Ballot, phase: Phase, cmd: Command) {
        self.enter_round(PaxosKey::Slot { slot, proposer: ballot.1 }, ballot, phase);
        self.send(node, cmd);
    }

    #[inline(always)]
    fn send_catchup_at(&mut self, node: NodeId, slot: Slot, ballot: Ballot, cmd: Command) {
        if node == self.config.current() {
            return;
        }
        self.enter_round(PaxosKey::Catchup { slot, leader: node }, ballot, Phase::Catchup);
        self.send(node, cmd);
    }

    #[inline(always)]
    fn broadcast_at(&mut self, slot: Slot, ballot: Ballot, phase: Phase, cmd: Command) {
        self.enter_round(PaxosKey::Slot { slot, proposer: ballot.1 }, ballot, phase);
        for node in self.config.peer_node_ids().collect::<Vec<_>>() {
            self.send(node, cmd.clone());
        }
    }

    pub fn receive_stamped(&mut self, key: PaxosKey, round: PaxosRound, cmd: Command) {
        assert_eq!(
            key,
            cmd.key_for(self.config.current()),
            "transport key must match the command's Paxos key for this receiver"
        );
        if let Some((protocol_key, protocol_round)) = cmd.protocol_stamp() {
            assert_eq!(key, protocol_key, "transport key must match the command's Paxos key");
            assert_eq!(
                round, protocol_round,
                "transport stamp must match the command's Paxos round"
            );
            self.comm
                .on(key)
                .rounds()
                .jump(round)
                .expect("received stamped command must not move to the past");
        }
        self.receive(cmd);
    }

    fn receive_stamped_checked(&mut self, key: PaxosKey, round: PaxosRound, cmd: Command) -> bool {
        if key != cmd.key_for(self.config.current()) {
            return false;
        }

        if let Some((protocol_key, protocol_round)) = cmd.protocol_stamp() {
            if key != protocol_key || round != protocol_round {
                return false;
            }

            if self.comm.on(key).rounds().jump(round).is_err() {
                return false;
            }
        }

        self.receive(cmd);
        true
    }

    pub fn collect_promise_inbox(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        min: usize,
        max: usize,
    ) -> Result<usize, SetCommError<PaxosKey, PaxosRound, Command, T::Error>>
    where
        Command: 'static,
    {
        let key = PaxosKey::Slot { slot, proposer: ballot.1 };
        self.enter_round(key, ballot, Phase::Promise);
        let messages = self.comm.on(key).inbox_with_bounds_with::<Command, _>(
            min,
            Some(max),
            move |local, remote| {
                local.ballot() == remote.ballot()
                    && local.phase() == Phase::Promise
                    && remote.phase() == Phase::Promise
            },
        )?;

        let mut count = 0;
        for msg in messages.into_iter().flatten() {
            if let Command::Promise { .. } = msg {
                count += 1;
                self.receive(msg);
            }
        }
        Ok(count)
    }

    pub fn collect_accepted_inbox(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        min: usize,
        max: usize,
    ) -> Result<usize, SetCommError<PaxosKey, PaxosRound, Command, T::Error>>
    where
        Command: 'static,
    {
        let key = PaxosKey::Slot { slot, proposer: ballot.1 };
        self.enter_round(key, ballot, Phase::Accepted);
        let messages = self.comm.on(key).inbox_with_bounds_with::<Command, _>(
            min,
            Some(max),
            move |local, remote| {
                local.ballot() == remote.ballot()
                    && local.phase() == Phase::Accepted
                    && remote.phase() == Phase::Accepted
            },
        )?;

        let mut count = 0;
        for msg in messages.into_iter().flatten() {
            if let Command::Accepted { .. } = msg {
                count += 1;
                self.receive(msg);
            }
        }
        Ok(count)
    }
}

impl<T> StampedReceiver for Node<T>
where
    T: SetTransport<PaxosKey, PaxosRound, Command>,
    T::Node: Clone,
    T::Error: std::fmt::Debug,
{
    fn try_receive_stamped(&mut self, key: PaxosKey, round: PaxosRound, cmd: Command) -> bool {
        self.receive_stamped_checked(key, round, cmd)
    }
}

impl<T> Commander for Node<T>
where
    T: SetTransport<PaxosKey, PaxosRound, Command>,
    T::Node: Clone,
    T::Error: std::fmt::Debug,
{
    fn proposal(&mut self, val: Bytes) {
        // redirect to the distinguished proposer or start PREPARE
        match *self.proposer.state() {
            ProposerState::Follower if self.proposer.highest_observed_ballot().is_none() => {
                // no known proposers, go through prepare cycle
                self.proposer.push_proposal(val);
                self.propose_leadership();
            }
            ProposerState::Follower => {
                let leader_node = self.proposer.highest_observed_ballot().unwrap().1;
                if leader_node == self.config.current() {
                    self.proposer.push_proposal(val);
                    self.propose_leadership();
                } else {
                    self.send(leader_node, Command::Proposal(val));
                }
            }
            ProposerState::Candidate { .. } => {
                // still waiting for promises, queue up the value
                // TODO: should this re-send some PREPARE messages?
                self.proposer.push_proposal(val);
            }
            ProposerState::Leader { proposal: bal } => {
                // node is the distinguished proposer
                let slot = {
                    let mut slot_ref = self.window.next_slot();
                    slot_ref.acceptor().notice_value(bal, val.clone());
                    slot_ref.slot()
                };
                self.broadcast_at(
                    slot,
                    bal,
                    Phase::Accept,
                    Command::Accept { slot, ballot: bal, value: val },
                );
            }
        }
    }

    fn prepare(&mut self, slot: Slot, bal: Ballot) {
        self.proposer.observe_ballot(bal);

        let node_id = self.config.current();
        let mut accepted = Vec::new();
        let mut promise_slots = self.window.open_range().collect::<Vec<_>>();
        if !promise_slots.contains(&slot) {
            promise_slots.push(slot);
        }
        promise_slots.sort_unstable();

        for open_slot in promise_slots {
            let mut rejected = None;
            match self.window.slot_mut(open_slot) {
                SlotMutRef::Open(ref mut open_ref) => {
                    match open_ref.acceptor().receive_prepare(bal) {
                        PrepareResponse::Promise {
                            value: Some((accepted_ballot, value)), ..
                        } => {
                            accepted.push((open_slot, accepted_ballot, value));
                        }
                        PrepareResponse::Reject { proposed, preempted } => {
                            rejected = Some((proposed, preempted));
                        }
                        PrepareResponse::Promise { value: None, .. }
                        | PrepareResponse::Resolved => {}
                    }
                }
                SlotMutRef::Resolved(accepted_ballot, value) => {
                    accepted.push((open_slot, accepted_ballot, value));
                }
                SlotMutRef::Empty(_) => {
                    warn!("Empty slot {} detected in the middle of the open range", open_slot);
                }
                SlotMutRef::ResolutionTruncated => {
                    unreachable!("Cannot be resolved in the middle of the open range")
                }
            }
            if let Some((proposed, preempted)) = rejected {
                self.send_at(
                    bal.1,
                    slot,
                    proposed,
                    Phase::Reject,
                    Command::Reject {
                        from: node_id,
                        slot,
                        proposed,
                        preempted,
                        phase: Phase::Prepare,
                    },
                );
                return;
            }
        }

        self.send_at(
            bal.1,
            slot,
            bal,
            Phase::Promise,
            Command::Promise { from: node_id, slot, ballot: bal, accepted },
        );
    }

    fn promise(
        &mut self,
        node: NodeId,
        _slot: Slot,
        bal: Ballot,
        accepted: Vec<(Slot, Ballot, Bytes)>,
    ) {
        if !self.proposer.state().is_candidate() {
            return;
        }

        self.proposer.receive_promise(node, bal);

        // track highest proposals
        for (accepted_slot, accepted_ballot, val) in accepted {
            match self.window.slot_mut(accepted_slot) {
                SlotMutRef::Open(ref mut open_slot) => {
                    open_slot.acceptor().notice_value(accepted_ballot, val);
                }
                SlotMutRef::Empty(empty_slot) => {
                    empty_slot.fill().acceptor().notice_value(accepted_ballot, val);
                }
                _ => {}
            }
        }

        // if we have phase 1 quorum, we can send out ACCEPT messages
        self.drive_accept();
    }

    fn accept(&mut self, slot: Slot, bal: Ballot, val: Bytes) {
        self.observe_ballot_and_forward(bal);

        let current_node = self.config.current();
        let acceptor_res = match self.window.slot_mut(slot) {
            SlotMutRef::Empty(empty_slot) => {
                let mut open_slot = empty_slot.fill();
                open_slot.acceptor().receive_accept(bal, val)
            }
            SlotMutRef::Open(ref mut open_slot) => open_slot.acceptor().receive_accept(bal, val),
            _ => return,
        };

        match acceptor_res {
            AcceptResponse::Accepted { .. } => {
                self.send_at(
                    bal.1,
                    slot,
                    bal,
                    Phase::Accepted,
                    Command::Accepted { from: current_node, slot, ballot: bal },
                );
            }
            AcceptResponse::Reject { proposed, preempted } => {
                self.send_at(
                    bal.1,
                    slot,
                    proposed,
                    Phase::Reject,
                    Command::Reject {
                        from: current_node,
                        slot,
                        proposed,
                        preempted,
                        phase: Phase::Accept,
                    },
                );
            }
            AcceptResponse::Resolved => {}
        }
    }

    fn reject(
        &mut self,
        node: NodeId,
        _slot: Slot,
        proposed: Ballot,
        promised: Ballot,
        _phase: Phase,
    ) {
        // reject preempted ballot within the proposer
        self.proposer.receive_reject(node, proposed, promised);
        self.forward();
    }

    fn accepted(&mut self, node: NodeId, slot: Slot, bal: Ballot) {
        self.proposer.observe_ballot(bal);

        // notify each slot of the accepted, collecting resolutions
        let resolution = match self.window.slot_mut(slot) {
            SlotMutRef::Open(ref mut open_ref) => {
                open_ref.acceptor().receive_accepted(node, bal);
                open_ref.acceptor().resolution().map(|(_, value)| value)
            }
            SlotMutRef::Empty(_) => {
                warn!("Received accepted() for slot {} which is unknown", slot);
                None
            }
            _ => None,
        };

        if let Some(value) = resolution {
            self.broadcast_at(
                slot,
                bal,
                Phase::Resolution,
                Command::Resolution { slot, ballot: bal, value },
            );
        }
    }

    fn resolution(&mut self, slot: Slot, bal: Ballot, val: Bytes) {
        self.observe_ballot_and_forward(bal);

        match self.window.slot_mut(slot) {
            SlotMutRef::Empty(empty_slot) => empty_slot.fill().acceptor().resolve(bal, val),
            SlotMutRef::Open(ref mut open) => open.acceptor().resolve(bal, val),
            _ => {}
        }

        // Send catchup for holds in the decision making
        // We can skip catchup if we're caught up and the range only
        // contains one slot
        let range = self.window.open_range();
        if range.end > range.start + 1 {
            let slots = range
                .filter(|slot| {
                    if let SlotMutRef::Resolved(..) = self.window.slot_mut(*slot) {
                        false
                    } else {
                        true
                    }
                })
                .collect::<Vec<Slot>>();
            trace!("Sending catchup for slots {:?}", slots);
            let leader = self.proposer.highest_observed_ballot().unwrap().1;
            let node = self.config.current();
            for slot in slots {
                self.send_catchup_at(leader, slot, bal, Command::Catchup { from: node, slot });
            }
        }
    }

    fn catchup(&mut self, node: NodeId, slot: Slot) {
        // TODO: do we want to redirect at this point? Dropping is certainly safer
        if !self.is_leader() {
            return;
        }

        let resolved = match self.window.slot_mut(slot) {
            SlotMutRef::Resolved(bal, val) => Some((bal, val)),
            _ => None,
        };

        if let Some((bal, val)) = resolved {
            self.send_at(
                node,
                slot,
                bal,
                Phase::Resolution,
                Command::Resolution { slot, ballot: bal, value: val },
            );
        }
    }
}

impl<T> Replica for Node<T>
where
    T: SetTransport<PaxosKey, PaxosRound, Command>,
    T::Node: Clone,
    T::Error: std::fmt::Debug,
{
    fn propose_leadership(&mut self) {
        match *self.proposer.state() {
            ProposerState::Candidate { proposal, .. } => {
                for slot in self.unresolved_open_slots() {
                    self.broadcast_at(
                        slot,
                        proposal,
                        Phase::Prepare,
                        Command::Prepare { slot, ballot: proposal },
                    );
                }
            }
            ProposerState::Follower => {
                let bal = self.proposer.prepare();
                for slot in self.unresolved_open_slots() {
                    self.broadcast_at(
                        slot,
                        bal,
                        Phase::Prepare,
                        Command::Prepare { slot, ballot: bal },
                    );
                }
            }
            ProposerState::Leader { proposal } => {
                // TODO: do we want a special sync here? What about periodic bumping ballot?
                for slot in self.unresolved_open_slots() {
                    self.broadcast_at(
                        slot,
                        proposal,
                        Phase::Accept,
                        Command::Accept { slot, ballot: proposal, value: Bytes::default() },
                    );
                }
            }
        }
    }

    fn is_leader(&self) -> bool {
        self.proposer.state().is_leader()
    }

    fn tick(&mut self) {
        let current = self.config.current();
        for slot in self.window.open_range().collect::<Vec<_>>() {
            let _ = self.comm.on(PaxosKey::Slot { slot, proposer: current }).rounds().tick();
            let _ = self.comm.on(PaxosKey::Catchup { slot, leader: current }).rounds().tick();
        }
    }

    fn decisions(&self) -> DecisionSet {
        self.window.decisions()
    }
}

#[cfg(test)]
mod comm_tests {
    use super::*;
    use std::{convert::Infallible, ops::Index};
    use traceforge_rounds::set::SetEnvelope;

    fn config() -> Configuration<NodeId> {
        Configuration::new(4u32, vec![(0, 0), (1, 1), (2, 2), (3, 3)].into_iter())
    }

    #[derive(Default)]
    struct VecTransport([Vec<Command>; 4]);

    impl VecTransport {
        fn clear(&mut self) {
            for i in 0usize..4 {
                self.0[i].clear();
            }
        }
    }

    impl Index<usize> for VecTransport {
        type Output = [Command];

        fn index(&self, n: usize) -> &[Command] {
            assert!(n < 4);
            &self.0[n]
        }
    }

    impl SetTransport<PaxosKey, PaxosRound, Command> for VecTransport {
        type Node = NodeId;
        type Error = Infallible;

        fn send(
            &mut self,
            dst: Self::Node,
            envelope: SetEnvelope<PaxosKey, PaxosRound, Command>,
        ) -> Result<(), Self::Error> {
            assert!(dst < 4);
            let (_, _, msg) = envelope.into_parts();
            self.0[dst as usize].push(msg);
            Ok(())
        }

        fn recv<F>(
            &mut self,
            _key: &PaxosKey,
            _current: &PaxosRound,
            _filter: F,
        ) -> Result<Option<SetEnvelope<PaxosKey, PaxosRound, Command>>, Self::Error>
        where
            F: Fn(&PaxosRound, &PaxosRound) -> bool + Send + Sync + 'static,
        {
            Ok(None)
        }

        fn recv_block<F>(
            &mut self,
            _key: &PaxosKey,
            _current: &PaxosRound,
            _filter: F,
        ) -> Result<SetEnvelope<PaxosKey, PaxosRound, Command>, Self::Error>
        where
            F: Fn(&PaxosRound, &PaxosRound) -> bool + Send + Sync + 'static,
        {
            unreachable!("VecTransport has no blocking receive queue")
        }

        fn recv_keyed<F>(
            &mut self,
            _filter: F,
        ) -> Result<Option<SetEnvelope<PaxosKey, PaxosRound, Command>>, Self::Error>
        where
            F: Fn(&PaxosKey, &PaxosRound) -> bool + Send + Sync + 'static,
        {
            Ok(None)
        }

        fn inbox<F>(
            &mut self,
            _key: &PaxosKey,
            _current: &PaxosRound,
            _filter: F,
            _min: usize,
            _max: Option<usize>,
        ) -> Result<Vec<Option<SetEnvelope<PaxosKey, PaxosRound, Command>>>, Self::Error>
        where
            F: Fn(&PaxosRound, &PaxosRound) -> bool + Send + Sync + 'static,
        {
            Ok(Vec::new())
        }
    }

    #[test]
    fn proposal_starts_prepare_round() {
        let mut replica = Node::new(VecTransport::default(), config());

        replica.proposal("123".into());

        let expected = Command::Prepare { slot: 0, ballot: Ballot(0, 4) };
        for node in 0..4 {
            assert_eq!(&[expected.clone()], &replica.comm.transport()[node]);
        }
        assert_eq!(
            replica.comm.on(PaxosKey::Slot { slot: 0, proposer: 4 }).rounds().current(),
            &PaxosRound::new(0, Phase::Prepare)
        );
    }

    #[test]
    fn promise_quorum_drives_one_slot_accept() {
        let mut replica = Node::new(VecTransport::default(), config());
        replica.proposal("123".into());
        replica.comm.transport_mut().clear();

        replica.promise(0, 0, Ballot(0, 4), Vec::new());
        for node in 0..4 {
            assert!(replica.comm.transport()[node].is_empty());
        }

        replica.promise(2, 0, Ballot(0, 4), Vec::new());
        let expected = Command::Accept { slot: 0, ballot: Ballot(0, 4), value: "123".into() };
        for node in 0..4 {
            assert_eq!(&[expected.clone()], &replica.comm.transport()[node]);
        }
        assert_eq!(
            replica.comm.on(PaxosKey::Slot { slot: 0, proposer: 4 }).rounds().current(),
            &PaxosRound::new(0, Phase::Accept)
        );
    }

    #[test]
    fn accepted_quorum_broadcasts_resolution() {
        let mut replica = Node::new(VecTransport::default(), config());
        replica.proposal("123".into());
        replica.promise(1, 0, Ballot(0, 4), Vec::new());
        replica.promise(2, 0, Ballot(0, 4), Vec::new());
        replica.comm.transport_mut().clear();

        replica.accepted(0, 0, Ballot(0, 4));
        for node in 0..4 {
            assert!(replica.comm.transport()[node].is_empty());
        }

        replica.accepted(2, 0, Ballot(0, 4));
        let expected = Command::Resolution { slot: 0, ballot: Ballot(0, 4), value: "123".into() };
        for node in 0..4 {
            assert_eq!(&[expected.clone()], &replica.comm.transport()[node]);
        }
        assert_eq!(vec![(0, "123".into())], replica.window.decisions().iter().collect::<Vec<_>>());
    }

    #[test]
    fn resolution_requests_catchup_per_missing_slot() {
        let mut replica = Node::new(VecTransport::default(), config());

        replica.resolution(4, Ballot(1, 2), "123".into());

        for slot in 0..4 {
            assert_eq!(
                replica.comm.transport()[2][slot as usize],
                Command::Catchup { from: 4, slot }
            );
        }
    }

    #[test]
    fn stamped_proposal_does_not_import_envelope_round() {
        let mut replica = Node::new(VecTransport::default(), config());

        replica.receive_stamped(
            PaxosKey::ProposalTo(4),
            PaxosRound::new(3, Phase::Resolution),
            Command::Proposal("123".into()),
        );

        assert_eq!(
            replica.comm.on(PaxosKey::Slot { slot: 0, proposer: 4 }).rounds().current(),
            &PaxosRound::new(0, Phase::Prepare)
        );
    }

    #[test]
    fn stamped_catchup_does_not_import_envelope_round() {
        let mut replica = Node::new(VecTransport::default(), config());

        replica.receive_stamped(
            PaxosKey::Catchup { slot: 7, leader: 4 },
            PaxosRound::new(3, Phase::Catchup),
            Command::Catchup { from: 1, slot: 7 },
        );

        assert_eq!(
            replica.comm.on(PaxosKey::Slot { slot: 0, proposer: 4 }).rounds().current(),
            &PaxosRound::new(0, Phase::Prepare)
        );
    }

    #[test]
    fn liveness_retry_skips_already_resolved_slots() {
        let mut replica = Node::new(VecTransport::default(), config());

        replica.proposal("first".into());
        replica.promise(1, 0, Ballot(0, 4), Vec::new());
        replica.promise(2, 0, Ballot(0, 4), Vec::new());

        replica.receive_stamped(
            PaxosKey::Slot { slot: 1, proposer: 4 },
            PaxosRound::new(0, Phase::Resolution),
            Command::Resolution { slot: 1, ballot: Ballot(0, 4), value: "second".into() },
        );
        replica.comm.transport_mut().clear();

        replica.propose_leadership();

        let expected = Command::Accept { slot: 0, ballot: Ballot(0, 4), value: Bytes::default() };
        for node in 0..4 {
            assert_eq!(&[expected.clone()], &replica.comm.transport()[node]);
        }
        assert_eq!(
            replica.comm.on(PaxosKey::Slot { slot: 1, proposer: 4 }).rounds().current(),
            &PaxosRound::new(0, Phase::Resolution)
        );
    }

    #[test]
    fn resolution_can_follow_reject_for_same_ballot() {
        let mut replica = Node::new(VecTransport::default(), config());

        replica.receive_stamped(
            PaxosKey::Slot { slot: 0, proposer: 4 },
            PaxosRound::new(0, Phase::Reject),
            Command::Reject {
                from: 1,
                slot: 0,
                proposed: Ballot(0, 4),
                preempted: Ballot(1, 2),
                phase: Phase::Accept,
            },
        );
        replica.receive_stamped(
            PaxosKey::Slot { slot: 0, proposer: 4 },
            PaxosRound::new(0, Phase::Resolution),
            Command::Resolution { slot: 0, ballot: Ballot(0, 4), value: "chosen".into() },
        );

        assert_eq!(
            vec![(0, "chosen".into())],
            replica.window.decisions().iter().collect::<Vec<_>>()
        );
    }
}
