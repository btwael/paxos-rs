use crate::{
    Replica,
    commands::{Command, Receiver},
    window::DecisionSet,
};
use std::time::{Duration, Instant};

/// Adds liveness to a commander by taking leadership
/// when a timeout occurs
pub struct Liveness<R: Replica> {
    inner: R,
    leader_election: Timeout,
}

impl<R: Replica> Liveness<R> {
    pub(crate) fn new(inner: R) -> Liveness<R> {
        Liveness {
            inner,
            // TODO: configurable leadership election timeout
            leader_election: Timeout::new(Duration::from_secs(2)),
        }
    }
}

impl<R: Replica> Receiver for Liveness<R> {
    fn receive(&mut self, cmd: Command) {
        // Bump leadership timeout if the command is not a catchup or proposal
        match &cmd {
            &Command::Proposal(_) | &Command::Catchup { .. } => {}
            _ => self.leader_election.bump(),
        }

        self.inner.receive(cmd);
    }
}

impl<R: Replica> Replica for Liveness<R> {
    fn tick(&mut self) {
        let lapsed = if self.inner.is_leader() {
            self.leader_election.near()
        } else {
            self.leader_election.lapsed()
        };

        if lapsed {
            info!("Leadership timeout lapsed, proposing leadership");
            self.inner.propose_leadership();
            self.leader_election.clear();
        }

        self.inner.tick();
    }

    fn propose_leadership(&mut self) {
        self.inner.propose_leadership();
    }

    fn is_leader(&self) -> bool {
        self.inner.is_leader()
    }

    fn decisions(&self) -> DecisionSet {
        self.inner.decisions()
    }
}

struct Timeout {
    latest_message: Option<Instant>,
    timeout: Duration,
}

impl Timeout {
    fn new(timeout: Duration) -> Timeout {
        Timeout { latest_message: None, timeout }
    }

    fn clear(&mut self) {
        self.latest_message = None;
    }

    fn bump(&mut self) {
        trace!("Leadership timeout bumped");
        self.latest_message = Some(Instant::now());
    }

    fn lapsed(&self) -> bool {
        if let Some(latest) = self.latest_message {
            Instant::now() > latest + self.timeout
        } else {
            false
        }
    }

    fn near(&self) -> bool {
        if let Some(latest) = self.latest_message {
            Instant::now() > latest + (self.timeout / 2)
        } else {
            false
        }
    }

    #[cfg(test)]
    fn fast_forward(&mut self, d: Duration) {
        // "jump" in time for tests by setting the clock back
        self.latest_message = self.latest_message.take().map(|i| i - d - Duration::from_nanos(10));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Ballot, commands::Command, round::Phase};

    #[test]
    fn propose_does_not_bump_timeout() {
        let mut live = Liveness::new(Inner::default());
        live.receive(Command::Proposal("123".into()));

        // does not bump leadership
        assert!(live.leader_election.latest_message.is_none());
        assert_eq!(Command::Proposal("123".into()), live.inner.commands[0]);
    }

    #[test]
    fn commands_bump_timeout() {
        let mut live = Liveness::new(Inner::default());
        live.receive(Command::Prepare { slot: 0, ballot: Ballot(2, 3) });
        assert!(live.leader_election.latest_message.is_some());
        assert_eq!(live.inner.commands[0], Command::Prepare { slot: 0, ballot: Ballot(2, 3) });

        let mut live = Liveness::new(Inner::default());
        live.receive(Command::Promise {
            from: 0,
            slot: 0,
            ballot: Ballot(2, 3),
            accepted: Vec::new(),
        });
        assert!(live.leader_election.latest_message.is_some());
        assert_eq!(
            live.inner.commands[0],
            Command::Promise { from: 0, slot: 0, ballot: Ballot(2, 3), accepted: Vec::new() }
        );

        let mut live = Liveness::new(Inner::default());
        live.receive(Command::Reject {
            from: 4,
            slot: 0,
            proposed: Ballot(0, 1),
            preempted: Ballot(4, 5),
            phase: Phase::Prepare,
        });
        assert!(live.leader_election.latest_message.is_some());
        assert_eq!(
            live.inner.commands[0],
            Command::Reject {
                from: 4,
                slot: 0,
                proposed: Ballot(0, 1),
                preempted: Ballot(4, 5),
                phase: Phase::Prepare,
            }
        );

        let mut live = Liveness::new(Inner::default());
        live.receive(Command::Accept { slot: 0, ballot: Ballot(4, 5), value: "x".into() });
        assert!(live.leader_election.latest_message.is_some());
        assert_eq!(
            live.inner.commands[0],
            Command::Accept { slot: 0, ballot: Ballot(4, 5), value: "x".into() }
        );

        let mut live = Liveness::new(Inner::default());
        live.receive(Command::Accepted { from: 5, slot: 2, ballot: Ballot(1, 2) });
        assert!(live.leader_election.latest_message.is_some());
        assert_eq!(
            live.inner.commands[0],
            Command::Accepted { from: 5, slot: 2, ballot: Ballot(1, 2) }
        );

        let mut live = Liveness::new(Inner::default());
        live.receive(Command::Resolution { slot: 0, ballot: Ballot(1, 2), value: "x".into() });
        assert!(live.leader_election.latest_message.is_some());
        assert_eq!(
            live.inner.commands[0],
            Command::Resolution { slot: 0, ballot: Ballot(1, 2), value: "x".into() }
        );
    }

    #[test]
    fn tick_leader() {
        let mut live = Liveness::new(Inner::default());
        live.inner.leader = true;
        assert!(live.is_leader());
        live.tick();
        assert!(!live.inner.proposed_leadership);

        // receive a message
        live.receive(Command::Accepted { from: 5, slot: 2, ballot: Ballot(1, 2) });
        live.tick();
        assert!(!live.inner.proposed_leadership);

        // jump forward the timeout duration
        live.leader_election.fast_forward(Duration::from_secs(1));

        live.tick();
        assert!(live.inner.proposed_leadership);
    }

    #[test]
    fn tick_follower() {
        let mut live = Liveness::new(Inner::default());
        live.inner.leader = false;
        assert!(!live.is_leader());
        live.tick();
        assert!(!live.inner.proposed_leadership);

        // receive a message
        live.receive(Command::Resolution { slot: 0, ballot: Ballot(0, 1), value: "x".into() });
        live.tick();
        assert!(!live.inner.proposed_leadership);

        // jump forward the timeout duration
        live.leader_election.fast_forward(Duration::from_secs(2));

        live.tick();
        assert!(live.inner.proposed_leadership);
    }

    #[derive(Default)]
    struct Inner {
        commands: Vec<Command>,
        leader: bool,
        proposed_leadership: bool,
    }

    impl Receiver for Inner {
        fn receive(&mut self, cmd: Command) {
            self.commands.push(cmd);
        }
    }

    impl Replica for Inner {
        fn propose_leadership(&mut self) {
            self.proposed_leadership = true;
        }

        fn is_leader(&self) -> bool {
            self.leader
        }

        fn tick(&mut self) {}
        fn decisions(&self) -> DecisionSet {
            unimplemented!();
        }
    }
}
