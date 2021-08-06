use std::fmt;

/// Object returned by [ActorHandle::observe()].
//
// TODO It should be a struct with a kind enum rather than a rich enum
#[derive(Debug)]
pub enum Observation<ObservableState> {
    /// The actor is alive and was able to snapshot its state within `HEARTBEAT`
    Running(ObservableState),
    /// The actor is terminated. The post-mortem state was snapshotted and is joined.
    Terminated(ObservableState),
    /// Timeout is returned if an observation could not be made with HEARTBEAT, because
    /// the actor had too much work. In that case, in a best effort fashion, the
    /// last observed state is returned.
    Timeout(ObservableState),
}

impl<ObservableState> Observation<ObservableState> {
    pub fn into_inner(self) -> ObservableState {
        match self {
            Observation::Running(obs) => obs,
            Observation::Terminated(obs) => obs,
            Observation::Timeout(obs) => obs,
        }
    }
}

impl<ObservableState: fmt::Debug> Observation<ObservableState> {
    pub fn state(&self) -> &ObservableState {
        match self {
            Observation::Running(state) => state,
            Observation::Terminated(state) => state,
            Observation::Timeout(state) => state,
        }
    }
}

impl<State: fmt::Debug + PartialEq> PartialEq for Observation<State> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Observation::Running(left), Observation::Running(right)) => left.eq(right),
            (Observation::Terminated(left), Observation::Terminated(right)) => left.eq(right),
            (Observation::Timeout(left), Observation::Timeout(right)) => left.eq(right),
            _ => false,
        }
    }
}

impl<State: fmt::Debug + PartialEq + Eq> Eq for Observation<State> {}
