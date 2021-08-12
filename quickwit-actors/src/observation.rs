use std::fmt;

#[derive(Debug)]
pub struct Observation<ObservableState> {
    pub obs_type: ObservationType,
    pub state: ObservableState,
}

// Describes the actual outcome of observation.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ObservationType {
    /// The actor is alive and was able to snapshot its state within `HEARTBEAT`
    Success,
    /// The actor is terminated. The post-mortem state was snapshotted and is joined.
    LastStateAfterActorTerminated,
    /// Timeout is returned if an observation could not be made with HEARTBEAT, because
    /// the actor had too much work. In that case, in a best effort fashion, the
    /// last observed state is returned.
    Timeout,
}

impl<State: fmt::Debug + PartialEq> PartialEq for Observation<State> {
    fn eq(&self, other: &Self) -> bool {
        self.obs_type.eq(&other.obs_type) && self.state.eq(&other.state)
    }
}

impl<State: fmt::Debug + PartialEq + Eq> Eq for Observation<State> {}
