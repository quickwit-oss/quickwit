// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;
use std::ops::Deref;

#[derive(Debug)]
pub struct Observation<ObservableState> {
    pub obs_type: ObservationType,
    pub state: ObservableState,
}

impl<ObservableState> Deref for Observation<ObservableState> {
    type Target = ObservableState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

// Describes the actual outcome of observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationType {
    /// The actor is alive and was able to snapshot its state within `HEARTBEAT`
    Alive,
    /// An observation could not be made with HEARTBEAT, because
    /// the actor had too much work. In that case, in a best effort fashion, the
    /// last observed state is returned. The actor will still update its state,
    /// as soon as it has finished processing the current message.
    Timeout,
    /// The actor has exited. The post-mortem state is joined.
    PostMortem,
}

impl<State: fmt::Debug + PartialEq> PartialEq for Observation<State> {
    fn eq(&self, other: &Self) -> bool {
        self.obs_type.eq(&other.obs_type) && self.state.eq(&other.state)
    }
}

impl<State: fmt::Debug + PartialEq + Eq> Eq for Observation<State> {}
