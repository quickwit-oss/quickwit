// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

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
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
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
