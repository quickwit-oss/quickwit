// Copyright (C) 2024 Quickwit, Inc.
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

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use quickwit_metastore::checkpoint::PartitionId;

use super::message::{CheckpointedMessage, InProgressMessage, PreProcessedMessage};
use super::visibility::VisibilityTaskHandle;
use super::Categorized;

/// Tracks the state of the the queue messages that is known to the owning
/// indexing pipeline.
///
/// Messages first land in the `ready` queue. In parallel, they are also
/// recorded as `in_flight`, where the visibility handle is kept alive. Messages
/// are then moved to `in_progress` to track the reader's progress. It's only
/// when the events are published that the in-flight visiblity handles are
/// dropped and the partition is marked as `completed``.
#[derive(Default)]
pub struct QueueLocalState {
    in_flight: BTreeMap<PartitionId, VisibilityTaskHandle>,
    ready: VecDeque<CheckpointedMessage>,
    in_progress: Option<InProgressMessage>,
    completed: BTreeSet<PartitionId>,
}

impl QueueLocalState {
    /// Split the message list into:
    /// - processable (returned)
    /// - already processed (returned)
    /// - tracked as in progress (dropped)
    pub fn filter_completed(
        &self,
        messages: Vec<PreProcessedMessage>,
    ) -> Categorized<PreProcessedMessage, PreProcessedMessage> {
        let mut processable = Vec::new();
        let mut completed = Vec::new();
        for message in messages {
            let partition_id = message.partition_id();
            if self.completed.contains(&partition_id) {
                completed.push(message);
            } else if self.in_flight.contains_key(&partition_id) {
                // already in progress, drop the message
            } else {
                processable.push(message);
            }
        }
        Categorized {
            already_processed: completed,
            processable,
        }
    }

    pub fn set_ready_messages(
        &mut self,
        ready_messages: Vec<(CheckpointedMessage, VisibilityTaskHandle)>,
    ) {
        for (message, handle) in ready_messages {
            let partition_id = message.partition_id();
            self.in_flight.insert(partition_id, handle);
            self.ready.push_back(message)
        }
    }

    pub fn in_progress_mut(&mut self) -> Option<&mut InProgressMessage> {
        self.in_progress.as_mut()
    }

    pub fn set_in_progress(&mut self, in_progress: Option<InProgressMessage>) {
        self.in_progress = in_progress;
    }

    pub fn get_ready_message(&mut self) -> Option<CheckpointedMessage> {
        self.ready.pop_front()
    }

    pub fn mark_completed(&mut self, partition_id: PartitionId) -> Option<VisibilityTaskHandle> {
        let visibility_handle = self.in_flight.remove(&partition_id);
        self.completed.insert(partition_id);
        visibility_handle
    }
}

#[cfg(test)]
pub mod test_helpers {
    use quickwit_metastore::checkpoint::PartitionId;

    use super::*;

    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    pub enum LocalStateForPartition {
        Unknown,
        Ready,
        ReadInProgress,
        WaitForCommit,
        Completed,
    }

    impl QueueLocalState {
        pub fn state(&self, partition_id: &PartitionId) -> LocalStateForPartition {
            let is_completed = self.completed.contains(partition_id);
            let is_read_in_progress = self
                .in_progress
                .as_ref()
                .map(|m| m.partition_id() == partition_id)
                .unwrap_or(false);
            let is_wait_for_commit =
                self.in_flight.contains_key(partition_id) && !is_read_in_progress;
            let is_ready = self.ready.iter().any(|m| m.partition_id() == *partition_id);

            let states = [
                is_completed,
                is_read_in_progress,
                is_wait_for_commit,
                is_ready,
            ];
            let simulteanous_states = states.into_iter().filter(|x| *x).count();
            assert!(simulteanous_states <= 1);

            if is_completed {
                LocalStateForPartition::Completed
            } else if is_ready {
                LocalStateForPartition::Ready
            } else if is_read_in_progress {
                LocalStateForPartition::ReadInProgress
            } else if is_wait_for_commit {
                LocalStateForPartition::WaitForCommit
            } else {
                LocalStateForPartition::Unknown
            }
        }
    }
}
