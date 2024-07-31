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

use super::message::{InProgressMessage, ReadyMessage};

/// Tracks the state of the queue messages that are known to the owning indexing
/// pipeline.
///
/// Messages first land in the `ready_for_read` queue. They are then moved to
/// `read_in_progress` to track the reader's progress. Once the reader reaches
/// EOF, the message is transitioned as `awaiting_commit`. Once the message is
/// known to be fully indexed and committed (e.g after receiving the
/// `suggest_truncate` call), it is moved to `completed`.
#[derive(Default)]
pub struct QueueLocalState {
    /// Messages that were received from the queue and are ready to be read
    ready_for_read: VecDeque<ReadyMessage>,
    /// Message that is currently being read and sent to the `DocProcessor`
    read_in_progress: Option<InProgressMessage>,
    /// Partitions that were read and are still being indexed, with their
    /// associated ack_id
    awaiting_commit: BTreeMap<PartitionId, String>,
    /// Partitions that were fully indexed and committed
    completed: BTreeSet<PartitionId>,
}

impl QueueLocalState {
    pub fn is_ready_for_read(&self, partition_id: &PartitionId) -> bool {
        self.ready_for_read
            .iter()
            .any(|msg| &msg.partition_id() == partition_id)
    }

    pub fn is_read_in_progress(&self, partition_id: &PartitionId) -> bool {
        self.read_in_progress
            .as_ref()
            .map_or(false, |msg| msg.partition_id() == partition_id)
    }

    pub fn is_awating_commit(&self, partition_id: &PartitionId) -> bool {
        self.awaiting_commit.contains_key(partition_id)
    }

    pub fn is_completed(&self, partition_id: &PartitionId) -> bool {
        self.completed.contains(partition_id)
    }

    pub fn is_tracked(&self, partition_id: &PartitionId) -> bool {
        self.is_ready_for_read(partition_id)
            || self.is_read_in_progress(partition_id)
            || self.is_awating_commit(partition_id)
            || self.is_completed(partition_id)
    }

    pub fn set_ready_for_read(&mut self, ready_messages: Vec<ReadyMessage>) {
        for message in ready_messages {
            self.ready_for_read.push_back(message)
        }
    }

    pub fn get_ready_for_read(&mut self) -> Option<ReadyMessage> {
        while let Some(msg) = self.ready_for_read.pop_front() {
            // don't return messages for which we didn't manage to extend the
            // visibility, they will pop up in the queue again anyway
            if !msg.visibility_handle.extension_failed() {
                return Some(msg);
            }
        }
        None
    }

    pub fn read_in_progress_mut(&mut self) -> Option<&mut InProgressMessage> {
        self.read_in_progress.as_mut()
    }

    pub fn replace_currently_read(&mut self, in_progress: Option<InProgressMessage>) {
        if let Some(just_finished) = self.read_in_progress.take() {
            self.awaiting_commit.insert(
                just_finished.partition_id().clone(),
                just_finished.ack_id().to_string(),
            );
        }
        self.read_in_progress = in_progress;
    }

    /// Returns the ack_id if that message was awaiting_commit
    pub fn mark_completed(&mut self, partition_id: PartitionId) -> Option<String> {
        let ack_id_opt = self.awaiting_commit.remove(&partition_id);
        self.completed.insert(partition_id);
        ack_id_opt
    }
}
