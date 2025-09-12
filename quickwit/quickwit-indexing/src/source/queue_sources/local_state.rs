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

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use anyhow::bail;
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
            .map(|msg| &msg.partition_id == partition_id)
            .unwrap_or(false)
    }

    pub fn is_awaiting_commit(&self, partition_id: &PartitionId) -> bool {
        self.awaiting_commit.contains_key(partition_id)
    }

    pub fn is_completed(&self, partition_id: &PartitionId) -> bool {
        self.completed.contains(partition_id)
    }

    pub fn is_tracked(&self, partition_id: &PartitionId) -> bool {
        self.is_ready_for_read(partition_id)
            || self.is_read_in_progress(partition_id)
            || self.is_awaiting_commit(partition_id)
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

    pub async fn drop_currently_read(&mut self) -> anyhow::Result<()> {
        if let Some(in_progress) = self.read_in_progress.take() {
            self.awaiting_commit.insert(
                in_progress.partition_id.clone(),
                in_progress.visibility_handle.ack_id().to_string(),
            );
            in_progress
                .visibility_handle
                .request_last_extension()
                .await?;
        }
        Ok(())
    }

    /// Tries to set the message that is currently being read. Returns an error
    /// if there is already a message being read.
    pub fn set_currently_read(
        &mut self,
        in_progress: Option<InProgressMessage>,
    ) -> anyhow::Result<()> {
        if self.read_in_progress.is_some() {
            bail!("trying to replace in progress message");
        }
        self.read_in_progress = in_progress;
        Ok(())
    }

    /// Returns the ack_id if that message was awaiting_commit
    pub fn mark_completed(&mut self, partition_id: PartitionId) -> Option<String> {
        let ack_id_opt = self.awaiting_commit.remove(&partition_id);
        self.completed.insert(partition_id);
        ack_id_opt
    }
}
