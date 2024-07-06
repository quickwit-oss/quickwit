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

mod local_state;
#[cfg(test)]
mod memory_queue;
mod message;
mod processor;
mod shared_state;
#[cfg(feature = "sqs")]
mod sqs_queue;
#[cfg(feature = "sqs")]
pub mod sqs_source;
mod visibility;

use std::fmt;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use message::{PreProcessedMessage, RawMessage};

/// The queue abstraction is based on the AWS SQS and Google Pubsub APIs. The
/// only requirement of the underlying implementation is that messages exposed
/// to a given consumer are hidden to other consumers for a configurable period
/// of time. Retries are
#[async_trait]
pub trait Queue: fmt::Debug + Send + Sync + 'static {
    /// Poll the queue to receive messages.
    ///
    /// The implementation is in charge of choosing the wait strategy when there
    /// are no messages in the queue. It will typically use long polling to do
    /// this efficiently. On the other hand, when there is a message in the
    /// queue, it should be returned as quickly as possible.
    async fn receive(&self) -> anyhow::Result<Vec<RawMessage>>;

    /// Try to acknowledge the messages, effectively deleting them from the queue.
    ///
    /// The call might return `Ok(())` yet fail partially:
    /// - if it's a transient failure? -> TODO check
    /// - if the message was already acknowledged
    async fn acknowledge(&self, ack_ids: &[&str]) -> anyhow::Result<()>;

    /// Modify the visibility deadline of the messages.
    ///
    /// We try to set the initial visibility large enough to avoid having to
    /// call this too often. The implementation can retry as long as desired,
    /// it's the caller's responsibility to cancel the task if the deadline is
    /// about to expire. The returned `Instant` is a conservative estimate of
    /// the new deadline expiration time.
    async fn modify_deadlines(
        &self,
        ack_id: &str,
        suggested_deadline: Duration,
    ) -> anyhow::Result<Instant>;
}

pub struct Categorized<U, V> {
    pub processable: Vec<U>,
    pub already_processed: Vec<V>,
}

/// Acknowledges a list of messages
pub async fn acknowledge(
    queue: &dyn Queue,
    messages: Vec<PreProcessedMessage>,
) -> anyhow::Result<()> {
    let ack_ids = messages
        .iter()
        .map(|message| message.metadata.ack_id.as_str())
        .collect::<Vec<_>>();
    queue.acknowledge(&ack_ids).await
}
