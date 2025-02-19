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

pub mod coordinator;
mod helpers;
mod local_state;
#[cfg(test)]
mod memory_queue;
mod message;
mod shared_state;
#[cfg(feature = "sqs")]
pub mod sqs_queue;
mod visibility;

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use message::RawMessage;

/// The queue abstraction is based on the AWS SQS and Google Pubsub APIs. The
/// only requirement of the underlying implementation is that messages exposed
/// to a given consumer are hidden to other consumers for a configurable period
/// of time. Retries are handled by the implementation because queues might
/// behave differently (throttling, deduplication...).
#[async_trait]
pub trait Queue: fmt::Debug + Send + Sync + 'static {
    /// Polls the queue to receive messages.
    ///
    /// The implementation is in charge of choosing the wait strategy when there
    /// are no messages in the queue. It will typically use long polling to do
    /// this efficiently. On the other hand, when there is a message available
    /// in the queue, it should be returned as quickly as possible, regardless
    /// of the `max_messages` parameter. The `max_messages` paramater should
    /// always be clamped by the implementation to not violate the maximum value
    /// supported by the backing queue (e.g 10 messages for AWS SQS).
    ///
    /// As soon as the message is received, the caller is responsible for
    /// maintaining the message visibility in a timely fashion. Failing to do so
    /// implies that duplicates will be received by other indexing pipelines,
    /// thus increasing competition for the commit lock.
    async fn receive(
        // `Arc` to make the resulting future `'static` and thus easily
        // wrappable by the `QueueReceiver`
        self: Arc<Self>,
        max_messages: usize,
        suggested_deadline: Duration,
    ) -> anyhow::Result<Vec<RawMessage>>;

    /// Tries to acknowledge (delete) the messages.
    ///
    /// The call returns `Ok(())` if at the message level:
    /// - the acknowledgement failed due to a transient failure
    /// - the message was already acknowledged
    /// - the message was not acknowledged in time and is back to the queue
    ///
    /// If an empty list of ack_ids is provided, the call should be a no-op.
    async fn acknowledge(&self, ack_ids: &[String]) -> anyhow::Result<()>;

    /// Modifies the visibility deadline of the messages.
    ///
    /// We try to set the initial visibility large enough to avoid having to
    /// call this too often. The implementation can retry as long as desired,
    /// it's the caller's responsibility to cancel the future if the deadline is
    /// getting to close to the expiration. The returned `Instant` is a
    /// conservative estimate of the new deadline expiration time.
    async fn modify_deadlines(
        &self,
        ack_id: &str,
        suggested_deadline: Duration,
    ) -> anyhow::Result<Instant>;
}
