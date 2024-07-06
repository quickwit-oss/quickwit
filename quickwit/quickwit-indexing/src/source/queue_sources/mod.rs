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

pub mod coordinator;
mod local_state;
#[cfg(test)]
mod memory_queue;
mod message;
mod shared_state;
#[cfg(feature = "sqs")]
pub mod sqs_queue;
#[cfg(feature = "sqs")]
pub mod sqs_source;
mod visibility;

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::future::BoxFuture;
use message::RawMessage;

type ReceiveResult = anyhow::Result<Vec<RawMessage>>;

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
    /// this efficiently. On the other hand, when there is a message in the
    /// queue, it should be returned as quickly as possible, regardless of the
    /// `max_messages` parameter.
    ///
    /// As soon as the message is received, the caller is responsible for
    /// maintaining the message visibility in a timely fashion. Failing to do
    /// implies that duplicates will be received by other indexing pipelines,
    /// thus increasing competition for the commit lock.
    ///
    /// The self parameter is `Arc` to make the resulting future `'static` and
    /// thus easily wrappable by the `QueueReceiver`
    async fn receive(
        self: Arc<Self>,
        max_messages: usize,
        suggested_deadline: Duration,
    ) -> ReceiveResult;

    /// Tries to acknowledge (delete) the messages.
    ///
    /// The call returns `Ok(())` if at the message level:
    /// - the acknowledgement failed due to a transient failure
    /// - the message was already acknowledged
    /// - the message was not acknowledged in time and is back to the queue
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

/// A statefull wrapper around a `Queue` that chunks the slow `receive()` call
/// into shorter iterations. This enables yielding back to the actor system
/// without compromising on queue poll durations. Without this, an actor that
/// tries to receive messages from a `Queue` will be blocked for multiple seconds
/// before being able to process new mailbox messages (or shutting down).
pub struct QueueReceiver {
    queue: Arc<dyn Queue>,
    receive: Option<BoxFuture<'static, ReceiveResult>>,
    iteration: Duration,
}

impl QueueReceiver {
    pub fn new(queue: Arc<dyn Queue>, iteration: Duration) -> Self {
        Self {
            queue,
            receive: None,
            iteration,
        }
    }

    pub async fn receive(
        &mut self,
        max_messages: usize,
        suggested_deadline: Duration,
    ) -> ReceiveResult {
        if self.receive.is_none() {
            self.receive = Some(self.queue.clone().receive(max_messages, suggested_deadline));
        }
        tokio::select! {
            res = self.receive.as_mut().unwrap() => {
                self.receive = None;
                res
            }
            _ = tokio::time::sleep(self.iteration) => {
                Ok(Vec::new())
            }

        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::bail;

    use super::*;

    #[derive(Clone, Debug)]
    struct SleepyQueue {
        receive_sleep: Duration,
    }

    #[async_trait]
    impl Queue for SleepyQueue {
        async fn receive(
            self: Arc<Self>,
            _max_messages: usize,
            _suggested_deadline: Duration,
        ) -> anyhow::Result<Vec<RawMessage>> {
            tokio::time::sleep(self.receive_sleep).await;
            bail!("Waking up from my nap")
        }

        async fn acknowledge(&self, _ack_ids: &[String]) -> anyhow::Result<()> {
            unimplemented!()
        }

        async fn modify_deadlines(
            &self,
            _ack_id: &str,
            _suggested_deadline: Duration,
        ) -> anyhow::Result<Instant> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_queue_receiver_slow_receive() {
        let queue = Arc::new(SleepyQueue {
            receive_sleep: Duration::from_millis(100),
        });
        let mut receiver = QueueReceiver::new(queue, Duration::from_millis(20));
        let mut iterations = 0;
        while receiver.receive(1, Duration::from_secs(1)).await.is_ok() {
            iterations += 1;
        }
        assert!(iterations >= 4);
    }

    #[tokio::test]
    async fn test_queue_receiver_fast_receive() {
        let queue = Arc::new(SleepyQueue {
            receive_sleep: Duration::from_millis(10),
        });
        let mut receiver = QueueReceiver::new(queue, Duration::from_millis(50));
        assert!(receiver.receive(1, Duration::from_secs(1)).await.is_err());
    }
}
