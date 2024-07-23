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

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;

use super::message::RawMessage;
use super::Queue;

type ReceiveResult = anyhow::Result<Vec<RawMessage>>;

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
    ) -> anyhow::Result<Vec<RawMessage>> {
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
    use std::time::{Duration, Instant};

    use anyhow::bail;
    use async_trait::async_trait;

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
