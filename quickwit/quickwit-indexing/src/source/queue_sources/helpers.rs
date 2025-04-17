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

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;

use super::Queue;
use super::message::RawMessage;

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
