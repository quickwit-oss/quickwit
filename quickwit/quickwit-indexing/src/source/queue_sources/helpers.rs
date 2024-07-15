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
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use tracing::info;

use super::{Queue, ReceiveResult};

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

const MAX_OPTIMAL_RECEIVE_COUNT: usize = 100;

/// A helper that keeps track of processing durations to adjust the number of
/// messages requested from the queue. The optimal count should always be
/// between 1 and [`MAX_OPTIMAL_RECEIVE_COUNT`]
pub struct MessageCountOptimizer {
    last_message_enqueued_at: Instant,
    current_optimal_count: usize,
    current_optimization: i32,
}

impl MessageCountOptimizer {
    pub fn new() -> Self {
        Self {
            last_message_enqueued_at: Instant::now(),
            current_optimal_count: 1,
            current_optimization: 0,
        }
    }

    pub fn optimal_count(&self) -> usize {
        let cnt = self.current_optimal_count as i32 + self.current_optimization;
        assert!(cnt > 0, "Receive count should be positive");
        cnt as usize
    }

    /// Call when new messages are enqueued for processing
    pub fn new_ready_messages_enqueued(&mut self) {
        let new_optimal_count = self.optimal_count();
        if new_optimal_count != self.current_optimal_count {
            info!(
                old = self.current_optimal_count,
                new = new_optimal_count,
                "optimal receive count changed"
            );
        }
        self.current_optimal_count = new_optimal_count;
        self.current_optimization = 0;
        self.last_message_enqueued_at = Instant::now();
    }

    /// Call each time a message is read completely
    pub fn message_processed(&mut self) {
        let processing_duration = self.last_message_enqueued_at.elapsed();
        if processing_duration > Duration::from_secs(15) && self.current_optimal_count > 1 {
            // processing is slow, decrease the number of requested messages to
            // avoid starving other pipelines from messages to process
            self.current_optimization = -1;
        } else if processing_duration < Duration::from_secs(5)
            && self.current_optimal_count < MAX_OPTIMAL_RECEIVE_COUNT
        {
            // processing is fast, increase the number of requested messages to
            // avoid spending all the time fetching new messages
            self.current_optimization = 1;
        } else {
            self.current_optimization = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
        ) -> ReceiveResult {
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

    #[tokio::test]
    async fn test_message_count_optimizer() {
        let mut optimizer = MessageCountOptimizer::new();
        for _ in 0..=MAX_OPTIMAL_RECEIVE_COUNT + 1 {
            let initial_optimal_count = optimizer.optimal_count();
            optimizer.new_ready_messages_enqueued();
            assert!(optimizer.optimal_count() == initial_optimal_count);
            optimizer.message_processed();
            assert!(optimizer.optimal_count() >= 1);
            assert!(optimizer.optimal_count() <= MAX_OPTIMAL_RECEIVE_COUNT);
        }
    }
}
