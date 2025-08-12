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

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::bail;
use async_trait::async_trait;
use quickwit_storage::OwnedBytes;
use ulid::Ulid;

use super::Queue;
use super::message::{MessageMetadata, RawMessage};

#[derive(Default)]
struct InnerState {
    in_queue: VecDeque<RawMessage>,
    in_flight: BTreeMap<String, RawMessage>,
    acked: Vec<RawMessage>,
}

impl fmt::Debug for InnerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Queue")
            .field("in_queue_count", &self.in_queue.len())
            .field("in_flight_count", &self.in_flight.len())
            .field("acked_count", &self.acked.len())
            .finish()
    }
}

/// A simple in-memory queue
#[derive(Clone, Debug)]
pub struct MemoryQueueForTests {
    inner_state: Arc<Mutex<InnerState>>,
    receive_sleep: Duration,
}

impl MemoryQueueForTests {
    pub fn new() -> Self {
        let inner_state = Arc::new(Mutex::new(InnerState::default()));
        let inner_weak = Arc::downgrade(&inner_state);
        tokio::spawn(async move {
            loop {
                if let Some(inner_state) = inner_weak.upgrade() {
                    let mut inner_state = inner_state.lock().unwrap();
                    let mut expired = Vec::new();
                    for (ack_id, msg) in inner_state.in_flight.iter() {
                        if msg.metadata.initial_deadline < Instant::now() {
                            expired.push(ack_id.clone());
                        }
                    }
                    for ack_id in expired {
                        let msg = inner_state.in_flight.remove(&ack_id).unwrap();
                        inner_state.in_queue.push_back(msg);
                    }
                } else {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });
        MemoryQueueForTests {
            inner_state: Arc::new(Mutex::new(InnerState::default())),
            receive_sleep: Duration::from_millis(50),
        }
    }

    pub fn send_message(&self, payload: String, ack_id: &str) {
        let message = RawMessage {
            payload: OwnedBytes::new(payload.into_bytes()),
            metadata: MessageMetadata {
                ack_id: ack_id.to_string(),
                delivery_attempts: 0,
                initial_deadline: Instant::now(),
                message_id: Ulid::new().to_string(),
            },
        };
        self.inner_state.lock().unwrap().in_queue.push_back(message);
    }

    /// Returns the next visibility deadline for the message if it is in flight
    pub fn next_visibility_deadline(&self, ack_id: &str) -> Option<Instant> {
        let inner_state = self.inner_state.lock().unwrap();
        inner_state
            .in_flight
            .get(ack_id)
            .map(|msg| msg.metadata.initial_deadline)
    }
}

#[async_trait]
impl Queue for MemoryQueueForTests {
    async fn receive(
        self: Arc<Self>,
        max_messages: usize,
        suggested_deadline: Duration,
    ) -> anyhow::Result<Vec<RawMessage>> {
        {
            let mut inner_state = self.inner_state.lock().unwrap();
            let mut response = Vec::new();
            while let Some(mut msg) = inner_state.in_queue.pop_front() {
                msg.metadata.delivery_attempts += 1;
                msg.metadata.initial_deadline = Instant::now() + suggested_deadline;
                let msg_cloned = RawMessage {
                    payload: msg.payload.clone(),
                    metadata: msg.metadata.clone(),
                };
                inner_state
                    .in_flight
                    .insert(msg.metadata.ack_id.clone(), msg_cloned);
                response.push(msg);
                if response.len() >= max_messages {
                    break;
                }
            }
            if !response.is_empty() {
                return Ok(response);
            }
        }
        // `sleep` to avoid using all the CPU when called in a loop
        tokio::time::sleep(self.receive_sleep).await;

        Ok(vec![])
    }

    async fn acknowledge(&self, ack_ids: &[String]) -> anyhow::Result<()> {
        let mut inner_state = self.inner_state.lock().unwrap();
        for ack_id in ack_ids {
            if let Some(msg) = inner_state.in_flight.remove(ack_id) {
                inner_state.acked.push(msg);
            }
        }
        Ok(())
    }

    async fn modify_deadlines(
        &self,
        ack_id: &str,
        suggested_deadline: Duration,
    ) -> anyhow::Result<Instant> {
        let mut inner_state = self.inner_state.lock().unwrap();
        let in_flight = inner_state.in_flight.get_mut(ack_id);
        if let Some(msg) = in_flight {
            msg.metadata.initial_deadline = Instant::now() + suggested_deadline;
        } else {
            bail!("ack_id {} not found in in-flight", ack_id);
        }
        return Ok(Instant::now() + suggested_deadline);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prefilled_queue(nb_message: usize) -> Arc<MemoryQueueForTests> {
        let memory_queue = MemoryQueueForTests::new();
        for i in 0..nb_message {
            let payload = format!("Test message {i}");
            let ack_id = i.to_string();
            memory_queue.send_message(payload.clone(), &ack_id);
        }
        Arc::new(memory_queue)
    }

    #[tokio::test]
    async fn test_receive_1_by_1() {
        let memory_queue = prefilled_queue(2);
        for i in 0..2 {
            let messages = memory_queue
                .clone()
                .receive(1, Duration::from_secs(5))
                .await
                .unwrap();
            assert_eq!(messages.len(), 1);
            let message = &messages[0];
            let exp_payload = format!("Test message {i}");
            let exp_ack_id = i.to_string();
            assert_eq!(message.payload.as_ref(), exp_payload.as_bytes());
            assert_eq!(message.metadata.ack_id, exp_ack_id);
        }
    }

    #[tokio::test]
    async fn test_receive_2_by_2() {
        let memory_queue = prefilled_queue(2);
        let messages = memory_queue
            .receive(2, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(messages.len(), 2);
        for (i, message) in messages.iter().enumerate() {
            let exp_payload = format!("Test message {i}");
            let exp_ack_id = i.to_string();
            assert_eq!(message.payload.as_ref(), exp_payload.as_bytes());
            assert_eq!(message.metadata.ack_id, exp_ack_id);
        }
    }

    #[tokio::test]
    async fn test_receive_early_if_only_1() {
        let memory_queue = prefilled_queue(1);
        let messages = memory_queue
            .receive(2, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);
        let message = &messages[0];
        let exp_payload = "Test message 0".to_string();
        let exp_ack_id = "0";
        assert_eq!(message.payload.as_ref(), exp_payload.as_bytes());
        assert_eq!(message.metadata.ack_id, exp_ack_id);
    }
}
