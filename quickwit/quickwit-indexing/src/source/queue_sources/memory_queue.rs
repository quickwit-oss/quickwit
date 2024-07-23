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

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use quickwit_storage::OwnedBytes;
use ulid::Ulid;

use super::message::{MessageMetadata, RawMessage};
use super::Queue;

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
#[derive(Clone, Default, Debug)]
pub struct MemoryQueue {
    inner_state: Arc<Mutex<InnerState>>,
}

impl MemoryQueue {
    pub fn send_message(&self, payload: String, ack_id: &str) {
        let message = RawMessage {
            payload: OwnedBytes::new(payload.into_bytes()),
            metadata: MessageMetadata {
                ack_id: ack_id.to_string(),
                delivery_attempts: 0,
                initial_deadline: Instant::now() + Duration::from_secs(30),
                message_id: Ulid::new().to_string(),
            },
        };
        self.inner_state.lock().unwrap().in_queue.push_back(message);
    }
}

#[async_trait]
impl Queue for MemoryQueue {
    async fn receive(&self, max_messages: usize) -> anyhow::Result<Vec<RawMessage>> {
        for _ in 0..3 {
            {
                let mut inner_state = self.inner_state.lock().unwrap();
                let mut response = Vec::new();
                while let Some(msg) = inner_state.in_queue.pop_front() {
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
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
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
        _ack_id: &str,
        suggested_deadline: Duration,
    ) -> anyhow::Result<Instant> {
        // TODO implement deadlines
        return Ok(Instant::now() + suggested_deadline);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prefilled_queue(nb_message: usize) -> MemoryQueue {
        let memory_queue = MemoryQueue::default();
        for i in 0..nb_message {
            let payload = format!("Test message {}", i);
            let ack_id = i.to_string();
            memory_queue.send_message(payload.clone(), &ack_id);
        }
        memory_queue
    }

    #[tokio::test]
    async fn test_receive_1_by_1() {
        let memory_queue = prefilled_queue(2);
        for i in 0..2 {
            let messages = memory_queue.receive(1).await.unwrap();
            assert_eq!(messages.len(), 1);
            let message = &messages[0];
            let exp_payload = format!("Test message {}", i);
            let exp_ack_id = i.to_string();
            assert_eq!(message.payload.as_ref(), exp_payload.as_bytes());
            assert_eq!(message.metadata.ack_id, exp_ack_id);
        }
    }

    #[tokio::test]
    async fn test_receive_2_by_2() {
        let memory_queue = prefilled_queue(2);
        let messages = memory_queue.receive(2).await.unwrap();
        assert_eq!(messages.len(), 2);
        for (i, message) in messages.iter().enumerate() {
            let exp_payload = format!("Test message {}", i);
            let exp_ack_id = i.to_string();
            assert_eq!(message.payload.as_ref(), exp_payload.as_bytes());
            assert_eq!(message.metadata.ack_id, exp_ack_id);
        }
    }

    #[tokio::test]
    async fn test_receive_early_if_only_1() {
        let memory_queue = prefilled_queue(1);
        let messages = memory_queue.receive(2).await.unwrap();
        assert_eq!(messages.len(), 1);
        let message = &messages[0];
        let exp_payload = "Test message 0".to_string();
        let exp_ack_id = "0";
        assert_eq!(message.payload.as_ref(), exp_payload.as_bytes());
        assert_eq!(message.metadata.ack_id, exp_ack_id);
    }
}
