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
    async fn receive(&self) -> anyhow::Result<Vec<RawMessage>> {
        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(100) {
            {
                let mut inner_state = self.inner_state.lock().unwrap();
                if let Some(msg) = inner_state.in_queue.pop_back() {
                    inner_state
                        .in_flight
                        .insert(msg.metadata.ack_id.clone(), msg.clone());
                    return Ok(vec![msg]);
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        Ok(vec![])
    }

    async fn acknowledge(&self, ack_ids: &Vec<String>) -> anyhow::Result<()> {
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
        return Ok(Instant::now() + suggested_deadline);
    }
}
