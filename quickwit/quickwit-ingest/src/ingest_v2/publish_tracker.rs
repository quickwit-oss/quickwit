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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use quickwit_common::pubsub::{EventBroker, EventSubscriptionHandle};
use quickwit_proto::indexing::ShardPositionsUpdate;
use quickwit_proto::types::{Position, ShardId};
use tokio::sync::Notify;
use tracing::error;

/// A helper for awaiting shard publish events when running in `wait_for` and
/// `force` commit mode.
///
/// Registers a set of shard positions and listens to [`ShardPositionsUpdate`]
/// events to assert when all the persisted events have been published. To
/// ensure that no events are missed:
/// - create the tracker before any persist requests is sent
/// - call `register_requested_shards` before each persist request to ensure that
/// the associated publish events are recorded
/// - call `track_persisted_shard_position` after each successful persist subrequests
pub struct PublishTracker {
    state: Arc<Mutex<ShardPublishStates>>,
    // sync::notify instead of sync::oneshot because we don't want to store the permit
    publish_complete: Arc<Notify>,
    _publish_listen_handle: EventSubscriptionHandle,
}

impl PublishTracker {
    pub fn new(event_tracker: EventBroker) -> Self {
        let state = Arc::new(Mutex::new(ShardPublishStates::default()));
        let state_clone = state.clone();
        let publish_complete = Arc::new(Notify::new());
        let publish_complete_notifier = publish_complete.clone();
        let _publish_listen_handle =
            event_tracker.subscribe(move |update: ShardPositionsUpdate| {
                let mut publish_states = state_clone.lock().unwrap();
                for (updated_shard_id, updated_position) in &update.updated_shard_positions {
                    publish_states.position_published(
                        updated_shard_id,
                        updated_position,
                        &publish_complete_notifier,
                    );
                }
            });
        Self {
            state,
            _publish_listen_handle,
            publish_complete,
        }
    }

    pub fn register_requested_shards<'a>(
        &'a self,
        shard_ids: impl IntoIterator<Item = &'a ShardId>,
    ) {
        let mut publish_states = self.state.lock().unwrap();
        for shard_id in shard_ids {
            publish_states.shard_tracked(shard_id.clone());
        }
    }

    pub fn track_persisted_shard_position(&self, shard_id: ShardId, new_position: Position) {
        let mut publish_states = self.state.lock().unwrap();
        publish_states.position_persisted(&shard_id, &new_position)
    }

    pub async fn wait_publish_complete(self) {
        // correctness: `awaiting_count` cannot be increased after this point
        // because `self` is consumed. By subscribing to `publish_complete`
        // before checking `awaiting_count`, we make sure we don't miss the
        // moment when it becomes 0.
        let notified = self.publish_complete.notified();
        if self.state.lock().unwrap().awaiting_count == 0 {
            return;
        }
        notified.await;
    }
}

enum PublishState {
    /// The persist request for this shard has been sent
    Tracked,
    /// The persist request for this shard success response has been received
    /// but the position has not yet been published
    AwaitingPublish(Position),
    ///  The shard has been published up to this position (might happen before
    ///  the persist success is received)
    Published(Position),
}

#[derive(Default)]
struct ShardPublishStates {
    states: HashMap<ShardId, PublishState>,
    awaiting_count: usize,
}

impl ShardPublishStates {
    fn shard_tracked(&mut self, shard_id: ShardId) {
        self.states.entry(shard_id).or_insert(PublishState::Tracked);
    }

    fn position_published(
        &mut self,
        shard_id: &ShardId,
        new_position: &Position,
        publish_complete_notifier: &Notify,
    ) {
        if let Some(publish_state) = self.states.get_mut(shard_id) {
            match publish_state {
                PublishState::AwaitingPublish(shard_position) if new_position >= shard_position => {
                    *publish_state = PublishState::Published(new_position.clone());
                    self.awaiting_count -= 1;
                    if self.awaiting_count == 0 {
                        // The notification is only relevant once
                        // `self.wait_publish_complete()` is called.
                        // Before that, `state.awaiting_publish` might
                        // still be re-populated.
                        publish_complete_notifier.notify_waiters();
                    }
                }
                PublishState::Published(current_position) if new_position > current_position => {
                    *current_position = new_position.clone();
                }
                PublishState::Tracked => {
                    *publish_state = PublishState::Published(new_position.clone());
                }
                PublishState::Published(_) => {
                    // looks like a duplicate or out-of-order event
                }
                PublishState::AwaitingPublish(_) => {
                    // the shard made some progress but we are waiting for more
                }
            }
        }
        // else: this shard is not being tracked here
    }

    fn position_persisted(&mut self, shard_id: &ShardId, new_position: &Position) {
        if let Some(publish_state) = self.states.get_mut(shard_id) {
            match publish_state {
                PublishState::Published(current_position) if new_position <= current_position => {
                    // new position already published, no need to track it
                }
                PublishState::AwaitingPublish(old_position) => {
                    error!(
                        %old_position,
                        %new_position,
                        %shard_id,
                        "shard persisted positions should not be tracked multiple times"
                    );
                }
                PublishState::Tracked | PublishState::Published(_) => {
                    *publish_state = PublishState::AwaitingPublish(new_position.clone());
                    self.awaiting_count += 1;
                }
            }
        } else {
            error!(%shard_id, "requested shards should be registered before their position is tracked")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_proto::types::{IndexUid, ShardId, SourceUid};

    use super::*;

    #[tokio::test]
    async fn test_publish_tracker() {
        let index_uid: IndexUid = IndexUid::for_test("test-index-0", 0);
        let event_broker = EventBroker::default();
        let tracker = PublishTracker::new(event_broker.clone());
        let shard_id_1 = ShardId::from("test-shard-1");
        let shard_id_2 = ShardId::from("test-shard-2");
        let shard_id_3 = ShardId::from("test-shard-3");
        let shard_id_4 = ShardId::from("test-shard-4");
        let shard_id_5 = ShardId::from("test-shard-5"); // not tracked

        tracker.register_requested_shards([&shard_id_1, &shard_id_2, &shard_id_3, &shard_id_4]);

        tracker.track_persisted_shard_position(shard_id_1.clone(), Position::offset(42usize));
        tracker.track_persisted_shard_position(shard_id_2.clone(), Position::offset(42usize));
        tracker.track_persisted_shard_position(shard_id_3.clone(), Position::offset(42usize));

        event_broker.publish(ShardPositionsUpdate {
            source_uid: SourceUid {
                index_uid: index_uid.clone(),
                source_id: "test-source".to_string(),
            },
            updated_shard_positions: vec![
                (shard_id_1.clone(), Position::offset(42usize)),
                (shard_id_2.clone(), Position::offset(666usize)),
                (shard_id_5.clone(), Position::offset(888usize)),
            ]
            .into_iter()
            .collect(),
        });

        event_broker.publish(ShardPositionsUpdate {
            source_uid: SourceUid {
                index_uid: index_uid.clone(),
                source_id: "test-source".to_string(),
            },
            updated_shard_positions: vec![
                (shard_id_3.clone(), Position::eof(42usize)),
                (shard_id_4.clone(), Position::offset(42usize)),
            ]
            .into_iter()
            .collect(),
        });

        // persist response received after the publish event
        tracker.track_persisted_shard_position(shard_id_4.clone(), Position::offset(42usize));

        tokio::time::timeout(Duration::from_millis(200), tracker.wait_publish_complete())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_publish_tracker_waits() {
        let index_uid: IndexUid = IndexUid::for_test("test-index-0", 0);
        let shard_id_1 = ShardId::from("test-shard-1");
        let shard_id_2 = ShardId::from("test-shard-2");
        let position = Position::offset(42usize);

        {
            let event_broker = EventBroker::default();
            let tracker = PublishTracker::new(event_broker.clone());
            tracker.register_requested_shards([&shard_id_1, &shard_id_2]);
            tracker.track_persisted_shard_position(shard_id_1.clone(), position.clone());
            tracker.track_persisted_shard_position(shard_id_2.clone(), position.clone());

            event_broker.publish(ShardPositionsUpdate {
                source_uid: SourceUid {
                    index_uid: index_uid.clone(),
                    source_id: "test-source".to_string(),
                },
                updated_shard_positions: vec![(shard_id_1.clone(), position.clone())]
                    .into_iter()
                    .collect(),
            });

            tokio::time::timeout(Duration::from_millis(200), tracker.wait_publish_complete())
                .await
                .unwrap_err();
        }
        {
            let event_broker = EventBroker::default();
            let tracker = PublishTracker::new(event_broker.clone());
            tracker.register_requested_shards([&shard_id_1, &shard_id_2]);
            tracker.track_persisted_shard_position(shard_id_1.clone(), position.clone());
            event_broker.publish(ShardPositionsUpdate {
                source_uid: SourceUid {
                    index_uid: index_uid.clone(),
                    source_id: "test-source".to_string(),
                },
                updated_shard_positions: vec![(shard_id_1.clone(), position.clone())]
                    .into_iter()
                    .collect(),
            });
            // sleep to make sure the event is processed
            tokio::time::sleep(Duration::from_millis(50)).await;
            tracker.track_persisted_shard_position(shard_id_2.clone(), position.clone());

            tokio::time::timeout(Duration::from_millis(200), tracker.wait_publish_complete())
                .await
                .unwrap_err();
        }
    }
}
