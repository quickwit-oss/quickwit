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

use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use tracing::info;

use super::state::WeakIngesterState;
use crate::with_lock_metrics;

const RUN_INTERVAL_PERIOD: Duration = if cfg!(test) {
    Duration::from_millis(50)
} else {
    Duration::from_secs(60)
};

/// Periodically closes idle shards.
pub(super) struct CloseIdleShardsTask {
    weak_state: WeakIngesterState,
    idle_shard_timeout: Duration,
}

impl CloseIdleShardsTask {
    pub fn spawn(weak_state: WeakIngesterState, idle_shard_timeout: Duration) -> JoinHandle<()> {
        let task = Self {
            weak_state,
            idle_shard_timeout,
        };
        tokio::spawn(async move {
            let Some(mut state) = task.weak_state.upgrade() else {
                return;
            };
            state.wait_for_ready().await;
            drop(state);

            task.run().await
        })
    }

    async fn run(&self) {
        let mut interval = tokio::time::interval(RUN_INTERVAL_PERIOD);

        loop {
            interval.tick().await;

            let Some(state) = self.weak_state.upgrade() else {
                return;
            };
            let Ok(mut state_guard) =
                with_lock_metrics!(state.lock_partially(), "close_idle_shards", "write").await
            else {
                return;
            };

            let now = Instant::now();

            for (queue_id, shard) in &mut state_guard.shards {
                if shard.is_open() && shard.is_idle(now, self.idle_shard_timeout) {
                    shard.close();
                    info!("closed idle shard `{queue_id}`");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::ingest::ShardState;
    use quickwit_proto::types::{IndexUid, Position, ShardId, queue_id};

    use super::*;
    use crate::ingest_v2::models::IngesterShard;
    use crate::ingest_v2::state::IngesterState;

    #[tokio::test]
    async fn test_close_idle_shards_run() {
        let (_temp_dir, state) = IngesterState::for_test().await;
        let weak_state = state.weak();
        let idle_shard_timeout = Duration::from_millis(200);
        let join_handle = CloseIdleShardsTask::spawn(weak_state, idle_shard_timeout);

        let mut state_guard = state.lock_partially().await.unwrap();
        let now = Instant::now();

        let index_uid = IndexUid::for_test("test-index", 0);
        let shard_01 = IngesterShard::new_solo(
            ShardState::Open,
            Position::Beginning,
            Position::Beginning,
            None,
            now - idle_shard_timeout,
            false,
        );
        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));
        state_guard.shards.insert(queue_id_01.clone(), shard_01);

        let shard_02 = IngesterShard::new_solo(
            ShardState::Open,
            Position::Beginning,
            Position::Beginning,
            None,
            now - idle_shard_timeout / 2,
            false,
        );
        let queue_id_02 = queue_id(&index_uid, "test-source", &ShardId::from(2));
        state_guard.shards.insert(queue_id_02.clone(), shard_02);
        drop(state_guard);

        tokio::time::sleep(RUN_INTERVAL_PERIOD * 2).await;

        let state_guard = state.lock_partially().await.unwrap();
        state_guard
            .shards
            .get(&queue_id_01)
            .unwrap()
            .assert_is_closed();
        state_guard
            .shards
            .get(&queue_id_02)
            .unwrap()
            .assert_is_open();
        drop(state_guard);

        tokio::time::sleep(idle_shard_timeout).await;

        let state_guard = state.lock_partially().await.unwrap();
        state_guard
            .shards
            .get(&queue_id_02)
            .unwrap()
            .assert_is_closed();
        drop(state_guard);
        drop(state);

        tokio::time::timeout(Duration::from_secs(1), join_handle)
            .await
            .unwrap()
            .unwrap();
    }
}
