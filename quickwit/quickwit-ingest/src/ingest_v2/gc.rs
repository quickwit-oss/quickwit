// Copyright (C) 2023 Quickwit, Inc.
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

//! This module holds the logic for removing shards after successful indexing. The
//! ingesters are in charge of monitoring the state of the shards and removing any shard that mets
//! the following criteria:
//! - the shards is closed
//! - its publish position is greater or equal than its replication position
//! - the removal grace period has elapsed
//!
//! Removing a shard consists in:
//! 1. deleting the shard from the metastore
//! 2. removing the shard from the in-memory data structures
//! 3. deleting the associated mrecordlog queue
//!
//! Shard removal should not be confused with shard truncation. Shard truncation is the process of
//! deleting of sets of records from a shard's queue after they have been successfully indexed.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use mrecordlog::error::DeleteQueueError;
use mrecordlog::MultiRecordLog;
use once_cell::sync::Lazy;
use quickwit_proto::metastore::{DeleteShardsRequest, DeleteShardsSubrequest};
use quickwit_proto::{split_queue_id, IndexUid, QueueId, ShardId, SourceId};
use tokio::sync::{RwLock, Semaphore};
use tracing::{error, info};

use super::ingest_metastore::IngestMetastore;
use super::ingester::IngesterState;

/// Period of time after which shards are actually deleted.
pub(super) const REMOVAL_GRACE_PERIOD: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(5)
} else {
    Duration::from_secs(60 * 10) // 10 minutes
};

/// Maximum number of concurrent removal tasks.
const MAX_CONCURRENCY: usize = 3;

/// Limits the number of concurrent removal tasks.
static MAX_CONCURRENCY_SEMAPHORE: Lazy<Arc<Semaphore>> =
    Lazy::new(|| Arc::new(Semaphore::new(MAX_CONCURRENCY)));

/// Deletes the shards asynchronously after the grace period has elapsed.
pub(super) fn remove_shards_after(
    queues_to_remove: Vec<QueueId>,
    grace_period: Duration,
    metastore: Arc<dyn IngestMetastore>,
    mrecordlog: Arc<RwLock<MultiRecordLog>>,
    state: Arc<RwLock<IngesterState>>,
) {
    if queues_to_remove.is_empty() {
        return;
    }
    let remove_shards_fut = async move {
        tokio::time::sleep(grace_period).await;

        let _permit = MAX_CONCURRENCY_SEMAPHORE
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore should be open");

        let mut per_source_shard_ids: HashMap<(IndexUid, SourceId), Vec<ShardId>> = HashMap::new();

        for queue_id in &queues_to_remove {
            let (index_uid, source_id, shard_id) =
                split_queue_id(queue_id).expect("queue ID should be well-formed");
            per_source_shard_ids
                .entry((index_uid, source_id))
                .or_default()
                .push(shard_id);
        }
        let delete_shards_subrequests = per_source_shard_ids
            .into_iter()
            .map(
                |((index_uid, source_id), shard_ids)| DeleteShardsSubrequest {
                    index_uid: index_uid.into(),
                    source_id,
                    shard_ids,
                },
            )
            .collect();
        let delete_shards_request = DeleteShardsRequest {
            subrequests: delete_shards_subrequests,
            force: false,
        };
        if let Err(error) = metastore.delete_shards(delete_shards_request).await {
            error!("failed to delete shards: `{}`", error);
            return;
        }
        let mut state_guard = state.write().await;

        for queue_id in &queues_to_remove {
            if state_guard.primary_shards.remove(queue_id).is_none() {
                state_guard.replica_shards.remove(queue_id);
            }
        }
        drop(state_guard);

        let mut mrecordlog_guard = mrecordlog.write().await;

        for queue_id in &queues_to_remove {
            if let Err(DeleteQueueError::IoError(error)) =
                mrecordlog_guard.delete_queue(queue_id).await
            {
                error!("failed to delete mrecordlog queue: `{}`", error);
            }
        }
        info!("deleted {} shard(s)", queues_to_remove.len());
    };
    tokio::spawn(remove_shards_fut);
}

#[cfg(test)]
mod tests {
    use quickwit_proto::ingest::ShardState;
    use quickwit_proto::metastore::DeleteShardsResponse;
    use quickwit_proto::queue_id;

    use super::*;
    use crate::ingest_v2::ingest_metastore::MockIngestMetastore;
    use crate::ingest_v2::models::{PrimaryShard, ReplicaShard};

    #[tokio::test]
    async fn test_remove_shards() {
        let mut mock_metastore = MockIngestMetastore::default();
        mock_metastore
            .expect_delete_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.subrequests.len(), 1);

                assert_eq!(request.subrequests[0].index_uid, "test-index:0");
                assert_eq!(request.subrequests[0].source_id, "test-source");
                assert_eq!(request.subrequests[0].shard_ids, [0, 1]);

                let response = DeleteShardsResponse {};
                Ok(response)
            });
        let metastore = Arc::new(mock_metastore);

        let queue_id_0 = queue_id("test-index:0", "test-source", 0);
        let queue_id_1 = queue_id("test-index:0", "test-source", 1);

        let tempdir = tempfile::tempdir().unwrap();
        let mut mrecordlog = MultiRecordLog::open(tempdir.path()).await.unwrap();

        for queue_id in [&queue_id_0, &queue_id_1] {
            mrecordlog.create_queue(queue_id).await.unwrap();
        }
        let mrecordlog = Arc::new(RwLock::new(mrecordlog));

        let mut state = IngesterState {
            primary_shards: HashMap::new(),
            replica_shards: HashMap::new(),
            replication_clients: HashMap::new(),
            replication_tasks: HashMap::new(),
        };
        let primary_shard_0 = PrimaryShard::for_test(
            Some("test-ingester-1"),
            ShardState::Closed,
            12,
            12,
            Some(12),
        );
        state
            .primary_shards
            .insert(queue_id_0.clone(), primary_shard_0);

        let replica_shard_1 = ReplicaShard::for_test("test-ingester-1", ShardState::Closed, 42, 42);
        state
            .replica_shards
            .insert(queue_id_1.clone(), replica_shard_1);

        let state = Arc::new(RwLock::new(state));

        remove_shards_after(
            vec![queue_id_0, queue_id_1],
            REMOVAL_GRACE_PERIOD,
            metastore,
            mrecordlog.clone(),
            state.clone(),
        );
        // Wait for the removal task to run.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let state_guard = state.read().await;
        assert!(state_guard.primary_shards.is_empty());
        assert!(state_guard.replica_shards.is_empty());

        let mrecordlog_guard = mrecordlog.read().await;
        assert!(mrecordlog_guard
            .list_queues()
            .collect::<Vec<_>>()
            .is_empty());
    }
}
