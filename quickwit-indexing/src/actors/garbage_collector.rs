// Copyright (C) 2021 Quickwit, Inc.
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

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use quickwit_actors::{Actor, ActorContext, AsyncActor};
use quickwit_metastore::{Metastore, SplitMetadataAndFooterOffsets, SplitState};
use quickwit_storage::Storage;
use tantivy::chrono::Utc;
use tracing::{info, warn};

// TODO: discuss & settle on best default for this
// https://aws.amazon.com/premiumsupport/knowledge-center/s3-request-limit-avoid-throttling/
const MAX_CONCURRENT_STORAGE_REQUESTS: usize = 1000;

const RUN_INTERVAL_IN_SECONDS: u64 = 10 * 60; // 10 minutes
const GRACE_PERIOD_IN_SECONDS: u64 = 2 * 60; // 2 minutes

#[derive(Debug, Clone, Default)]
pub struct GarbageCollectorCounters {
    // Denotes the number of passes the garbage collector has performed.
    pub num_passes: u64,
}

/// An actor for collecting garbage periodically from an index.
pub struct GarbageCollector {
    index_id: String,
    index_storage: Arc<dyn Storage>,
    metastore: Arc<dyn Metastore>,
    counters: GarbageCollectorCounters,
}

impl GarbageCollector {
    pub fn new(
        index_id: String,
        index_storage: Arc<dyn Storage>,
        metastore: Arc<dyn Metastore>,
    ) -> Self {
        Self {
            index_id,
            index_storage,
            metastore,
            counters: GarbageCollectorCounters::default(),
        }
    }

    pub async fn run_garbage_collect_operation(&mut self) -> anyhow::Result<()> {
        info!(index = %self.index_id, "garbage-collect-operation");
        self.counters.num_passes += 1;

        // Select staged splits with staging timestamp older than grace period timestamp.
        let grace_period_timestamp = Utc::now().timestamp() - GRACE_PERIOD_IN_SECONDS as i64;
        let staged_splits = self
            .metastore
            .list_splits(&self.index_id, SplitState::Staged, None, &[])
            .await?
            .into_iter()
            // TODO: Update metastore API and push this filter down.
            .filter(|meta| meta.split_metadata.update_timestamp < grace_period_timestamp)
            .collect::<Vec<_>>();

        // Schedule all eligible staged splits for delete
        let split_ids = staged_splits
            .iter()
            .map(|meta| meta.split_metadata.split_id.as_str())
            .collect::<Vec<_>>();
        self.metastore
            .mark_splits_as_deleted(&self.index_id, &split_ids)
            .await?;

        // Select split to deletes
        let splits_to_delete = self
            .metastore
            .list_splits(&self.index_id, SplitState::ScheduledForDeletion, None, &[])
            .await?;

        delete_splits_with_files(
            &self.index_id,
            splits_to_delete,
            self.metastore.clone(),
            self.index_storage.clone(),
        )
        .await
    }
}

impl Actor for GarbageCollector {
    type Message = ();
    type ObservableState = GarbageCollectorCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }
}

#[async_trait]
impl AsyncActor for GarbageCollector {
    async fn initialize(
        &mut self,
        ctx: &ActorContext<Self::Message>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        self.process_message((), ctx).await
    }

    async fn process_message(
        &mut self,
        _: (),
        ctx: &ActorContext<Self::Message>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        self.run_garbage_collect_operation().await?;
        ctx.schedule_self_msg(Duration::from_secs(RUN_INTERVAL_IN_SECONDS), ())
            .await;
        Ok(())
    }
}

/// Delete a list of splits from the storage and the metastore.
/// It should leave the index and the metastore in good state.
///
/// * `index_id` - The target index id.
/// * `splits`  - The list of splits to delete.
/// * `metastore` - The metastore managing the target index.
/// * `storage - The storage managing the target index.
pub async fn delete_splits_with_files(
    index_id: &str,
    splits: Vec<SplitMetadataAndFooterOffsets>,
    metastore: Arc<dyn Metastore>,
    storage: Arc<dyn Storage>,
) -> anyhow::Result<()> {
    let mut deleted_split_ids: Vec<String> = Vec::new();
    let mut failed_to_delete_split_ids: Vec<String> = Vec::new();

    let mut delete_splits_results_stream = tokio_stream::iter(splits.into_iter())
        .map(|meta| {
            let moved_storage = storage.clone();
            async move {
                let file_name = quickwit_common::split_file(&meta.split_metadata.split_id);
                let delete_result = moved_storage.delete(Path::new(&file_name)).await;
                (meta.split_metadata.split_id.clone(), delete_result.is_ok())
            }
        })
        .buffer_unordered(MAX_CONCURRENT_STORAGE_REQUESTS);

    while let Some((split_id, is_success)) = delete_splits_results_stream.next().await {
        if is_success {
            deleted_split_ids.push(split_id);
        } else {
            failed_to_delete_split_ids.push(split_id);
        }
    }

    if !failed_to_delete_split_ids.is_empty() {
        warn!(splits=?failed_to_delete_split_ids, "Some splits were not deleted");
    }

    if !deleted_split_ids.is_empty() {
        let split_ids: Vec<&str> = deleted_split_ids.iter().map(String::as_str).collect();
        metastore.delete_splits(index_id, &split_ids).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use quickwit_actors::Universe;
    use quickwit_metastore::{MockMetastore, SplitMetadata};
    use quickwit_storage::{MockStorage, StorageErrorKind};

    use super::*;

    fn make_split(id: &str) -> SplitMetadataAndFooterOffsets {
        SplitMetadataAndFooterOffsets {
            split_metadata: SplitMetadata {
                split_id: id.to_string(),
                ..Default::default()
            },
            footer_offsets: 2..6,
        }
    }

    #[tokio::test]
    async fn test_garbage_collect_calls_dependencies_appropriately() {
        quickwit_common::setup_logging_for_tests();
        let foo_index = "foo-index";

        let mut mock_storage = MockStorage::default();
        mock_storage.expect_delete().times(3).returning(|path| {
            assert!(
                path == Path::new("a.split")
                    || path == Path::new("b.split")
                    || path == Path::new("c.split")
            );
            if path == Path::new("c.split") {
                return Err(
                    StorageErrorKind::InternalError.with_error(anyhow::anyhow!("Some error"))
                );
            }
            Ok(())
        });

        let mut mock_metastore = MockMetastore::default();
        mock_metastore.expect_list_splits().times(2).returning(
            |index_id, split_state, _time_range, _tags| {
                assert_eq!(index_id, "foo-index");
                let splits = match split_state {
                    SplitState::Staged => vec![make_split("a")],
                    SplitState::ScheduledForDeletion => {
                        vec![make_split("a"), make_split("b"), make_split("c")]
                    }
                    _ => panic!("only Staged and ScheduledForDeletion expected."),
                };
                Ok(splits)
            },
        );
        mock_metastore
            .expect_mark_splits_as_deleted()
            .times(1)
            .returning(|index_id, split_ids| {
                assert_eq!(index_id, "foo-index");
                assert_eq!(split_ids, vec!["a"]);
                Ok(())
            });
        mock_metastore
            .expect_delete_splits()
            .times(1)
            .returning(|index_id, split_ids| {
                assert_eq!(index_id, "foo-index");
                assert_eq!(split_ids, vec!["a", "b"]);
                Ok(())
            });

        let universe = Universe::new();
        let garbage_collect_actor = GarbageCollector::new(
            foo_index.to_string(),
            Arc::new(mock_storage),
            Arc::new(mock_metastore),
        );
        let (_maibox, handler) = universe.spawn_actor(garbage_collect_actor).spawn_async();

        let state_after_initialization = handler.process_pending_and_observe().await.state;
        assert_eq!(state_after_initialization.num_passes, 1);

        // universe.simulate_time_shift(Duration::from_secs(200)).await;
        // let count_after_advance_time = handler.process_pending_and_observe().await.state;
        // Note the count is 2 here and not 1 + 3  = 4.
        // See comment on `universe.simulate_advance_time`.
        // assert_eq!(count_after_advance_time, 4);
    }

    #[tokio::test]
    async fn test_garbage_collect_get_calls_repeatedly() {
        quickwit_common::setup_logging_for_tests();
        let foo_index = "foo-index";

        let mut mock_storage = MockStorage::default();
        mock_storage.expect_delete().times(4).returning(|path| {
            assert!(path == Path::new("a.split") || path == Path::new("b.split"));
            Ok(())
        });

        let mut mock_metastore = MockMetastore::default();
        mock_metastore.expect_list_splits().times(4).returning(
            |index_id, split_state, _time_range, _tags| {
                assert_eq!(index_id, "foo-index");
                let splits = match split_state {
                    SplitState::Staged => vec![make_split("a")],
                    SplitState::ScheduledForDeletion => vec![make_split("a"), make_split("b")],
                    _ => panic!("only Staged and ScheduledForDeletion expected."),
                };
                Ok(splits)
            },
        );
        mock_metastore
            .expect_mark_splits_as_deleted()
            .times(2)
            .returning(|index_id, split_ids| {
                assert_eq!(index_id, "foo-index");
                assert_eq!(split_ids, vec!["a"]);
                Ok(())
            });
        mock_metastore
            .expect_delete_splits()
            .times(2)
            .returning(|index_id, split_ids| {
                assert_eq!(index_id, "foo-index");
                assert_eq!(split_ids, vec!["a", "b"]);
                Ok(())
            });

        let universe = Universe::new();
        let garbage_collect_actor = GarbageCollector::new(
            foo_index.to_string(),
            Arc::new(mock_storage),
            Arc::new(mock_metastore),
        );
        let (_maibox, handler) = universe.spawn_actor(garbage_collect_actor).spawn_async();

        let state_after_initialization = handler.process_pending_and_observe().await.state;
        assert_eq!(state_after_initialization.num_passes, 1);

        // 5 minutes later
        universe
            .simulate_time_shift(Duration::from_secs(5 * 60))
            .await;
        let state_after_initialization = handler.process_pending_and_observe().await.state;
        assert_eq!(state_after_initialization.num_passes, 1);

        // 15 minutes later
        universe
            .simulate_time_shift(Duration::from_secs(RUN_INTERVAL_IN_SECONDS))
            .await;
        let state_after_initialization = handler.process_pending_and_observe().await.state;
        assert_eq!(state_after_initialization.num_passes, 2);
    }
}
