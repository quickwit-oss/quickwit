// Copyright (C) 2022 Quickwit, Inc.
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

use std::collections::HashSet;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, Handler};
use quickwit_control_plane::MetastoreService;
use tracing::info;

use crate::garbage_collection::run_garbage_collect;
use crate::split_store::IndexingSplitStore;

const RUN_INTERVAL: Duration = Duration::from_secs(60); // 1 minutes
/// Staged files needs to be deleted if there was a failure.
/// TODO ideally we want clean up all staged splits every time we restart the indexing pipeline, but
/// the grace period strategy should do the job for the moment.
const STAGED_GRACE_PERIOD: Duration = Duration::from_secs(60 * 60 * 24); // 24 hours
/// We cannot safely delete splits right away as a in-flight queries could actually
/// have selected this split.
/// We deal this probably by introducing a grace period. A split is first marked as delete,
/// and hence won't be selected for search. After a few minutes, once it reasonably safe to assume
/// that all queries involving this split have terminated, we effectively delete the split.
/// This duration is controlled by `DELETION_GRACE_PERIOD`.
const DELETION_GRACE_PERIOD: Duration = Duration::from_secs(120); // 2 min

#[derive(Clone, Debug, Default)]
pub struct GarbageCollectorCounters {
    /// The number of passes the garbage collector has performed.
    pub num_passes: usize,
    /// The number of deleted files.
    pub num_deleted_files: usize,
    /// The number of bytes deleted.
    pub num_deleted_bytes: usize,
}

#[derive(Debug)]
struct Loop;

/// An actor for collecting garbage periodically from an index.
pub struct GarbageCollector {
    index_id: String,
    split_store: IndexingSplitStore,
    metastore_service: MetastoreService,
    counters: GarbageCollectorCounters,
}

impl GarbageCollector {
    pub fn new(
        index_id: String,
        split_store: IndexingSplitStore,
        metastore_service: MetastoreService,
    ) -> Self {
        Self {
            index_id,
            split_store,
            metastore_service,
            counters: GarbageCollectorCounters::default(),
        }
    }
}

#[async_trait]
impl Actor for GarbageCollector {
    type ObservableState = GarbageCollectorCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn name(&self) -> String {
        "GarbageCollector".to_string()
    }

    async fn initialize(
        &mut self,
        ctx: &ActorContext<Self>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        self.handle(Loop, ctx).await
    }
}

#[async_trait]
impl Handler<Loop> for GarbageCollector {
    type Reply = ();

    async fn handle(
        &mut self,
        _: Loop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        info!("garbage-collect-operation");
        self.counters.num_passes += 1;

        let deleted_file_entries = run_garbage_collect(
            &self.index_id,
            self.split_store.clone(),
            self.metastore_service.clone(),
            STAGED_GRACE_PERIOD,
            DELETION_GRACE_PERIOD,
            false,
            Some(ctx),
        )
        .await?;

        if !deleted_file_entries.is_empty() {
            let deleted_files: HashSet<&str> = deleted_file_entries
                .iter()
                .map(|deleted_entry| deleted_entry.file_name.as_str())
                .collect();
            info!(deleted_files=?deleted_files, "gc-delete");

            self.counters.num_deleted_files += deleted_file_entries.len();
            self.counters.num_deleted_bytes += deleted_file_entries
                .iter()
                .map(|entry| entry.file_size_in_bytes as usize)
                .sum::<usize>();
        }

        ctx.schedule_self_msg(RUN_INTERVAL, Loop).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use quickwit_actors::Universe;
    use quickwit_metastore::{MockMetastore, Split, SplitMetadata, SplitState};
    use quickwit_storage::MockStorage;

    use super::*;

    fn make_splits(split_ids: &[&str], split_state: SplitState) -> Vec<Split> {
        split_ids
            .iter()
            .map(|split_id| Split {
                split_metadata: SplitMetadata {
                    split_id: split_id.to_string(),
                    footer_offsets: 5..20,
                    ..Default::default()
                },
                split_state,
                update_timestamp: 0i64,
            })
            .collect()
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
            Ok(())
        });

        let mut mock_metastore = MockMetastore::default();
        mock_metastore.expect_list_splits().times(2).returning(
            |index_id, split_state, _time_range, _tags| {
                assert_eq!(index_id, "foo-index");
                let splits = match split_state {
                    SplitState::Staged => make_splits(&["a"], SplitState::Staged),
                    SplitState::MarkedForDeletion => {
                        make_splits(&["a", "b", "c"], SplitState::MarkedForDeletion)
                    }
                    _ => panic!("only Staged and MarkedForDeletion expected."),
                };
                Ok(splits)
            },
        );
        mock_metastore
            .expect_mark_splits_for_deletion()
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
                assert_eq!(split_ids, vec!["a", "b", "c"]);
                Ok(())
            });

        let universe = Universe::new();
        let metastore_service = MetastoreService::from_metastore(Arc::new(mock_metastore));
        let garbage_collect_actor = GarbageCollector::new(
            foo_index.to_string(),
            IndexingSplitStore::create_with_no_local_store(Arc::new(mock_storage)),
            metastore_service,
        );
        let (_maibox, handler) = universe.spawn_actor(garbage_collect_actor).spawn();

        let state_after_initialization = handler.process_pending_and_observe().await.state;
        assert_eq!(state_after_initialization.num_passes, 1);
        assert_eq!(state_after_initialization.num_deleted_files, 3);
        assert_eq!(state_after_initialization.num_deleted_bytes, 60);
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
                    SplitState::Staged => make_splits(&["a"], SplitState::Staged),
                    SplitState::MarkedForDeletion => {
                        make_splits(&["a", "b"], SplitState::MarkedForDeletion)
                    }
                    _ => panic!("only Staged and MarkedForDeletion expected."),
                };
                Ok(splits)
            },
        );
        mock_metastore
            .expect_mark_splits_for_deletion()
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
        let metastore_service = MetastoreService::from_metastore(Arc::new(mock_metastore));
        let garbage_collect_actor = GarbageCollector::new(
            foo_index.to_string(),
            IndexingSplitStore::create_with_no_local_store(Arc::new(mock_storage)),
            metastore_service,
        );
        let (_maibox, handler) = universe.spawn_actor(garbage_collect_actor).spawn();

        let state_after_initialization = handler.process_pending_and_observe().await.state;
        assert_eq!(state_after_initialization.num_passes, 1);
        assert_eq!(state_after_initialization.num_deleted_files, 2);
        assert_eq!(state_after_initialization.num_deleted_bytes, 40);

        // 30 secs later
        universe.simulate_time_shift(Duration::from_secs(30)).await;
        let state_after_initialization = handler.process_pending_and_observe().await.state;
        assert_eq!(state_after_initialization.num_passes, 1);
        assert_eq!(state_after_initialization.num_deleted_files, 2);
        assert_eq!(state_after_initialization.num_deleted_bytes, 40);

        // 60 secs later
        universe.simulate_time_shift(RUN_INTERVAL).await;
        let state_after_initialization = handler.process_pending_and_observe().await.state;
        assert_eq!(state_after_initialization.num_passes, 2);
        assert_eq!(state_after_initialization.num_deleted_files, 4);
        assert_eq!(state_after_initialization.num_deleted_bytes, 80);
    }
}
