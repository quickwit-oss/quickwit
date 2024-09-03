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

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::{stream, StreamExt};
use quickwit_actors::{Actor, ActorContext, Handler};
use quickwit_common::shared_consts::split_deletion_grace_period;
use quickwit_index_management::run_garbage_collect;
use quickwit_metastore::ListIndexesMetadataResponseExt;
use quickwit_proto::metastore::{
    ListIndexesMetadataRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::types::IndexUid;
use quickwit_storage::{Storage, StorageResolver};
use serde::Serialize;
use tracing::{debug, error, info};

const RUN_INTERVAL: Duration = Duration::from_secs(10 * 60); // 10 minutes

/// Staged files needs to be deleted if there was a failure.
/// TODO ideally we want clean up all staged splits every time we restart the indexing pipeline, but
/// the grace period strategy should do the job for the moment.
const STAGED_GRACE_PERIOD: Duration = Duration::from_secs(60 * 60 * 24); // 24 hours

#[derive(Clone, Debug, Default, Serialize)]
pub struct GarbageCollectorCounters {
    /// The number of passes the garbage collector has performed.
    pub num_passes: usize,
    /// The number of deleted files.
    pub num_deleted_files: usize,
    /// The number of bytes deleted.
    pub num_deleted_bytes: usize,
    /// The number of failed garbage collection run on an index.
    pub num_failed_gc_run_on_index: usize,
    /// The number of successful garbage collection run on an index.
    pub num_successful_gc_run_on_index: usize,
    /// The number or failed storage resolution.
    pub num_failed_storage_resolution: usize,
    /// The number of splits that were unable to be removed.
    pub num_failed_splits: usize,
}

#[derive(Debug)]
struct Loop;

/// An actor for collecting garbage periodically from an index.
pub struct GarbageCollector {
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
    counters: GarbageCollectorCounters,
}

impl GarbageCollector {
    pub fn new(metastore: MetastoreServiceClient, storage_resolver: StorageResolver) -> Self {
        Self {
            metastore,
            storage_resolver,
            counters: GarbageCollectorCounters::default(),
        }
    }

    /// Gc Loop handler logic.
    /// Should not return an error to prevent the actor from crashing.
    async fn handle_inner(&mut self, ctx: &ActorContext<Self>) {
        debug!("loading indexes from the metastore");
        self.counters.num_passes += 1;

        let response = match self
            .metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await
        {
            Ok(response) => response,
            Err(error) => {
                error!(%error, "failed to list indexes from the metastore");
                return;
            }
        };
        let indexes = match response.deserialize_indexes_metadata().await {
            Ok(indexes) => indexes,
            Err(error) => {
                error!(%error, "failed to deserialize indexes metadata");
                return;
            }
        };
        info!("loaded {} indexes from the metastore", indexes.len());

        let expected_count = indexes.len();
        let index_storages: HashMap<IndexUid, Arc<dyn Storage>> = stream::iter(indexes).filter_map(|index| {
            let storage_resolver = self.storage_resolver.clone();
            async move {
                let index_uid = index.index_uid.clone();
                let index_uri = index.index_uri();
                let storage = match storage_resolver.resolve(index_uri).await {
                    Ok(storage) => storage,
                    Err(error) => {
                        error!(index=%index.index_id(), error=?error, "failed to resolve the index storage Uri");
                        return None;
                    }
                };
                Some((index_uid, storage))
            }}).collect()
            .await;
        let got_count = index_storages.len();

        let gc_res = run_garbage_collect(
            index_storages,
            self.metastore.clone(),
            STAGED_GRACE_PERIOD,
            split_deletion_grace_period(),
            false,
            Some(ctx.progress()),
        )
        .await;

        self.counters.num_failed_storage_resolution += expected_count - got_count;

        let deleted_file_entries = match gc_res {
            Ok(removal_info) => {
                self.counters.num_successful_gc_run_on_index += 1;
                self.counters.num_failed_splits += removal_info.failed_splits.len();
                removal_info.removed_split_entries
            }
            Err(error) => {
                self.counters.num_failed_gc_run_on_index += 1;
                error!(error=?error, "failed to run garbage collection");
                return;
            }
        };
        if !deleted_file_entries.is_empty() {
            let num_deleted_splits = deleted_file_entries.len();
            let deleted_files: HashSet<&Path> = deleted_file_entries
                .iter()
                .map(|deleted_entry| deleted_entry.file_name.as_path())
                .take(5)
                .collect();
            info!(
                num_deleted_splits = num_deleted_splits,
                "Janitor deleted {:?} and {} other splits.", deleted_files, num_deleted_splits,
            );
            self.counters.num_deleted_files += deleted_file_entries.len();
            self.counters.num_deleted_bytes += deleted_file_entries
                .iter()
                .map(|entry| entry.file_size_bytes.as_u64() as usize)
                .sum::<usize>();
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
        self.handle(Loop, ctx).await?;
        Ok(())
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
        self.handle_inner(ctx).await;
        ctx.schedule_self_msg(RUN_INTERVAL, Loop);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::path::Path;
    use std::str::FromStr;
    use std::sync::Arc;

    use quickwit_actors::Universe;
    use quickwit_common::shared_consts::split_deletion_grace_period;
    use quickwit_common::ServiceStream;
    use quickwit_metastore::{
        IndexMetadata, ListSplitsRequestExt, ListSplitsResponseExt, Split, SplitMetadata,
        SplitState,
    };
    use quickwit_proto::metastore::{
        EmptyResponse, ListIndexesMetadataResponse, ListSplitsResponse, MetastoreError,
        MockMetastoreService,
    };
    use quickwit_proto::types::IndexUid;
    use quickwit_storage::MockStorage;
    use time::OffsetDateTime;

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
                publish_timestamp: None,
            })
            .collect()
    }

    #[tokio::test]
    async fn test_run_garbage_collect_calls_dependencies_appropriately() {
        let index_uid = IndexUid::from_str("test-index:11111111111111111111111111").unwrap();
        let mut mock_storage = MockStorage::default();
        mock_storage
            .expect_bulk_delete()
            .times(1)
            .returning(|paths: &[&Path]| {
                let actual: HashSet<&Path> = HashSet::from_iter(paths.iter().copied());
                let expected: HashSet<&Path> = HashSet::from_iter([
                    Path::new("a.split"),
                    Path::new("b.split"),
                    Path::new("c.split"),
                ]);

                assert_eq!(actual, expected);

                Ok(())
            });

        let mut mock_metastore = MockMetastoreService::new();
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_list_splits()
            .times(2)
            .returning(move |list_splits_request| {
                let query = list_splits_request.deserialize_list_splits_query().unwrap();
                assert_eq!(query.index_uids[0], index_uid_clone,);
                let splits = match query.split_states[0] {
                    SplitState::Staged => make_splits(&["a"], SplitState::Staged),
                    SplitState::MarkedForDeletion => {
                        let expected_deletion_timestamp = OffsetDateTime::now_utc()
                            .unix_timestamp()
                            - split_deletion_grace_period().as_secs() as i64;
                        assert_eq!(
                            query.update_timestamp.end,
                            Bound::Included(expected_deletion_timestamp),
                            "Expected splits query to only select splits which have not been \
                             updated since the expected deletion timestamp.",
                        );
                        assert_eq!(
                            query.update_timestamp.start,
                            Bound::Unbounded,
                            "Expected the lower bound to be unbounded when filtering splits.",
                        );

                        make_splits(&["a", "b", "c"], SplitState::MarkedForDeletion)
                    }
                    _ => panic!("only Staged and MarkedForDeletion expected."),
                };
                let splits = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits)]))
            });
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_mark_splits_for_deletion()
            .times(1)
            .returning(move |mark_splits_for_deletion_request| {
                assert_eq!(
                    mark_splits_for_deletion_request.index_uid(),
                    &index_uid_clone
                );
                assert_eq!(mark_splits_for_deletion_request.split_ids, vec!["a"]);
                Ok(EmptyResponse {})
            });
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_delete_splits()
            .times(1)
            .returning(move |delete_splits_request| {
                assert_eq!(delete_splits_request.index_uid(), &index_uid_clone);
                let split_ids = HashSet::<&str>::from_iter(
                    delete_splits_request
                        .split_ids
                        .iter()
                        .map(|split_id| split_id.as_str()),
                );
                let expected_split_ids = HashSet::<&str>::from_iter(["a", "b", "c"]);
                assert_eq!(split_ids, expected_split_ids);

                Ok(EmptyResponse {})
            });

        let result = run_garbage_collect(
            index_uid,
            Arc::new(mock_storage),
            MetastoreServiceClient::from_mock(mock_metastore),
            STAGED_GRACE_PERIOD,
            split_deletion_grace_period(),
            false,
            None,
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_garbage_collect_calls_dependencies_appropriately() {
        let storage_resolver = StorageResolver::unconfigured();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_list_indexes_metadata()
            .times(1)
            .returning(|_list_indexes_request| {
                let indexes_metadata = vec![IndexMetadata::for_test(
                    "test-index",
                    "ram://indexes/test-index",
                )];
                Ok(ListIndexesMetadataResponse::for_test(indexes_metadata))
            });
        mock_metastore
            .expect_list_splits()
            .times(2)
            .returning(|list_splits_request| {
                let query = list_splits_request.deserialize_list_splits_query().unwrap();
                assert_eq!(&query.index_uids[0].index_id, "test-index");
                let splits = match query.split_states[0] {
                    SplitState::Staged => make_splits(&["a"], SplitState::Staged),
                    SplitState::MarkedForDeletion => {
                        make_splits(&["a", "b", "c"], SplitState::MarkedForDeletion)
                    }
                    _ => panic!("only Staged and MarkedForDeletion expected."),
                };
                let splits = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits)]))
            });
        mock_metastore
            .expect_mark_splits_for_deletion()
            .times(1)
            .returning(|mark_splits_for_deletion_request| {
                let index_uid: IndexUid = mark_splits_for_deletion_request.index_uid().clone();
                assert_eq!(&index_uid.index_id, "test-index");
                assert_eq!(mark_splits_for_deletion_request.split_ids, vec!["a"]);
                Ok(EmptyResponse {})
            });
        mock_metastore
            .expect_delete_splits()
            .times(1)
            .returning(|delete_splits_request| {
                let index_uid: IndexUid = delete_splits_request.index_uid().clone();
                assert_eq!(&index_uid.index_id, "test-index");

                let split_ids = HashSet::<&str>::from_iter(
                    delete_splits_request
                        .split_ids
                        .iter()
                        .map(|split_id| split_id.as_str()),
                );
                let expected_split_ids = HashSet::<&str>::from_iter(["a", "b", "c"]);

                assert_eq!(split_ids, expected_split_ids);
                Ok(EmptyResponse {})
            });

        let garbage_collect_actor = GarbageCollector::new(
            MetastoreServiceClient::from_mock(mock_metastore),
            storage_resolver,
        );
        let universe = Universe::with_accelerated_time();
        let (_mailbox, handler) = universe.spawn_builder().spawn(garbage_collect_actor);

        let state_after_initialization = handler.process_pending_and_observe().await.state;
        assert_eq!(state_after_initialization.num_passes, 1);
        assert_eq!(state_after_initialization.num_deleted_files, 3);
        assert_eq!(state_after_initialization.num_deleted_bytes, 60);
        assert_eq!(state_after_initialization.num_failed_splits, 0);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_garbage_collect_get_calls_repeatedly() {
        let storage_resolver = StorageResolver::unconfigured();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_list_indexes_metadata()
            .times(3)
            .returning(|_list_indexes_metadata| {
                let indexes_metadata = vec![IndexMetadata::for_test(
                    "test-index",
                    "ram://indexes/test-index",
                )];
                Ok(ListIndexesMetadataResponse::for_test(indexes_metadata))
            });
        mock_metastore
            .expect_list_splits()
            .times(6)
            .returning(|list_splits_request| {
                let query = list_splits_request.deserialize_list_splits_query().unwrap();
                assert_eq!(&query.index_uids[0].index_id, "test-index");
                let splits = match query.split_states[0] {
                    SplitState::Staged => make_splits(&["a"], SplitState::Staged),
                    SplitState::MarkedForDeletion => {
                        make_splits(&["a", "b"], SplitState::MarkedForDeletion)
                    }
                    _ => panic!("only Staged and MarkedForDeletion expected."),
                };
                let splits = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits)]))
            });
        mock_metastore
            .expect_mark_splits_for_deletion()
            .times(3)
            .returning(|mark_splits_for_deletion_request| {
                let index_uid: IndexUid = mark_splits_for_deletion_request.index_uid().clone();
                assert_eq!(&index_uid.index_id, "test-index");
                assert_eq!(mark_splits_for_deletion_request.split_ids, vec!["a"]);
                Ok(EmptyResponse {})
            });
        mock_metastore
            .expect_delete_splits()
            .times(3)
            .returning(|delete_splits_request| {
                let index_uid: IndexUid = delete_splits_request.index_uid().clone();
                assert_eq!(&index_uid.index_id, "test-index");

                let split_ids = HashSet::<&str>::from_iter(
                    delete_splits_request
                        .split_ids
                        .iter()
                        .map(|split_id| split_id.as_str()),
                );
                let expected_split_ids = HashSet::<&str>::from_iter(["a", "b"]);

                assert_eq!(split_ids, expected_split_ids);
                Ok(EmptyResponse {})
            });

        let garbage_collect_actor = GarbageCollector::new(
            MetastoreServiceClient::from_mock(mock_metastore),
            storage_resolver,
        );
        let universe = Universe::with_accelerated_time();
        let (_mailbox, handle) = universe.spawn_builder().spawn(garbage_collect_actor);

        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_passes, 1);
        assert_eq!(counters.num_deleted_files, 2);
        assert_eq!(counters.num_deleted_bytes, 40);
        assert_eq!(counters.num_successful_gc_run_on_index, 1);
        assert_eq!(counters.num_failed_storage_resolution, 0);
        assert_eq!(counters.num_failed_gc_run_on_index, 0);
        assert_eq!(counters.num_failed_splits, 0);

        // 30 secs later
        universe.sleep(Duration::from_secs(30)).await;
        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_passes, 1);
        assert_eq!(counters.num_deleted_files, 2);
        assert_eq!(counters.num_deleted_bytes, 40);
        assert_eq!(counters.num_successful_gc_run_on_index, 1);
        assert_eq!(counters.num_failed_storage_resolution, 0);
        assert_eq!(counters.num_failed_gc_run_on_index, 0);
        assert_eq!(counters.num_failed_splits, 0);

        // 60 secs later
        universe.sleep(RUN_INTERVAL).await;
        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_passes, 2);
        assert_eq!(counters.num_deleted_files, 4);
        assert_eq!(counters.num_deleted_bytes, 80);
        assert_eq!(counters.num_successful_gc_run_on_index, 2);
        assert_eq!(counters.num_failed_storage_resolution, 0);
        assert_eq!(counters.num_failed_gc_run_on_index, 0);
        assert_eq!(counters.num_failed_splits, 0);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_garbage_collect_get_called_repeatedly_on_failure() {
        let storage_resolver = StorageResolver::unconfigured();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_list_indexes_metadata()
            .times(4)
            .returning(|_list_indexes_request| {
                Err(MetastoreError::Db {
                    message: "fail to list indexes".to_string(),
                })
            });

        let garbage_collect_actor = GarbageCollector::new(
            MetastoreServiceClient::from_mock(mock_metastore),
            storage_resolver,
        );
        let universe = Universe::with_accelerated_time();
        let (_mailbox, handle) = universe.spawn_builder().spawn(garbage_collect_actor);

        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_passes, 1);

        universe.sleep(RUN_INTERVAL).await;
        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_passes, 2);

        universe.sleep(RUN_INTERVAL).await;
        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_passes, 3);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_garbage_collect_fails_to_resolve_storage() {
        let storage_resolver = StorageResolver::unconfigured();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_list_indexes_metadata()
            .times(1)
            .returning(move |_list_indexes_request| {
                let indexes_metadata = vec![IndexMetadata::for_test(
                    "test-index",
                    "postgresql://indexes/test-index",
                )];
                Ok(ListIndexesMetadataResponse::for_test(indexes_metadata))
            });

        let garbage_collect_actor = GarbageCollector::new(
            MetastoreServiceClient::from_mock(mock_metastore),
            storage_resolver,
        );
        let universe = Universe::with_accelerated_time();
        let (_mailbox, handle) = universe.spawn_builder().spawn(garbage_collect_actor);

        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_passes, 1);
        assert_eq!(counters.num_deleted_files, 0);
        assert_eq!(counters.num_deleted_bytes, 0);
        assert_eq!(counters.num_successful_gc_run_on_index, 0);
        assert_eq!(counters.num_failed_storage_resolution, 1);
        assert_eq!(counters.num_failed_gc_run_on_index, 0);
        assert_eq!(counters.num_failed_splits, 0);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_garbage_collect_fails_to_run_gc_on_one_index() {
        let storage_resolver = StorageResolver::unconfigured();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_list_indexes_metadata()
            .times(1)
            .returning(|_list_indexes_request| {
                let indexes_metadata = vec![
                    IndexMetadata::for_test("test-index-1", "ram:///indexes/test-index-1"),
                    IndexMetadata::for_test("test-index-2", "ram:///indexes/test-index-2"),
                ];
                Ok(ListIndexesMetadataResponse::for_test(indexes_metadata))
            });
        mock_metastore
            .expect_list_splits()
            .times(3)
            .returning(|list_splits_request| {
                let query = list_splits_request.deserialize_list_splits_query().unwrap();
                assert!(["test-index-1", "test-index-2"]
                    .contains(&query.index_uids[0].index_id.as_ref()));

                if query.index_uids[0].index_id == "test-index-2" {
                    return Err(MetastoreError::Db {
                        message: "fail to delete".to_string(),
                    });
                }
                let splits = match query.split_states[0] {
                    SplitState::Staged => make_splits(&["a"], SplitState::Staged),
                    SplitState::MarkedForDeletion => {
                        make_splits(&["a", "b"], SplitState::MarkedForDeletion)
                    }
                    _ => panic!("only Staged and MarkedForDeletion expected."),
                };
                let splits = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits)]))
            });
        mock_metastore
            .expect_mark_splits_for_deletion()
            .once()
            .returning(|mark_splits_for_deletion_request| {
                let index_uid: IndexUid = mark_splits_for_deletion_request.index_uid().clone();
                assert!(["test-index-1", "test-index-2"].contains(&index_uid.index_id.as_ref()));
                assert_eq!(mark_splits_for_deletion_request.split_ids, vec!["a"]);
                Ok(EmptyResponse {})
            });
        mock_metastore
            .expect_delete_splits()
            .once()
            .returning(|delete_splits_request| {
                let split_ids = HashSet::<&str>::from_iter(
                    delete_splits_request
                        .split_ids
                        .iter()
                        .map(|split_id| split_id.as_str()),
                );
                let expected_split_ids = HashSet::<&str>::from_iter(["a", "b"]);

                assert_eq!(split_ids, expected_split_ids);
                Ok(EmptyResponse {})
            });

        let garbage_collect_actor = GarbageCollector::new(
            MetastoreServiceClient::from_mock(mock_metastore),
            storage_resolver,
        );
        let universe = Universe::with_accelerated_time();
        let (_mailbox, handle) = universe.spawn_builder().spawn(garbage_collect_actor);

        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_passes, 1);
        assert_eq!(counters.num_deleted_files, 2);
        assert_eq!(counters.num_deleted_bytes, 40);
        assert_eq!(counters.num_successful_gc_run_on_index, 1);
        assert_eq!(counters.num_failed_storage_resolution, 0);
        assert_eq!(counters.num_failed_gc_run_on_index, 1);
        assert_eq!(counters.num_failed_splits, 0);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_garbage_collect_fails_to_run_delete_on_one_index() {
        let storage_resolver = StorageResolver::unconfigured();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_list_indexes_metadata()
            .times(1)
            .returning(|_list_indexes_request| {
                let indexes_metadata = vec![
                    IndexMetadata::for_test("test-index-1", "ram://indexes/test-index-1"),
                    IndexMetadata::for_test("test-index-2", "ram://indexes/test-index-2"),
                ];
                Ok(ListIndexesMetadataResponse::for_test(indexes_metadata))
            });
        mock_metastore
            .expect_list_splits()
            .times(4)
            .returning(|list_splits_request| {
                let query = list_splits_request.deserialize_list_splits_query().unwrap();
                assert!(["test-index-1", "test-index-2"]
                    .contains(&query.index_uids[0].index_id.as_ref()));
                let splits = match query.split_states[0] {
                    SplitState::Staged => make_splits(&["a"], SplitState::Staged),
                    SplitState::MarkedForDeletion => {
                        make_splits(&["a", "b"], SplitState::MarkedForDeletion)
                    }
                    _ => panic!("only Staged and MarkedForDeletion expected."),
                };
                let splits = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits)]))
            });
        mock_metastore
            .expect_mark_splits_for_deletion()
            .times(2)
            .returning(|mark_splits_for_deletion_request| {
                let index_uid: IndexUid = mark_splits_for_deletion_request.index_uid().clone();
                assert!(["test-index-1", "test-index-2"].contains(&index_uid.index_id.as_ref()));
                assert_eq!(mark_splits_for_deletion_request.split_ids, vec!["a"]);
                Ok(EmptyResponse {})
            });
        mock_metastore
            .expect_delete_splits()
            .times(2)
            .returning(|delete_splits_request| {
                let index_uid: IndexUid = delete_splits_request.index_uid().clone();
                let split_ids = HashSet::<&str>::from_iter(
                    delete_splits_request
                        .split_ids
                        .iter()
                        .map(|split_id| split_id.as_str()),
                );
                let expected_split_ids = HashSet::<&str>::from_iter(["a", "b"]);

                assert_eq!(split_ids, expected_split_ids);

                // This should not cause the whole run to fail and return an error,
                // instead this should simply get logged and return the list of splits
                // which have successfully been deleted.
                if index_uid.index_id == "test-index-2" {
                    Err(MetastoreError::Db {
                        message: "fail to delete".to_string(),
                    })
                } else {
                    Ok(EmptyResponse {})
                }
            });

        let garbage_collect_actor = GarbageCollector::new(
            MetastoreServiceClient::from_mock(mock_metastore),
            storage_resolver,
        );
        let universe = Universe::with_accelerated_time();
        let (_mailbox, handle) = universe.spawn_builder().spawn(garbage_collect_actor);

        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_passes, 1);
        assert_eq!(counters.num_deleted_files, 2);
        assert_eq!(counters.num_deleted_bytes, 40);
        assert_eq!(counters.num_successful_gc_run_on_index, 2);
        assert_eq!(counters.num_failed_storage_resolution, 0);
        assert_eq!(counters.num_failed_gc_run_on_index, 0);
        assert_eq!(counters.num_failed_splits, 2);
        universe.assert_quit().await;
    }
}
