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

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::{StreamExt, stream};
use quickwit_actors::{Actor, ActorContext, Handler};
use quickwit_common::shared_consts::split_deletion_grace_period;
use quickwit_index_management::{GcMetrics, run_garbage_collect};
use quickwit_metastore::ListIndexesMetadataResponseExt;
use quickwit_proto::metastore::{
    ListIndexesMetadataRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::types::IndexUid;
use quickwit_storage::{Storage, StorageResolver};
use serde::Serialize;
use tracing::{debug, error, info};

use crate::metrics::JANITOR_METRICS;

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
    /// The number of failed garbage collection run.
    pub num_failed_gc_run: usize,
    /// The number of successful garbage collection run.
    pub num_successful_gc_run: usize,
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

        let start = Instant::now();

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

        let storage_got_count = index_storages.len();
        self.counters.num_failed_storage_resolution += expected_count - storage_got_count;

        if index_storages.is_empty() {
            return;
        }

        let gc_res = run_garbage_collect(
            index_storages,
            self.metastore.clone(),
            STAGED_GRACE_PERIOD,
            split_deletion_grace_period(),
            false,
            Some(ctx.progress()),
            Some(GcMetrics {
                deleted_splits: JANITOR_METRICS
                    .gc_deleted_splits
                    .with_label_values(["success"])
                    .clone(),
                deleted_bytes: JANITOR_METRICS.gc_deleted_bytes.clone(),
                failed_splits: JANITOR_METRICS
                    .gc_deleted_splits
                    .with_label_values(["error"])
                    .clone(),
            }),
        )
        .await;

        let run_duration = start.elapsed().as_secs();
        JANITOR_METRICS.gc_seconds_total.inc_by(run_duration);

        let deleted_file_entries = match gc_res {
            Ok(removal_info) => {
                self.counters.num_successful_gc_run += 1;
                JANITOR_METRICS.gc_runs.with_label_values(["success"]).inc();
                self.counters.num_failed_splits += removal_info.failed_splits.len();
                removal_info.removed_split_entries
            }
            Err(error) => {
                self.counters.num_failed_gc_run += 1;
                JANITOR_METRICS.gc_runs.with_label_values(["error"]).inc();
                error!(error=?error, "failed to run garbage collection");
                return;
            }
        };
        if !deleted_file_entries.is_empty() {
            let num_deleted_splits = deleted_file_entries.len();
            let num_deleted_bytes = deleted_file_entries
                .iter()
                .map(|entry| entry.file_size_bytes.as_u64() as usize)
                .sum::<usize>();
            let deleted_files: HashSet<&Path> = deleted_file_entries
                .iter()
                .map(|deleted_entry| deleted_entry.file_name.as_path())
                .take(5)
                .collect();
            info!(
                num_deleted_splits = num_deleted_splits,
                "Janitor deleted {:?} and {} other splits.", deleted_files, num_deleted_splits,
            );
            self.counters.num_deleted_files += num_deleted_splits;
            self.counters.num_deleted_bytes += num_deleted_bytes;
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
    use std::sync::Arc;

    use quickwit_actors::Universe;
    use quickwit_common::ServiceStream;
    use quickwit_common::shared_consts::split_deletion_grace_period;
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

    fn hashmap<K: Eq + std::hash::Hash, V>(key: K, value: V) -> HashMap<K, V> {
        let mut map = HashMap::new();
        map.insert(key, value);
        map
    }

    fn make_splits(index_id: &str, split_ids: &[&str], split_state: SplitState) -> Vec<Split> {
        split_ids
            .iter()
            .map(|split_id| Split {
                split_metadata: SplitMetadata {
                    split_id: split_id.to_string(),
                    index_uid: IndexUid::for_test(index_id, 0),
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
        let index_uid = IndexUid::for_test("test-index", 0);
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
                let splits = match query.split_states[0] {
                    SplitState::Staged => {
                        assert_eq!(query.index_uids.unwrap()[0], index_uid_clone);
                        make_splits("test-index", &["a"], SplitState::Staged)
                    }
                    SplitState::MarkedForDeletion => {
                        assert!(query.index_uids.is_none());
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

                        make_splits(
                            "test-index",
                            &["a", "b", "c"],
                            SplitState::MarkedForDeletion,
                        )
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
            hashmap(index_uid, Arc::new(mock_storage)),
            MetastoreServiceClient::from_mock(mock_metastore),
            STAGED_GRACE_PERIOD,
            split_deletion_grace_period(),
            false,
            None,
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
                let splits = match query.split_states[0] {
                    SplitState::Staged => {
                        assert_eq!(&query.index_uids.unwrap()[0].index_id, "test-index");
                        make_splits("test-index", &["a"], SplitState::Staged)
                    }
                    SplitState::MarkedForDeletion => {
                        assert!(query.index_uids.is_none());
                        make_splits(
                            "test-index",
                            &["a", "b", "c"],
                            SplitState::MarkedForDeletion,
                        )
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
                let splits = match query.split_states[0] {
                    SplitState::Staged => {
                        assert_eq!(&query.index_uids.unwrap()[0].index_id, "test-index");
                        make_splits("test-index", &["a"], SplitState::Staged)
                    }
                    SplitState::MarkedForDeletion => {
                        assert!(&query.index_uids.is_none());
                        make_splits("test-index", &["a", "b"], SplitState::MarkedForDeletion)
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
        assert_eq!(counters.num_successful_gc_run, 1);
        assert_eq!(counters.num_failed_storage_resolution, 0);
        assert_eq!(counters.num_failed_gc_run, 0);
        assert_eq!(counters.num_failed_splits, 0);

        // 30 secs later
        universe.sleep(Duration::from_secs(30)).await;
        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_passes, 1);
        assert_eq!(counters.num_deleted_files, 2);
        assert_eq!(counters.num_deleted_bytes, 40);
        assert_eq!(counters.num_successful_gc_run, 1);
        assert_eq!(counters.num_failed_storage_resolution, 0);
        assert_eq!(counters.num_failed_gc_run, 0);
        assert_eq!(counters.num_failed_splits, 0);

        // 60 secs later
        universe.sleep(RUN_INTERVAL).await;
        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_passes, 2);
        assert_eq!(counters.num_deleted_files, 4);
        assert_eq!(counters.num_deleted_bytes, 80);
        assert_eq!(counters.num_successful_gc_run, 2);
        assert_eq!(counters.num_failed_storage_resolution, 0);
        assert_eq!(counters.num_failed_gc_run, 0);
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
        assert_eq!(counters.num_successful_gc_run, 0);
        assert_eq!(counters.num_failed_storage_resolution, 1);
        assert_eq!(counters.num_failed_gc_run, 0);
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
            .times(3)
            .returning(|list_splits_request| {
                let query = list_splits_request.deserialize_list_splits_query().unwrap();
                let splits_ids_string: Vec<String> =
                    (0..8000).map(|seq| format!("split-{seq:04}")).collect();
                let splits_ids: Vec<&str> = splits_ids_string
                    .iter()
                    .map(|string| string.as_str())
                    .collect();
                let mut splits = match query.split_states[0] {
                    SplitState::Staged => {
                        let index_uids = query.index_uids.unwrap();
                        assert_eq!(index_uids.len(), 2);
                        assert!(
                            ["test-index-1", "test-index-2"]
                                .contains(&index_uids[0].index_id.as_ref())
                        );
                        assert!(
                            ["test-index-1", "test-index-2"]
                                .contains(&index_uids[1].index_id.as_ref())
                        );
                        let mut splits = make_splits("test-index-1", &["a"], SplitState::Staged);
                        splits.append(&mut make_splits("test-index-2", &["a"], SplitState::Staged));
                        splits
                    }
                    SplitState::MarkedForDeletion => {
                        assert!(query.index_uids.is_none());
                        assert_eq!(query.limit, Some(10_000));
                        let mut splits =
                            make_splits("test-index-1", &splits_ids, SplitState::MarkedForDeletion);
                        splits.append(&mut make_splits(
                            "test-index-2",
                            &splits_ids,
                            SplitState::MarkedForDeletion,
                        ));
                        splits
                    }
                    _ => panic!("only Staged and MarkedForDeletion expected."),
                };
                if let Some((index_uid, split_id)) = query.after_split {
                    splits.retain(|split| {
                        (
                            &split.split_metadata.index_uid,
                            &split.split_metadata.split_id,
                        ) > (&index_uid, &split_id)
                    });
                }
                splits.truncate(10_000);
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
            .times(3)
            .returning(|delete_splits_request| {
                let index_uid: IndexUid = delete_splits_request.index_uid().clone();
                let split_ids = HashSet::<&str>::from_iter(
                    delete_splits_request
                        .split_ids
                        .iter()
                        .map(|split_id| split_id.as_str()),
                );
                if index_uid.index_id == "test-index-1" {
                    assert_eq!(split_ids.len(), 8000);
                    for seq in 0..8000 {
                        let split_id = format!("split-{seq:04}");
                        assert!(split_ids.contains(&*split_id));
                    }
                } else if split_ids.len() == 2000 {
                    for seq in 0..2000 {
                        let split_id = format!("split-{seq:04}");
                        assert!(split_ids.contains(&*split_id));
                    }
                } else if split_ids.len() == 6000 {
                    for seq in 2000..8000 {
                        let split_id = format!("split-{seq:04}");
                        assert!(split_ids.contains(&*split_id));
                    }
                } else {
                    panic!();
                }

                // This should not cause the whole run to fail and return an error,
                // instead this should simply get logged and return the list of splits
                // which have successfully been deleted.
                if index_uid.index_id == "test-index-2" && split_ids.len() == 2000 {
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
        assert_eq!(counters.num_deleted_files, 14000);
        assert_eq!(counters.num_deleted_bytes, 20 * 14000);
        assert_eq!(counters.num_successful_gc_run, 1);
        assert_eq!(counters.num_failed_storage_resolution, 0);
        assert_eq!(counters.num_failed_gc_run, 0);
        assert_eq!(counters.num_failed_splits, 2000);
        universe.assert_quit().await;
    }
}
