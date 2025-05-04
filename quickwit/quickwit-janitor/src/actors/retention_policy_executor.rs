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
use std::time::Duration;

use async_trait::async_trait;
use itertools::Itertools;
use quickwit_actors::{Actor, ActorContext, Handler};
use quickwit_config::IndexConfig;
use quickwit_metastore::ListIndexesMetadataResponseExt;
use quickwit_proto::metastore::{
    ListIndexesMetadataRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::types::IndexUid;
use serde::Serialize;
use tracing::{debug, error, info};

use crate::retention_policy_execution::run_execute_retention_policy;

const RUN_INTERVAL: Duration = Duration::from_secs(60 * 60); // 1 hours

#[derive(Clone, Debug, Default, Serialize)]
pub struct RetentionPolicyExecutorCounters {
    /// The number of refresh the config passes.
    pub num_refresh_passes: usize,

    /// The number of execution passes.
    pub num_execution_passes: usize,

    /// The number of expired splits.
    pub num_expired_splits: usize,
}

#[derive(Debug)]
struct Loop;

#[derive(Debug)]
struct Execute {
    index_uid: IndexUid,
}

/// An actor for scheduling retention policy execution on all indexes.
/// It keeps a list of indexes that have retention policy configured
/// in a cache and periodically update this list.
pub struct RetentionPolicyExecutor {
    metastore: MetastoreServiceClient,
    /// A map of index_id to index metadata that are managed by this executor.
    /// This act as local cache that is periodically updated while taking into
    /// account deleted indexes, updated or removed retention policy on indexes.
    index_configs: HashMap<String, IndexConfig>,
    counters: RetentionPolicyExecutorCounters,
}

impl RetentionPolicyExecutor {
    pub fn new(metastore: MetastoreServiceClient) -> Self {
        Self {
            metastore,
            index_configs: HashMap::new(),
            counters: RetentionPolicyExecutorCounters::default(),
        }
    }

    /// Indexes refresh Loop handler logic.
    /// Should not return an error to prevent the actor from crashing.
    async fn handle_refresh_loop(&mut self, ctx: &ActorContext<Self>) {
        debug!("loading indexes from the metastore");
        self.counters.num_refresh_passes += 1;

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

        let deleted_indexes = compute_deleted_indexes(
            self.index_configs.keys().map(String::as_str),
            indexes
                .iter()
                .map(|index_metadata| index_metadata.index_id()),
        );
        if !deleted_indexes.is_empty() {
            debug!(index_ids=%deleted_indexes.iter().join(", "), "deleting indexes from cache");
            for index_id in deleted_indexes {
                self.index_configs.remove(&index_id);
            }
        }
        for index_metadata in indexes {
            let index_uid = index_metadata.index_uid.clone();
            let index_config = index_metadata.into_index_config();
            // We only care about indexes with a retention policy configured.
            let retention_policy = match &index_config.retention_policy_opt {
                Some(policy) => policy,
                None => {
                    // Remove the index from the cache if it exist.
                    // In case where the retention policy was removed this index might have
                    // been inserted in the cache from a previous iteration.
                    self.index_configs.remove(&index_config.index_id);
                    continue;
                }
            };

            // Insert or update the index in the cache.
            if let Some(value) = self.index_configs.get_mut(&index_config.index_id) {
                // Update the cache index entry in case the retention policy was updated.
                *value = index_config;
                continue;
            }

            if let Ok(next_interval) = retention_policy.duration_until_next_evaluation() {
                let message = Execute { index_uid };
                info!(index_id=?index_config.index_id, scheduled_in=?next_interval, "retention-policy-schedule-operation");
                // Inserts & schedule the index's first retention policy execution.
                self.index_configs
                    .insert(index_config.index_id.clone(), index_config);
                ctx.schedule_self_msg(next_interval, message);
            } else {
                error!(index_id=%index_config.index_id, "Couldn't extract the index next schedule time.")
            }
        }
    }
}

#[async_trait]
impl Actor for RetentionPolicyExecutor {
    type ObservableState = RetentionPolicyExecutorCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn name(&self) -> String {
        "RetentionPolicyExecutor".to_string()
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
impl Handler<Loop> for RetentionPolicyExecutor {
    type Reply = ();

    async fn handle(
        &mut self,
        _: Loop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        self.handle_refresh_loop(ctx).await;
        ctx.schedule_self_msg(RUN_INTERVAL, Loop);
        Ok(())
    }
}

#[async_trait]
impl Handler<Execute> for RetentionPolicyExecutor {
    type Reply = ();

    async fn handle(
        &mut self,
        message: Execute,
        ctx: &ActorContext<Self>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        info!(index_id=%message.index_uid.index_id, "retention-policy-execute-operation");
        self.counters.num_execution_passes += 1;

        let index_config = match self.index_configs.get(&message.index_uid.index_id) {
            Some(config) => config,
            None => {
                debug!(index_id=%message.index_uid.index_id, "the index might have been deleted");
                return Ok(());
            }
        };

        let retention_policy = index_config
            .retention_policy_opt
            .as_ref()
            .expect("Expected index to have retention policy configure.");

        let execution_result = run_execute_retention_policy(
            message.index_uid.clone(),
            self.metastore.clone(),
            retention_policy,
            ctx,
        )
        .await;
        match execution_result {
            Ok(splits) => self.counters.num_expired_splits += splits.len(),
            Err(error) => {
                error!(index_id=%message.index_uid.index_id, error=?error, "Failed to execute the retention policy on the index.")
            }
        }

        if let Ok(next_interval) = retention_policy.duration_until_next_evaluation() {
            info!(index_id=?index_config.index_id, scheduled_in=?next_interval, "retention-policy-schedule-operation");
            ctx.schedule_self_msg(next_interval, message);
        } else {
            // Since we have failed to schedule next execution for this index,
            // we remove it from the cache for it to be retried next time it gets
            // added back by the RetentionPolicyExecutor cache refresh loop.
            self.index_configs.remove(&message.index_uid.index_id);
            error!(index_id=%message.index_uid.index_id, "couldn't extract the index next schedule interval");
        }
        Ok(())
    }
}

/// Extract the list of deleted indexes.
fn compute_deleted_indexes<'a>(
    cached_indexes: impl Iterator<Item = &'a str>,
    indexes: impl Iterator<Item = &'a str>,
) -> HashSet<String> {
    let cached_set: HashSet<_> = cached_indexes.collect();
    let indexes_set: HashSet<_> = indexes.collect();
    (&cached_set - &indexes_set)
        .into_iter()
        .map(ToString::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::ops::RangeInclusive;

    use mockall::Sequence;
    use quickwit_actors::Universe;
    use quickwit_common::ServiceStream;
    use quickwit_config::RetentionPolicy;
    use quickwit_metastore::{
        IndexMetadata, ListSplitsRequestExt, ListSplitsResponseExt, Split, SplitMetadata,
        SplitState,
    };
    use quickwit_proto::metastore::{
        EmptyResponse, ListIndexesMetadataResponse, ListSplitsResponse, MockMetastoreService,
    };

    use super::*;

    #[derive(Debug)]
    struct AssertState(Vec<(&'static str, Option<&'static str>)>);

    #[async_trait]
    impl Handler<AssertState> for RetentionPolicyExecutor {
        type Reply = ();

        async fn handle(
            &mut self,
            message: AssertState,
            _ctx: &ActorContext<Self>,
        ) -> Result<Self::Reply, quickwit_actors::ActorExitStatus> {
            let indexes_set: HashSet<_> = self
                .index_configs
                .values()
                .map(|im| (&im.index_id, &im.retention_policy_opt))
                .collect();

            let expected_indexes: Vec<IndexConfig> = make_indexes(&message.0)
                .into_iter()
                .map(IndexMetadata::into_index_config)
                .collect();
            let expected_indexes_set: HashSet<_> = expected_indexes
                .iter()
                .map(|im| (&im.index_id, &im.retention_policy_opt))
                .collect();
            assert_eq!(
                indexes_set, expected_indexes_set,
                "Mismatch set of indexes."
            );
            Ok(())
        }
    }

    const EVALUATION_SCHEDULE: &str = "hourly";

    fn make_index(index_id: &str, retention_period_opt: Option<&str>) -> IndexConfig {
        let mut index = IndexConfig::for_test(index_id, &format!("ram://indexes/{index_id}"));
        if let Some(retention_period) = retention_period_opt {
            index.retention_policy_opt = Some(RetentionPolicy {
                retention_period: retention_period.to_string(),
                evaluation_schedule: EVALUATION_SCHEDULE.to_string(),
                jitter_secs: None,
            })
        }
        index
    }

    fn make_indexes(index_ids: &[(&str, Option<&str>)]) -> Vec<IndexMetadata> {
        index_ids
            .iter()
            .map(|(index_id, retention_period_opt)| make_index(index_id, *retention_period_opt))
            .map(IndexMetadata::new)
            .collect()
    }

    fn make_split(split_id: &str, time_range: Option<RangeInclusive<i64>>) -> Split {
        Split {
            split_metadata: SplitMetadata {
                split_id: split_id.to_string(),
                footer_offsets: 5..20,
                time_range,
                ..Default::default()
            },
            split_state: SplitState::Published,
            update_timestamp: 0,
            publish_timestamp: Some(100),
        }
    }

    // Uses the retention policy scheduler to calculate
    // how much time to advance for the execution to take place.
    fn shift_time_by() -> Duration {
        let scheduler = RetentionPolicy {
            retention_period: "".to_string(),
            evaluation_schedule: EVALUATION_SCHEDULE.to_string(),
            jitter_secs: None,
        };

        scheduler.duration_until_next_evaluation().unwrap() + Duration::from_secs(1)
    }

    #[tokio::test]
    async fn test_retention_executor_refresh() -> anyhow::Result<()> {
        let mut mock_metastore = MockMetastoreService::new();

        let mut sequence = Sequence::new();
        mock_metastore
            .expect_list_splits()
            .times(..)
            .returning(|_| Ok(ServiceStream::empty()));
        mock_metastore
            .expect_list_indexes_metadata()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_list_indexes_request| {
                let indexes_metadata = make_indexes(&[
                    ("index-1", Some("1 hour")),
                    ("index-2", Some("1 hour")),
                    ("index-3", None),
                ]);
                Ok(ListIndexesMetadataResponse::for_test(indexes_metadata))
            });

        mock_metastore
            .expect_list_indexes_metadata()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_list_indexes_request| {
                let indexes_metadata = make_indexes(&[
                    ("index-1", Some("1 hour")),
                    ("index-2", Some("2 hour")),
                    ("index-3", Some("1 hour")),
                ]);
                Ok(ListIndexesMetadataResponse::for_test(indexes_metadata))
            });

        mock_metastore
            .expect_list_indexes_metadata()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_list_indexes_request| {
                let indexes_metadata = make_indexes(&[
                    ("index-2", Some("1 hour")),
                    ("index-4", Some("1 hour")),
                    ("index-5", None),
                ]);
                Ok(ListIndexesMetadataResponse::for_test(indexes_metadata))
            });

        let retention_policy_executor =
            RetentionPolicyExecutor::new(MetastoreServiceClient::from_mock(mock_metastore));
        let universe = Universe::with_accelerated_time();
        let (mailbox, handle) = universe.spawn_builder().spawn(retention_policy_executor);

        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_refresh_passes, 1);
        mailbox
            .ask(AssertState(vec![
                ("index-1", Some("1 hour")),
                ("index-2", Some("1 hour")),
            ]))
            .await?;

        universe.sleep(RUN_INTERVAL + Duration::from_secs(5)).await;
        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_refresh_passes, 2);
        mailbox
            .ask(AssertState(vec![
                ("index-1", Some("1 hour")),
                ("index-2", Some("2 hour")),
                ("index-3", Some("1 hour")),
            ]))
            .await?;

        universe.sleep(RUN_INTERVAL + Duration::from_secs(5)).await;
        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_refresh_passes, 3);
        mailbox
            .ask(AssertState(vec![
                ("index-2", Some("1 hour")),
                ("index-4", Some("1 hour")),
            ]))
            .await?;
        universe.assert_quit().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_retention_policy_execution_calls_dependencies() -> anyhow::Result<()> {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_list_indexes_metadata()
            .times(..)
            .returning(|_list_indexes_request| {
                let indexes_metadata = make_indexes(&[
                    ("index-1", Some("2 hour")),
                    ("index-2", Some("1 hour")),
                    ("index-3", None),
                ]);
                Ok(ListIndexesMetadataResponse::for_test(indexes_metadata))
            });

        mock_metastore
            .expect_list_splits()
            .times(2..=4)
            .returning(|list_splits_request| {
                let query = list_splits_request.deserialize_list_splits_query().unwrap();
                assert_eq!(query.split_states, &[SplitState::Published]);
                let splits = match query.index_uids.unwrap()[0].index_id.as_ref() {
                    "index-1" => {
                        vec![
                            make_split("split-1", Some(1000..=5000)),
                            make_split("split-2", Some(2000..=6000)),
                            make_split("split-3", None),
                        ]
                    }
                    "index-2" => Vec::new(),
                    unknown => panic!("Unknown index: `{unknown}`."),
                };
                let splits_response = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits_response)]))
            });

        mock_metastore
            .expect_mark_splits_for_deletion()
            .times(1..=3)
            .returning(|mark_splits_for_deletion_request| {
                let index_uid: IndexUid = mark_splits_for_deletion_request.index_uid().clone();
                assert_eq!(index_uid.index_id, "index-1");
                assert_eq!(
                    mark_splits_for_deletion_request.split_ids,
                    ["split-1", "split-2"]
                );
                Ok(EmptyResponse {})
            });

        let retention_policy_executor =
            RetentionPolicyExecutor::new(MetastoreServiceClient::from_mock(mock_metastore));
        let universe = Universe::with_accelerated_time();
        let (_mailbox, handle) = universe.spawn_builder().spawn(retention_policy_executor);

        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_execution_passes, 0);
        assert_eq!(counters.num_expired_splits, 0);

        universe.sleep(shift_time_by()).await;
        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_execution_passes, 2);
        assert_eq!(counters.num_expired_splits, 2);
        universe.assert_quit().await;

        Ok(())
    }
}
