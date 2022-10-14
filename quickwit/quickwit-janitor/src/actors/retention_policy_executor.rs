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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use itertools::Itertools;
use quickwit_actors::{Actor, ActorContext, Handler};
use quickwit_metastore::{IndexMetadata, Metastore};
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
    index_id: String,
}

/// An actor for scheduling retention policy execution on all indexes.
/// It keeps a list of indexes that have retention policy configured
/// in a cache and periodically update this list.
pub struct RetentionPolicyExecutor {
    metastore: Arc<dyn Metastore>,
    /// A map of index_id to index metadata that are managed by this executor.
    /// This act as local cache that is periodically updated while taking into
    /// account deleted indexes, updated or removed retention policy on indexes.
    index_metadatas: HashMap<String, IndexMetadata>,
    counters: RetentionPolicyExecutorCounters,
}

impl RetentionPolicyExecutor {
    pub fn new(metastore: Arc<dyn Metastore>) -> Self {
        Self {
            metastore,
            index_metadatas: HashMap::new(),
            counters: RetentionPolicyExecutorCounters::default(),
        }
    }

    /// Indexes refresh Loop handler logic.
    /// Should not return an error to prevent the actor from crashing.
    async fn handle_refresh_loop(&mut self, ctx: &ActorContext<Self>) {
        debug!("retention-policy-refresh-indexes-operation");
        self.counters.num_refresh_passes += 1;

        let index_metadatas = match self.metastore.list_indexes_metadatas().await {
            Ok(metadatas) => metadatas,
            Err(error) => {
                error!(error=?error, "Failed to list indexes from the metastore.");
                return;
            }
        };
        debug!(index_ids=%index_metadatas.iter().map(|im| &im.index_id).join(", "), "Retention policy refresh.");

        let deleted_indexes = compute_deleted_indexes(
            self.index_metadatas.keys(),
            index_metadatas.iter().map(|metadata| &metadata.index_id),
        );
        if !deleted_indexes.is_empty() {
            debug!(index_ids=%deleted_indexes.iter().join(", "), "Deleting indexes from cache.");
            for index_id in &deleted_indexes {
                self.index_metadatas.remove(index_id);
            }
        }

        for index_metadata in index_metadatas.into_iter() {
            // We only care about indexes with a retention policy configured.
            let retention_policy = match &index_metadata.retention_policy {
                Some(policy) => policy,
                None => {
                    // Remove the index from the cache if it exist.
                    // In case where the retention policy was removed this index might have
                    // been inserted in the cache from a previous iteration.
                    self.index_metadatas.remove(&index_metadata.index_id);
                    continue;
                }
            };

            // Insert or update the index in the cache.
            if let Some(value) = self.index_metadatas.get_mut(&index_metadata.index_id) {
                // Update the cache index entry in case the retention policy was updated.
                *value = index_metadata;
                continue;
            }

            if let Ok(next_interval) = retention_policy.duration_until_next_evaluation() {
                let message = Execute {
                    index_id: index_metadata.index_id.clone(),
                };
                info!(index_id=?index_metadata.index_id, scheduled_in=?next_interval, "retention-policy-shedule-operation");
                // Inserts & schedule the index's first retention policy execution.
                self.index_metadatas
                    .insert(index_metadata.index_id.clone(), index_metadata);
                ctx.schedule_self_msg(next_interval, message).await;
            } else {
                error!(index_id=%index_metadata.index_id, "Couldn't extract the index next schedule time.")
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
        ctx.schedule_self_msg(RUN_INTERVAL, Loop).await;
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
        info!(index_id=%message.index_id, "retention-policy-execute-operation");
        self.counters.num_execution_passes += 1;

        let index_metadata = match self.index_metadatas.get(&message.index_id) {
            Some(metadata) => metadata,
            None => {
                debug!(index_id=%message.index_id, "The index might have been deleted.");
                return Ok(());
            }
        };

        let retention_policy = index_metadata
            .retention_policy
            .as_ref()
            .expect("Expected index to have retention policy configure.");

        let execution_result = run_execute_retention_policy(
            &message.index_id,
            self.metastore.clone(),
            retention_policy,
            Some(ctx),
        )
        .await;
        match execution_result {
            Ok(splits) => self.counters.num_expired_splits += splits.len(),
            Err(error) => {
                error!(index_id=%message.index_id, error=?error, "Failed to execute the retention policy on the index.")
            }
        }

        if let Ok(next_interval) = retention_policy.duration_until_next_evaluation() {
            info!(index_id=?index_metadata.index_id, scheduled_in=?next_interval, "retention-policy-shedule-operation");
            ctx.schedule_self_msg(next_interval, message).await;
        } else {
            // Since we have failed to schedule next execution for this index,
            // we remove it from the cache for it to be retried next time it gets
            // added back by the RetentionPolicyExecutor cache refresh loop.
            self.index_metadatas.remove(&message.index_id);
            error!(index_id=%message.index_id, "Couldn't extract the index next schedule interval.");
        }
        Ok(())
    }
}

/// Extract the list of deleted indexes.
fn compute_deleted_indexes<'a>(
    cached_indexes: impl Iterator<Item = &'a String>,
    indexes: impl Iterator<Item = &'a String>,
) -> HashSet<String> {
    let cached_set: HashSet<_> = cached_indexes.collect();
    let indexes_set: HashSet<_> = indexes.collect();
    (&cached_set - &indexes_set).into_iter().cloned().collect()
}

#[cfg(test)]
mod tests {
    use std::ops::RangeInclusive;

    use mockall::Sequence;
    use quickwit_actors::Universe;
    use quickwit_config::{RetentionPolicy, RetentionPolicyCutoffReference};
    use quickwit_metastore::{IndexMetadata, MockMetastore, Split, SplitMetadata, SplitState};
    use time::OffsetDateTime;

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
                .index_metadatas
                .values()
                .map(|im| (&im.index_id, &im.retention_policy))
                .collect();

            let expected_indexes = make_indexes(&message.0);
            let expected_indexes_set: HashSet<_> = expected_indexes
                .iter()
                .map(|im| (&im.index_id, &im.retention_policy))
                .collect();
            assert_eq!(
                indexes_set, expected_indexes_set,
                "Mismatch set of indexes."
            );
            Ok(())
        }
    }

    const SCHEDULE_EXPR: &str = "hourly";

    fn make_index(index_id: &str, retention_period_opt: Option<&str>) -> IndexMetadata {
        let mut index = IndexMetadata::for_test(index_id, &format!("ram://indexes/{}", index_id));
        if let Some(retention_period) = retention_period_opt {
            index.retention_policy = Some(RetentionPolicy::new(
                retention_period.to_string(),
                RetentionPolicyCutoffReference::PublishTimestamp,
                SCHEDULE_EXPR.to_string(),
            ))
        }
        index
    }

    fn make_indexes(index_ids: &[(&str, Option<&str>)]) -> Vec<IndexMetadata> {
        index_ids
            .iter()
            .map(|(index_id, retention_period_opt)| make_index(index_id, *retention_period_opt))
            .collect()
    }

    fn make_split(
        split_id: &str,
        publish_ts_opt: Option<i64>,
        time_range: Option<RangeInclusive<i64>>,
    ) -> Split {
        Split {
            split_metadata: SplitMetadata {
                split_id: split_id.to_string(),
                footer_offsets: 5..20,
                time_range,
                ..Default::default()
            },
            split_state: SplitState::Published,
            update_timestamp: 0i64,
            publish_timestamp: publish_ts_opt,
        }
    }

    // Uses the retention policy scheduler to calculate
    // how much time to advance for the execution to take place.
    fn shift_time_by() -> Duration {
        let scheduler = RetentionPolicy::new(
            "".to_string(),
            RetentionPolicyCutoffReference::PublishTimestamp,
            SCHEDULE_EXPR.to_string(),
        );
        scheduler.duration_until_next_evaluation().unwrap() + Duration::from_secs(1)
    }

    #[tokio::test]
    async fn test_retention_executor_refresh() -> anyhow::Result<()> {
        let mut mock_metastore = MockMetastore::default();

        let mut sequence = Sequence::new();
        mock_metastore
            .expect_list_splits()
            .times(..)
            .returning(|_, _, _, _| Ok(vec![]));
        mock_metastore
            .expect_list_indexes_metadatas()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|| {
                Ok(make_indexes(&[
                    ("a", Some("1 hour")),
                    ("b", Some("1 hour")),
                    ("c", None),
                ]))
            });

        mock_metastore
            .expect_list_indexes_metadatas()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|| {
                Ok(make_indexes(&[
                    ("a", Some("1 hour")),
                    ("b", Some("2 hour")),
                    ("c", Some("1 hour")),
                ]))
            });

        mock_metastore
            .expect_list_indexes_metadatas()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|| {
                Ok(make_indexes(&[
                    ("b", Some("1 hour")),
                    ("d", Some("1 hour")),
                    ("e", None),
                ]))
            });

        let retention_policy_executor = RetentionPolicyExecutor::new(Arc::new(mock_metastore));
        let universe = Universe::new();
        let (mailbox, handle) = universe.spawn_builder().spawn(retention_policy_executor);

        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_refresh_passes, 1);
        mailbox
            .ask(AssertState(vec![
                ("a", Some("1 hour")),
                ("b", Some("1 hour")),
            ]))
            .await?;

        universe
            .simulate_time_shift(RUN_INTERVAL + Duration::from_secs(5))
            .await;
        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_refresh_passes, 2);
        mailbox
            .ask(AssertState(vec![
                ("a", Some("1 hour")),
                ("b", Some("2 hour")),
                ("c", Some("1 hour")),
            ]))
            .await?;

        universe
            .simulate_time_shift(RUN_INTERVAL + Duration::from_secs(5))
            .await;
        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_refresh_passes, 3);
        mailbox
            .ask(AssertState(vec![
                ("b", Some("1 hour")),
                ("d", Some("1 hour")),
            ]))
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_retention_policy_execution_calls_dependencies() -> anyhow::Result<()> {
        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_list_indexes_metadatas()
            .times(..)
            .returning(|| {
                Ok(make_indexes(&[
                    ("a", Some("2 hour")),
                    ("b", Some("1 hour")),
                    ("c", None),
                ]))
            });

        mock_metastore
            .expect_list_splits()
            .times(2)
            .returning(|index_id, split_state, _, _| {
                assert_eq!(split_state, SplitState::Published);
                let now = OffsetDateTime::now_utc().unix_timestamp();
                let two_hours_ago = now - (60 * 60 * 2);
                let three_hours_ago = now - (60 * 60 * 3);
                let splits = match index_id {
                    "a" => vec![
                        make_split("split-1", Some(two_hours_ago), None),
                        make_split("split-2", Some(three_hours_ago), None),
                        make_split("split-3", Some(now), None),
                    ],
                    "b" => vec![],
                    unknown => panic!("Unknown index: `{}`.", unknown),
                };
                Ok(splits)
            });

        mock_metastore
            .expect_mark_splits_for_deletion()
            .times(1)
            .returning(|index_id, split_ids| {
                assert_eq!(index_id, "a");
                assert_eq!(split_ids, ["split-1", "split-2"]);
                Ok(())
            });

        let retention_policy_executor = RetentionPolicyExecutor::new(Arc::new(mock_metastore));
        let universe = Universe::new();
        let (_mailbox, handle) = universe.spawn_builder().spawn(retention_policy_executor);

        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_execution_passes, 0);
        assert_eq!(counters.num_expired_splits, 0);

        universe.simulate_time_shift(shift_time_by()).await;
        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.num_execution_passes, 2);
        assert_eq!(counters.num_expired_splits, 2);

        Ok(())
    }
}
