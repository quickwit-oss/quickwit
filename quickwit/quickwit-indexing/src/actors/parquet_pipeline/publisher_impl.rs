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

//! `Handler<ParquetSplitsUpdate>` implementation for `Publisher`,
//! specific to the metrics pipeline.

use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::{ActorContext, ActorExitStatus, Handler};
use quickwit_dst::events::merge_pipeline::{MergePipelineEvent, record_merge_pipeline_event};
use quickwit_proto::metastore::{
    MetastoreService, PublishMetricsSplitsRequest, PublishSketchSplitsRequest,
};
use tracing::{info, instrument, warn};

use super::ParquetSplitsUpdate;
use crate::actors::publisher::{Publisher, serialize_checkpoint_delta, suggest_truncate};

pub(crate) const METRICS_PUBLISHER_NAME: &str = "ParquetPublisher";

#[async_trait]
impl Handler<ParquetSplitsUpdate> for Publisher {
    type Reply = ();

    #[instrument(name = "parquet_publisher", parent = split_update.parent_span.id(), skip(self, ctx))]
    async fn handle(
        &mut self,
        split_update: ParquetSplitsUpdate,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let ParquetSplitsUpdate {
            index_uid,
            new_splits,
            replaced_split_ids,
            checkpoint_delta_opt,
            publish_lock,
            publish_token_opt,
            _merge_task_opt,
            ..
        } = split_update;

        let index_checkpoint_delta_json_opt = serialize_checkpoint_delta(&checkpoint_delta_opt)?;
        let split_ids: Vec<String> = new_splits
            .iter()
            .map(|split| split.split_id.as_str().to_string())
            .collect();
        if let Some(_guard) = publish_lock.acquire().await {
            if quickwit_common::is_sketches_index(&index_uid.index_id) {
                let publish_request = PublishSketchSplitsRequest {
                    index_uid: Some(index_uid.clone()),
                    staged_split_ids: split_ids.clone(),
                    replaced_split_ids: replaced_split_ids.clone(),
                    index_checkpoint_delta_json_opt,
                    publish_token_opt: publish_token_opt.clone(),
                };
                ctx.protect_future(self.metastore.publish_sketch_splits(publish_request))
                    .await
                    .context("failed to publish sketch splits")?;
            } else {
                let publish_request = PublishMetricsSplitsRequest {
                    index_uid: Some(index_uid.clone()),
                    staged_split_ids: split_ids.clone(),
                    replaced_split_ids: replaced_split_ids.clone(),
                    index_checkpoint_delta_json_opt,
                    publish_token_opt: publish_token_opt.clone(),
                };
                ctx.protect_future(self.metastore.publish_metrics_splits(publish_request))
                    .await
                    .context("failed to publish metrics splits")?;
            }
        } else {
            info!(
                split_ids=?split_ids,
                "Splits' publish lock is dead."
            );
            return Ok(());
        }
        info!("publish-metrics-splits");

        // Emit lifecycle events for trace conformance (no-op when no
        // observer is installed). Each newly-published split corresponds
        // to either a fresh ingest (replaced empty) or a merge replacement
        // (replaced non-empty + 1 output split per merge).
        let index_uid_str = index_uid.to_string();
        if replaced_split_ids.is_empty() {
            for split in &new_splits {
                let window = split.window.clone().unwrap_or(
                    split.time_range.start_secs as i64..split.time_range.end_secs as i64,
                );
                record_merge_pipeline_event(&MergePipelineEvent::IngestSplit {
                    index_uid: index_uid_str.clone(),
                    split_id: split.split_id.as_str().to_string(),
                    num_rows: split.num_rows,
                    window,
                });
            }
        } else {
            // Merge replacement: each output split is one PublishMergeAndFeedback.
            // The merge_id is the output split_id (planner uses output id as merge id).
            for split in &new_splits {
                let window = split.window.clone().unwrap_or(
                    split.time_range.start_secs as i64..split.time_range.end_secs as i64,
                );
                record_merge_pipeline_event(&MergePipelineEvent::PublishMergeAndFeedback {
                    index_uid: index_uid_str.clone(),
                    merge_id: split.split_id.as_str().to_string(),
                    output_split_id: split.split_id.as_str().to_string(),
                    replaced_split_ids: replaced_split_ids.clone(),
                    output_window: window,
                    output_merge_ops: split.num_merge_ops,
                });
            }
        }

        suggest_truncate(ctx, &self.source_mailbox_opt, checkpoint_delta_opt).await;

        // Feedback loop: notify the merge planner about newly published splits so it can plan
        // further compaction. This is BEST-EFFORT and NON-BLOCKING by design: ingest
        // publish/truncate should not block on compaction. A blocking send here
        // would couple ingest WAL truncation to compaction liveness: if the planner is absent or
        // slow (mailbox full, or the merge pipeline failed to spawn), a blocking
        // send wedges the publisher and stalls truncation, which can fill the shared WAL and take
        // down every index on the ingester.
        // Infinite merge loops are prevented by the merge policy's maturity checks
        // (max_merge_ops, target_split_size_bytes, maturation_period), not by filtering here.
        if let Some(planner_mailbox) = &self.parquet_merge_planner_mailbox_opt
            && !new_splits.is_empty()
            && let Err(error) =
                planner_mailbox.try_send_message(super::ParquetNewSplits { new_splits })
        {
            // Dropping is "safe", but not efficient: the planner re-seeds its split set from the
            // metastore only on respawn, so we may have uncompacted files
            warn!(%error, "dropping new-splits feedback to merge planner (best-effort)");
        }

        if split_ids.is_empty() {
            self.counters.num_empty_splits += 1;
        } else if replaced_split_ids.is_empty() {
            self.counters.num_published_splits += 1;
        } else {
            self.counters.num_replace_operations += 1;
        }
        // Keep the merge task alive until after the metastore publish and
        // planner feedback have completed. Dropping it releases both the merge
        // semaphore permit and the planner's tracked-operation inventory guard.
        drop(_merge_task_opt);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_actors::{QueueCapacity, Universe};
    use quickwit_metastore::checkpoint::{IndexCheckpointDelta, SourceCheckpointDelta};
    use quickwit_parquet_engine::split::{ParquetSplitId, ParquetSplitMetadata, TimeRange};
    use quickwit_proto::metastore::{EmptyResponse, MetastoreServiceClient, MockMetastoreService};
    use quickwit_proto::types::IndexUid;
    use tracing::Span;

    use super::{METRICS_PUBLISHER_NAME, ParquetSplitsUpdate};
    use crate::actors::parquet_pipeline::ParquetMergePlanner;
    use crate::actors::publisher::Publisher;
    use crate::models::PublishLock;
    use crate::source::{SourceActor, SuggestTruncate};

    fn create_test_metrics_split_metadata(index_uid: &str, split_id: &str) -> ParquetSplitMetadata {
        ParquetSplitMetadata::metrics_builder()
            .index_uid(index_uid)
            .split_id(ParquetSplitId::new(split_id))
            .time_range(TimeRange::new(1000, 2000))
            .num_rows(100)
            .size_bytes(1024)
            .build()
    }

    #[tokio::test]
    async fn test_metrics_publisher_publishes_splits() {
        let universe = Universe::with_accelerated_time();

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_publish_metrics_splits()
            .withf(|request| {
                request.index_uid().to_string().starts_with("test-index:")
                    && request.staged_split_ids == vec!["split-1".to_string()]
                    && request.replaced_split_ids.is_empty()
                    && request.index_checkpoint_delta_json_opt.is_some()
                    && request.publish_token_opt.is_none()
            })
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));

        let publisher = Publisher::new(
            METRICS_PUBLISHER_NAME,
            QueueCapacity::Bounded(1),
            MetastoreServiceClient::from_mock(mock_metastore),
            None,
            None,
        );
        let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);

        let update = ParquetSplitsUpdate {
            index_uid: IndexUid::for_test("test-index", 0),
            new_splits: vec![create_test_metrics_split_metadata(
                "test-index:00000000000000000000000000",
                "split-1",
            )],
            replaced_split_ids: Vec::new(),
            checkpoint_delta_opt: Some(IndexCheckpointDelta {
                source_id: "test-source".to_string(),
                source_delta: SourceCheckpointDelta::from_range(0..10),
            }),
            publish_lock: PublishLock::default(),
            publish_token_opt: None,
            parent_span: Span::none(),
            _merge_task_opt: None,
        };

        publisher_mailbox.send_message(update).await.unwrap();

        let observation = publisher_handle.process_pending_and_observe().await.state;
        assert_eq!(observation.num_published_splits, 1);
        assert_eq!(observation.num_replace_operations, 0);
        assert_eq!(observation.num_empty_splits, 0);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_publisher_handles_empty_splits() {
        let universe = Universe::with_accelerated_time();

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_publish_metrics_splits()
            .withf(|request| {
                request.index_uid().to_string().starts_with("test-index:")
                    && request.staged_split_ids.is_empty()
                    && request.replaced_split_ids.is_empty()
                    && request.index_checkpoint_delta_json_opt.is_some()
            })
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));

        let publisher = Publisher::new(
            METRICS_PUBLISHER_NAME,
            QueueCapacity::Bounded(1),
            MetastoreServiceClient::from_mock(mock_metastore),
            None,
            None,
        );
        let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);

        let update = ParquetSplitsUpdate {
            index_uid: IndexUid::for_test("test-index", 0),
            new_splits: Vec::new(),
            replaced_split_ids: Vec::new(),
            checkpoint_delta_opt: Some(IndexCheckpointDelta {
                source_id: "test-source".to_string(),
                source_delta: SourceCheckpointDelta::from_range(0..1),
            }),
            publish_lock: PublishLock::default(),
            publish_token_opt: None,
            parent_span: Span::none(),
            _merge_task_opt: None,
        };

        publisher_mailbox.send_message(update).await.unwrap();

        let observation = publisher_handle.process_pending_and_observe().await.state;
        assert_eq!(observation.num_published_splits, 0);
        assert_eq!(observation.num_replace_operations, 0);
        assert_eq!(observation.num_empty_splits, 1);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_publisher_respects_publish_lock() {
        let universe = Universe::with_accelerated_time();

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore.expect_publish_metrics_splits().never();

        let publisher = Publisher::new(
            METRICS_PUBLISHER_NAME,
            QueueCapacity::Bounded(1),
            MetastoreServiceClient::from_mock(mock_metastore),
            None,
            None,
        );
        let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);

        let publish_lock = PublishLock::default();
        publish_lock.kill().await;

        let update = ParquetSplitsUpdate {
            index_uid: IndexUid::for_test("test-index", 0),
            new_splits: vec![create_test_metrics_split_metadata(
                "test-index:00000000000000000000000000",
                "split-1",
            )],
            replaced_split_ids: Vec::new(),
            checkpoint_delta_opt: Some(IndexCheckpointDelta {
                source_id: "test-source".to_string(),
                source_delta: SourceCheckpointDelta::from_range(0..10),
            }),
            publish_lock,
            publish_token_opt: None,
            parent_span: Span::none(),
            _merge_task_opt: None,
        };

        publisher_mailbox.send_message(update).await.unwrap();

        let observation = publisher_handle.process_pending_and_observe().await.state;
        assert_eq!(observation.num_published_splits, 0);
        assert_eq!(observation.num_replace_operations, 0);
        assert_eq!(observation.num_empty_splits, 0);

        universe.assert_quit().await;
    }

    /// Regression: a failed merge-pipeline spawn leaves the merge-planner MAILBOX created
    /// (`ParquetMergePipeline::new`) but its ACTOR never spawned (`spawn_pipeline` fails at
    /// `fetch_immature_splits`). The ingest publisher must NOT block publish/truncate on the
    /// `Bounded(1)` merge-feedback `send(...).await`, or a dead compaction pipeline wedges WAL
    /// truncation and closed shards with data never drain.
    ///
    /// On current code this times out (the publisher wedges on the consumer-less mailbox after
    /// its first feedback send); it should pass once the feedback send is made non-blocking.
    #[tokio::test]
    async fn test_metrics_publisher_not_wedged_when_merge_planner_absent() {
        let universe = Universe::with_accelerated_time();

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_publish_metrics_splits()
            .times(1..)
            .returning(|_| Ok(EmptyResponse {}));

        // Source mailbox is the progress signal (receives SuggestTruncate); unbounded so the
        // truncate send never blocks.
        let (source_mailbox, source_inbox) = universe.create_test_mailbox::<SourceActor>();

        // Merge-planner mailbox with NO consuming actor, Bounded(1): the failed-spawn state.
        // The inbox is held (not dropped) so the channel stays open and sends BLOCK when the
        // queue is full rather than erroring.
        let (planner_mailbox, _planner_inbox_held) = universe
            .create_mailbox::<ParquetMergePlanner>(
                "ParquetMergePlanner",
                QueueCapacity::Bounded(1),
            );

        let publisher = Publisher::new(
            METRICS_PUBLISHER_NAME,
            QueueCapacity::Bounded(1),
            MetastoreServiceClient::from_mock(mock_metastore),
            None,
            Some(source_mailbox),
        )
        .set_parquet_merge_planner_mailbox(planner_mailbox);
        let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);

        let observation = tokio::time::timeout(Duration::from_secs(10), async {
            for i in 0..3u64 {
                let update = ParquetSplitsUpdate {
                    index_uid: IndexUid::for_test("test-index", 0),
                    new_splits: vec![create_test_metrics_split_metadata(
                        "test-index:00000000000000000000000000",
                        &format!("split-{i}"),
                    )],
                    replaced_split_ids: Vec::new(),
                    checkpoint_delta_opt: Some(IndexCheckpointDelta {
                        source_id: "test-source".to_string(),
                        source_delta: SourceCheckpointDelta::from_range((i * 10)..((i + 1) * 10)),
                    }),
                    publish_lock: PublishLock::default(),
                    publish_token_opt: None,
                    parent_span: Span::none(),
                    _merge_task_opt: None,
                };
                publisher_mailbox.send_message(update).await.unwrap();
            }
            publisher_handle.process_pending_and_observe().await.state
        })
        .await
        .expect(
            "publisher wedged: ingest publish/truncate blocked on a consumer-less Bounded(1) \
             merge-planner mailbox",
        );

        assert_eq!(
            observation.num_published_splits, 3,
            "all splits must publish even when the merge planner never consumes feedback"
        );
        let truncates = source_inbox.drain_for_test_typed::<SuggestTruncate>();
        assert_eq!(
            truncates.len(),
            3,
            "WAL truncation must continue when the merge planner is absent"
        );

        universe.assert_quit().await;
    }
}
