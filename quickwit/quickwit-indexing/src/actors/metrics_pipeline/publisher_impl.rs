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
use quickwit_proto::metastore::{
    MetastoreService, PublishMetricsSplitsRequest, PublishSketchSplitsRequest,
};
use tracing::{info, instrument};

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
        suggest_truncate(ctx, &self.source_mailbox_opt, checkpoint_delta_opt).await;

        // Feedback loop: notify the merge planner about all newly published
        // splits — both ingest outputs and merge outputs — so it can plan
        // further compaction. Infinite loops are prevented by the merge
        // policy's maturity checks (max_merge_ops, target_split_size_bytes,
        // maturation_period), not by filtering here. This matches the Tantivy
        // publisher which sends NewSplits unconditionally.
        if let Some(planner_mailbox) = &self.parquet_merge_planner_mailbox_opt
            && !new_splits.is_empty()
        {
            let _ = ctx
                .send_message(planner_mailbox, super::ParquetNewSplits { new_splits })
                .await;
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
    use quickwit_actors::{QueueCapacity, Universe};
    use quickwit_metastore::checkpoint::{IndexCheckpointDelta, SourceCheckpointDelta};
    use quickwit_parquet_engine::split::{ParquetSplitId, ParquetSplitMetadata, TimeRange};
    use quickwit_proto::metastore::{EmptyResponse, MetastoreServiceClient, MockMetastoreService};
    use quickwit_proto::types::IndexUid;
    use tracing::Span;

    use super::{METRICS_PUBLISHER_NAME, ParquetSplitsUpdate};
    use crate::actors::publisher::Publisher;
    use crate::models::PublishLock;

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
}
