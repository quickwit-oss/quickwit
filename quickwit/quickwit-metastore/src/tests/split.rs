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

use std::time::Duration;

use futures::future::try_join_all;
use quickwit_common::rand::append_random_suffix;
use quickwit_config::{IndexConfig, SourceConfig, SourceParams};
use quickwit_proto::metastore::{
    CreateIndexRequest, DeleteSplitsRequest, EntityKind, IndexMetadataRequest, ListSplitsRequest,
    ListStaleSplitsRequest, MarkSplitsForDeletionRequest, MetastoreError, PublishSplitsRequest,
    StageSplitsRequest, UpdateSplitsDeleteOpstampRequest,
};
use quickwit_proto::types::{IndexUid, Position};
use time::OffsetDateTime;
use tokio::time::sleep;
use tracing::{error, info};

use super::DefaultForTest;
use crate::checkpoint::{IndexCheckpointDelta, PartitionId, SourceCheckpointDelta};
use crate::metastore::MetastoreServiceStreamSplitsExt;
use crate::tests::cleanup_index;
use crate::{
    CreateIndexRequestExt, IndexMetadataResponseExt, ListSplitsQuery, ListSplitsRequestExt,
    ListSplitsResponseExt, MetastoreServiceExt, SplitMetadata, SplitState, StageSplitsRequestExt,
};

pub async fn test_metastore_publish_splits_empty_splits_array_is_allowed<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-publish-splits-empty");
    let non_existent_index_uid = IndexUid::new_with_random_ulid(&index_id);
    let index_uri = format!("ram:///indexes/{index_id}");

    let source_id = format!("{index_id}--source");

    // Publish a split on a non-existent index
    {
        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(non_existent_index_uid),
            index_checkpoint_delta_json_opt: Some({
                let offsets = 1..10;
                let checkpoint_delta = IndexCheckpointDelta::for_test(&source_id, offsets);
                serde_json::to_string(&checkpoint_delta).unwrap()
            }),
            ..Default::default()
        };
        let error = metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MetastoreError::NotFound(EntityKind::Index { .. })
        ));
    }

    // Update the checkpoint, by publishing an empty array of splits with a non-empty
    // checkpoint. This operation is allowed and used in the Indexer.
    {
        let index_config = IndexConfig::for_test(&index_id, &index_uri);
        let source_configs = &[SourceConfig::for_test(&source_id, SourceParams::void())];
        let create_index_request =
            CreateIndexRequest::try_from_index_and_source_configs(&index_config, source_configs)
                .unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();
        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            index_checkpoint_delta_json_opt: Some({
                let offsets = 0..100;
                let checkpoint_delta = IndexCheckpointDelta::for_test(&source_id, offsets);
                serde_json::to_string(&checkpoint_delta).unwrap()
            }),
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        let index_metadata = metastore
            .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap();
        let source_checkpoint = index_metadata
            .checkpoint
            .source_checkpoint(&source_id)
            .unwrap();
        assert_eq!(source_checkpoint.num_partitions(), 1);
        assert_eq!(
            source_checkpoint
                .position_for_partition(&PartitionId::default())
                .unwrap(),
            &Position::offset(100u64 - 1)
        );
        cleanup_index(&mut metastore, index_uid).await;
    }
}

pub async fn test_metastore_publish_splits<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

    let index_id = append_random_suffix("test-publish-splits");
    let index_uid = IndexUid::new_with_random_ulid(&index_id);
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let source_id = format!("{index_id}--source");
    let source_configs = &[SourceConfig::for_test(&source_id, SourceParams::void())];

    let split_id_1 = format!("{index_id}--split-1");
    let split_metadata_1 = SplitMetadata {
        split_id: split_id_1.clone(),
        index_uid: index_uid.clone(),
        time_range: Some(0..=99),
        create_timestamp: current_timestamp,
        ..Default::default()
    };

    let split_id_2 = format!("{index_id}--split-2");
    let split_metadata_2 = SplitMetadata {
        split_id: split_id_2.clone(),
        index_uid: index_uid.clone(),
        time_range: Some(30..=99),
        create_timestamp: current_timestamp,
        ..Default::default()
    };

    // Publish a split on a non-existent index
    {
        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(IndexUid::new_with_random_ulid("index-not-found")),
            staged_split_ids: vec!["split-not-found".to_string()],
            index_checkpoint_delta_json_opt: Some({
                let offsets = 0..10;
                let checkpoint_delta = IndexCheckpointDelta::for_test(&source_id, offsets);
                serde_json::to_string(&checkpoint_delta).unwrap()
            }),
            ..Default::default()
        };
        let error = metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MetastoreError::NotFound(EntityKind::Index { .. })
        ));
    }

    // Publish a split on a wrong index uid
    {
        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(IndexUid::new_with_random_ulid(&index_id)),
            staged_split_ids: vec!["split-not-found".to_string()],
            index_checkpoint_delta_json_opt: Some({
                let offsets = 0..10;
                let checkpoint_delta = IndexCheckpointDelta::for_test(&source_id, offsets);
                serde_json::to_string(&checkpoint_delta).unwrap()
            }),
            ..Default::default()
        };
        let error = metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MetastoreError::NotFound(EntityKind::Index { .. })
        ));
    }

    // Publish a non-existent split on an index
    {
        let create_index_request =
            CreateIndexRequest::try_from_index_and_source_configs(&index_config, source_configs)
                .unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();
        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec!["split-not-found".to_string()],
            ..Default::default()
        };
        let error = metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MetastoreError::NotFound(EntityKind::Splits { .. })
        ));

        cleanup_index(&mut metastore, index_uid).await;
    }

    // Publish a staged split on an index
    {
        let create_index_request =
            CreateIndexRequest::try_from_index_and_source_configs(&index_config, source_configs)
                .unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata_1)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_1.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        cleanup_index(&mut metastore, index_uid).await;
    }

    // Publish a published split on an index
    {
        let create_index_request =
            CreateIndexRequest::try_from_index_and_source_configs(&index_config, source_configs)
                .unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata_1)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_1.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_1.clone()],
            index_checkpoint_delta_json_opt: Some({
                let offsets = 1..12;
                let checkpoint_delta = IndexCheckpointDelta::for_test(&source_id, offsets);
                serde_json::to_string(&checkpoint_delta).unwrap()
            }),
            ..Default::default()
        };
        let error = metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MetastoreError::FailedPrecondition {
                entity: EntityKind::Splits { .. },
                ..
            }
        ));

        cleanup_index(&mut metastore, index_uid).await;
    }

    // Publish a non-staged split on an index
    {
        let create_index_request =
            CreateIndexRequest::try_from_index_and_source_configs(&index_config, source_configs)
                .unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata_1)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_1.clone()],
            index_checkpoint_delta_json_opt: Some({
                let offsets = 12..15;
                let checkpoint_delta = IndexCheckpointDelta::for_test(&source_id, offsets);
                serde_json::to_string(&checkpoint_delta).unwrap()
            }),
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        let mark_splits_for_deletion_request =
            MarkSplitsForDeletionRequest::new(index_uid.clone(), vec![split_id_1.clone()]);
        metastore
            .mark_splits_for_deletion(mark_splits_for_deletion_request)
            .await
            .unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_1.clone()],
            index_checkpoint_delta_json_opt: Some({
                let offsets = 15..18;
                let checkpoint_delta = IndexCheckpointDelta::for_test(&source_id, offsets);
                serde_json::to_string(&checkpoint_delta).unwrap()
            }),
            ..Default::default()
        };
        let error = metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MetastoreError::FailedPrecondition {
                entity: EntityKind::Splits { .. },
                ..
            }
        ));

        cleanup_index(&mut metastore, index_uid).await;
    }

    // Publish a staged split and non-existent split on an index
    {
        let create_index_request =
            CreateIndexRequest::try_from_index_and_source_configs(&index_config, source_configs)
                .unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata_1)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_1.clone(), "split-not-found".to_string()],
            index_checkpoint_delta_json_opt: Some({
                let offsets = 15..18;
                let checkpoint_delta = IndexCheckpointDelta::for_test(&source_id, offsets);
                serde_json::to_string(&checkpoint_delta).unwrap()
            }),
            ..Default::default()
        };
        let error = metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MetastoreError::NotFound(EntityKind::Splits { .. })
        ));

        cleanup_index(&mut metastore, index_uid).await;
    }

    // Publish a published split and non-existent split on an index
    {
        let create_index_request =
            CreateIndexRequest::try_from_index_and_source_configs(&index_config, source_configs)
                .unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata_1)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_1.clone()],
            index_checkpoint_delta_json_opt: Some({
                let offsets = 15..18;
                let checkpoint_delta = IndexCheckpointDelta::for_test(&source_id, offsets);
                serde_json::to_string(&checkpoint_delta).unwrap()
            }),
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_1.clone(), "split-not-found".to_string()],
            index_checkpoint_delta_json_opt: Some({
                let offsets = 18..24;
                let checkpoint_delta = IndexCheckpointDelta::for_test(&source_id, offsets);
                serde_json::to_string(&checkpoint_delta).unwrap()
            }),
            ..Default::default()
        };
        let error = metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MetastoreError::NotFound(EntityKind::Splits { .. })
        ));

        cleanup_index(&mut metastore, index_uid).await;
    }

    // Publish a non-staged split and non-existent split on an index
    {
        let create_index_request =
            CreateIndexRequest::try_from_index_and_source_configs(&index_config, source_configs)
                .unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata_1)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_1.clone()],
            index_checkpoint_delta_json_opt: Some({
                let offsets = 18..24;
                let checkpoint_delta = IndexCheckpointDelta::for_test(&source_id, offsets);
                serde_json::to_string(&checkpoint_delta).unwrap()
            }),
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        let mark_splits_for_deletion_request =
            MarkSplitsForDeletionRequest::new(index_uid.clone(), vec![split_id_1.clone()]);
        metastore
            .mark_splits_for_deletion(mark_splits_for_deletion_request)
            .await
            .unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_1.clone(), "split-not-found".to_string()],
            index_checkpoint_delta_json_opt: Some({
                let offsets = 24..26;
                let checkpoint_delta = IndexCheckpointDelta::for_test(&source_id, offsets);
                serde_json::to_string(&checkpoint_delta).unwrap()
            }),
            ..Default::default()
        };
        let error = metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MetastoreError::NotFound(EntityKind::Splits { .. })
        ));

        cleanup_index(&mut metastore, index_uid).await;
    }

    // Publish staged splits on an index
    {
        let create_index_request =
            CreateIndexRequest::try_from_index_and_source_configs(&index_config, source_configs)
                .unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata_1)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata_2)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_1.clone(), split_id_2.clone()],
            index_checkpoint_delta_json_opt: Some({
                let offsets = 24..26;
                let checkpoint_delta = IndexCheckpointDelta::for_test(&source_id, offsets);
                serde_json::to_string(&checkpoint_delta).unwrap()
            }),
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        cleanup_index(&mut metastore, index_uid).await;
    }

    // Publish a staged split and published split on an index
    {
        let create_index_request =
            CreateIndexRequest::try_from_index_and_source_configs(&index_config, source_configs)
                .unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid.clone(),
            [split_metadata_1.clone(), split_metadata_2.clone()],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_2.clone()],
            index_checkpoint_delta_json_opt: Some({
                let offsets = 26..28;
                let checkpoint_delta = IndexCheckpointDelta::for_test(&source_id, offsets);
                serde_json::to_string(&checkpoint_delta).unwrap()
            }),
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_1.clone(), split_id_2.clone()],
            index_checkpoint_delta_json_opt: Some({
                let offsets = 28..30;
                let checkpoint_delta = IndexCheckpointDelta::for_test(&source_id, offsets);
                serde_json::to_string(&checkpoint_delta).unwrap()
            }),
            ..Default::default()
        };
        let error = metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MetastoreError::FailedPrecondition {
                entity: EntityKind::Splits { .. },
                ..
            }
        ));

        cleanup_index(&mut metastore, index_uid).await;
    }

    // Publish published splits on an index
    {
        let create_index_request =
            CreateIndexRequest::try_from_index_and_source_configs(&index_config, source_configs)
                .unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid.clone(),
            [split_metadata_1.clone(), split_metadata_2.clone()],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_1.clone(), split_id_2.clone()],
            index_checkpoint_delta_json_opt: Some({
                let offsets = 30..31;
                let checkpoint_delta = IndexCheckpointDelta::for_test(&source_id, offsets);
                serde_json::to_string(&checkpoint_delta).unwrap()
            }),
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_1.clone(), split_id_2.clone()],
            index_checkpoint_delta_json_opt: Some({
                let offsets = 30..31;
                let checkpoint_delta = IndexCheckpointDelta::for_test(&source_id, offsets);
                serde_json::to_string(&checkpoint_delta).unwrap()
            }),
            ..Default::default()
        };
        let error = metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MetastoreError::FailedPrecondition {
                entity: EntityKind::CheckpointDelta { .. },
                ..
            }
        ));

        cleanup_index(&mut metastore, index_uid).await;
    }
}

pub async fn test_metastore_publish_splits_concurrency<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest + Clone,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-publish-concurrency");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);
    let source_id = format!("{index_id}--source");

    let source_config = SourceConfig::for_test(&source_id, SourceParams::void());
    let create_index_request =
        CreateIndexRequest::try_from_index_and_source_configs(&index_config, &[source_config])
            .unwrap();

    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    let mut join_handles = Vec::with_capacity(10);

    for partition_id in 0..10 {
        let metastore_clone = metastore.clone();
        let index_id = index_id.clone();
        let source_id = source_id.clone();

        let join_handle = tokio::spawn({
            let index_uid = index_uid.clone();
            async move {
                let split_id = format!("{index_id}--split-{partition_id}");
                let split_metadata = SplitMetadata {
                    split_id: split_id.clone(),
                    index_uid: index_uid.clone(),
                    ..Default::default()
                };
                let stage_splits_request =
                    StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata)
                        .unwrap();
                metastore_clone
                    .stage_splits(stage_splits_request)
                    .await
                    .unwrap();
                let source_delta = SourceCheckpointDelta::from_partition_delta(
                    PartitionId::from(partition_id as u64),
                    Position::Beginning,
                    Position::offset(partition_id as u64),
                )
                .unwrap();
                let checkpoint_delta = IndexCheckpointDelta {
                    source_id,
                    source_delta,
                };
                let publish_splits_request = PublishSplitsRequest {
                    index_uid: Some(index_uid.clone()),
                    staged_split_ids: vec![split_id.clone()],
                    index_checkpoint_delta_json_opt: Some(
                        serde_json::to_string(&checkpoint_delta).unwrap(),
                    ),
                    ..Default::default()
                };
                metastore_clone
                    .publish_splits(publish_splits_request)
                    .await
                    .unwrap();
            }
        });
        join_handles.push(join_handle);
    }
    try_join_all(join_handles).await.unwrap();

    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap();
    let source_checkpoint = index_metadata
        .checkpoint
        .source_checkpoint(&source_id)
        .unwrap();

    assert_eq!(source_checkpoint.num_partitions(), 10);

    cleanup_index(&mut metastore, index_uid).await
}

pub async fn test_metastore_replace_splits<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

    let index_id = append_random_suffix("test-replace-splits");
    let index_uid = IndexUid::new_with_random_ulid(&index_id);
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let split_id_1 = format!("{index_id}--split-1");
    let split_metadata_1 = SplitMetadata {
        split_id: split_id_1.clone(),
        index_uid: index_uid.clone(),
        time_range: None,
        create_timestamp: current_timestamp,
        ..Default::default()
    };

    let split_id_2 = format!("{index_id}--split-2");
    let split_metadata_2 = SplitMetadata {
        split_id: split_id_2.clone(),
        index_uid: index_uid.clone(),
        time_range: None,
        create_timestamp: current_timestamp,
        ..Default::default()
    };

    let split_id_3 = format!("{index_id}--split-3");
    let split_metadata_3 = SplitMetadata {
        split_id: split_id_3.clone(),
        index_uid: index_uid.clone(),
        time_range: None,
        create_timestamp: current_timestamp,
        ..Default::default()
    };

    // Replace splits on a non-existent index
    {
        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(IndexUid::new_with_random_ulid("index-not-found")),
            staged_split_ids: vec!["split-not-found-1".to_string()],
            replaced_split_ids: vec!["split-not-found-2".to_string()],
            ..Default::default()
        };
        let error = metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MetastoreError::NotFound(EntityKind::Index { .. })
        ));
    }

    // Replace a non-existent split on an index
    {
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec!["split-not-found-1".to_string()],
            replaced_split_ids: vec!["split-not-found-2".to_string()],
            ..Default::default()
        };
        // TODO source id
        let error = metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MetastoreError::NotFound(EntityKind::Splits { .. })
        ));

        cleanup_index(&mut metastore, index_uid).await;
    }

    // Replace a publish split with a non existing split
    {
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata_1)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_1.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        // TODO Source id
        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_2.clone()],
            replaced_split_ids: vec![split_id_1.clone()],
            ..Default::default()
        };
        let error = metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MetastoreError::NotFound(EntityKind::Splits { .. })
        ));

        cleanup_index(&mut metastore, index_uid).await;
    }

    // Replace a publish split with a deleted split
    {
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid.clone(),
            [split_metadata_1.clone(), split_metadata_2.clone()],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_1.clone(), split_id_2.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        let mark_splits_for_deletion_request =
            MarkSplitsForDeletionRequest::new(index_uid.clone(), vec![split_id_2.clone()]);
        metastore
            .mark_splits_for_deletion(mark_splits_for_deletion_request)
            .await
            .unwrap();

        // TODO source_id
        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_2.clone()],
            replaced_split_ids: vec![split_id_1.clone()],
            ..Default::default()
        };
        let error = metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MetastoreError::FailedPrecondition {
                entity: EntityKind::Splits { .. },
                ..
            }
        ));

        cleanup_index(&mut metastore, index_uid).await;
    }

    // Replace a publish split with mixed splits
    {
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata_1)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_1.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata_2)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_2.clone(), split_id_3.clone()],
            replaced_split_ids: vec![split_id_1.clone()],
            ..Default::default()
        };
        let error = metastore
            .publish_splits(publish_splits_request) // TODO source id
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MetastoreError::NotFound(EntityKind::Splits { .. })
        ));

        cleanup_index(&mut metastore, index_uid).await;
    }

    // Replace a deleted split with a new split
    {
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata_1)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_1.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        let mark_splits_for_deletion_request =
            MarkSplitsForDeletionRequest::new(index_uid.clone(), vec![split_id_1.clone()]);
        metastore
            .mark_splits_for_deletion(mark_splits_for_deletion_request)
            .await
            .unwrap();

        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata_2)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_2.clone()],
            replaced_split_ids: vec![split_id_1.clone()],
            ..Default::default()
        };
        let error = metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap_err();
        assert!(
            matches!(error, MetastoreError::FailedPrecondition { entity: EntityKind::Splits { split_ids }, .. } if split_ids == [split_id_1.clone()])
        );

        cleanup_index(&mut metastore, index_uid).await;
    }

    // Replace a publish split with staged splits
    {
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata_1)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_1.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid.clone(),
            [split_metadata_2.clone(), split_metadata_3.clone()],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        // TODO Source id
        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_2.clone(), split_id_3.clone()],
            replaced_split_ids: vec![split_id_1.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        cleanup_index(&mut metastore, index_uid).await;
    }
}

pub async fn test_metastore_mark_splits_for_deletion<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

    let index_id = append_random_suffix("test-mark-splits-for-deletion");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);
    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();

    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    let mark_splits_for_deletion_request = MarkSplitsForDeletionRequest::new(
        "index-not-found:00000000000000000000000000"
            .parse()
            .unwrap(),
        Vec::new(),
    );
    let error = metastore
        .mark_splits_for_deletion(mark_splits_for_deletion_request)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    let mark_splits_for_deletion_request =
        MarkSplitsForDeletionRequest::new(index_uid.clone(), vec!["split-not-found".to_string()]);
    metastore
        .mark_splits_for_deletion(mark_splits_for_deletion_request)
        .await
        .unwrap();

    let split_id_1 = format!("{index_id}--split-1");
    let split_metadata_1 = SplitMetadata {
        split_id: split_id_1.clone(),
        index_uid: index_uid.clone(),
        create_timestamp: current_timestamp,
        ..Default::default()
    };
    let stage_splits_request =
        StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata_1).unwrap();
    metastore.stage_splits(stage_splits_request).await.unwrap();

    let split_id_2 = format!("{index_id}--split-2");
    let split_metadata_2 = SplitMetadata {
        split_id: split_id_2.clone(),
        index_uid: index_uid.clone(),
        create_timestamp: current_timestamp,
        ..Default::default()
    };
    let stage_splits_request =
        StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata_2).unwrap();
    metastore.stage_splits(stage_splits_request).await.unwrap();
    let publish_splits_request = PublishSplitsRequest {
        index_uid: Some(index_uid.clone()),
        staged_split_ids: vec![split_id_2.clone()],
        ..Default::default()
    };
    metastore
        .publish_splits(publish_splits_request)
        .await
        .unwrap();

    let split_id_3 = format!("{index_id}--split-3");
    let split_metadata_3 = SplitMetadata {
        split_id: split_id_3.clone(),
        index_uid: index_uid.clone(),
        create_timestamp: current_timestamp,
        ..Default::default()
    };
    let stage_splits_request =
        StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata_3).unwrap();
    metastore.stage_splits(stage_splits_request).await.unwrap();
    let publish_splits_request = PublishSplitsRequest {
        index_uid: Some(index_uid.clone()),
        staged_split_ids: vec![split_id_3.clone()],
        ..Default::default()
    };
    metastore
        .publish_splits(publish_splits_request)
        .await
        .unwrap();

    // Sleep for 1s so we can observe the timestamp update.
    sleep(Duration::from_secs(1)).await;

    let mark_splits_for_deletion_request =
        MarkSplitsForDeletionRequest::new(index_uid.clone(), vec![split_id_3.clone()]);
    metastore
        .mark_splits_for_deletion(mark_splits_for_deletion_request)
        .await
        .unwrap();

    let list_splits_request = ListSplitsRequest::try_from_list_splits_query(
        &ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::MarkedForDeletion),
    )
    .unwrap();
    let marked_splits = metastore
        .list_splits(list_splits_request)
        .await
        .unwrap()
        .collect_splits()
        .await
        .unwrap();

    assert_eq!(marked_splits.len(), 1);
    assert_eq!(marked_splits[0].split_id(), split_id_3);

    let split_3_update_timestamp = marked_splits[0].update_timestamp;
    assert!(current_timestamp < split_3_update_timestamp);

    // Sleep for 1s so we can observe the timestamp update.
    sleep(Duration::from_secs(1)).await;

    let mark_splits_for_deletion_request = MarkSplitsForDeletionRequest::new(
        index_uid.clone(),
        vec![
            split_id_1.clone(),
            split_id_2.clone(),
            split_id_3.clone(),
            "split-not-found".to_string(),
        ],
    );
    metastore
        .mark_splits_for_deletion(mark_splits_for_deletion_request)
        .await
        .unwrap();

    let list_splits_request = ListSplitsRequest::try_from_list_splits_query(
        &ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::MarkedForDeletion),
    )
    .unwrap();
    let mut marked_splits = metastore
        .list_splits(list_splits_request)
        .await
        .unwrap()
        .collect_splits()
        .await
        .unwrap();

    marked_splits.sort_by_key(|split| split.split_id().to_string());

    assert_eq!(marked_splits.len(), 3);

    assert_eq!(marked_splits[0].split_id(), split_id_1);
    assert!(current_timestamp + 2 <= marked_splits[0].update_timestamp);

    assert_eq!(marked_splits[1].split_id(), split_id_2);
    assert!(current_timestamp + 2 <= marked_splits[1].update_timestamp);

    assert_eq!(marked_splits[2].split_id(), split_id_3);
    assert_eq!(marked_splits[2].update_timestamp, split_3_update_timestamp);

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_delete_splits<MetastoreToTest: MetastoreServiceExt + DefaultForTest>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-delete-splits");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    let delete_splits_request = DeleteSplitsRequest {
        index_uid: Some(IndexUid::new_with_random_ulid("index-not-found")),
        split_ids: Vec::new(),
    };
    let error = metastore
        .delete_splits(delete_splits_request)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    let index_not_existing_uid = IndexUid::new_with_random_ulid(&index_id);
    // Check error if index does not exist.
    let delete_splits_request = DeleteSplitsRequest {
        index_uid: Some(index_not_existing_uid),
        split_ids: Vec::new(),
    };
    let error = metastore
        .delete_splits(delete_splits_request)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    let delete_splits_request = DeleteSplitsRequest {
        index_uid: Some(index_uid.clone()),
        split_ids: vec!["split-not-found".to_string()],
    };
    metastore
        .delete_splits(delete_splits_request)
        .await
        .unwrap();

    let split_id_1 = format!("{index_id}--split-1");
    let split_metadata_1 = SplitMetadata {
        split_id: split_id_1.clone(),
        index_uid: index_uid.clone(),
        ..Default::default()
    };
    let stage_splits_request =
        StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata_1).unwrap();
    metastore.stage_splits(stage_splits_request).await.unwrap();
    let publish_splits_request = PublishSplitsRequest {
        index_uid: Some(index_uid.clone()),
        staged_split_ids: vec![split_id_1.clone()],
        ..Default::default()
    };
    metastore
        .publish_splits(publish_splits_request)
        .await
        .unwrap();

    let split_id_2 = format!("{index_id}--split-2");
    let split_metadata_2 = SplitMetadata {
        split_id: split_id_2.clone(),
        index_uid: index_uid.clone(),
        ..Default::default()
    };
    let stage_splits_request =
        StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata_2).unwrap();
    metastore.stage_splits(stage_splits_request).await.unwrap();

    let delete_splits_request = DeleteSplitsRequest {
        index_uid: Some(index_uid.clone()),
        split_ids: vec![split_id_1.clone(), split_id_2.clone()],
    };
    let error = metastore
        .delete_splits(delete_splits_request)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        MetastoreError::FailedPrecondition {
            entity: EntityKind::Splits { .. },
            ..
        }
    ));

    assert_eq!(
        metastore
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap()
            .len(),
        2
    );

    let mark_splits_for_deletion_request = MarkSplitsForDeletionRequest::new(
        index_uid.clone(),
        vec![split_id_1.clone(), split_id_2.clone()],
    );
    metastore
        .mark_splits_for_deletion(mark_splits_for_deletion_request)
        .await
        .unwrap();

    let delete_splits_request = DeleteSplitsRequest {
        index_uid: Some(index_uid.clone()),
        split_ids: vec![
            split_id_1.clone(),
            split_id_2.clone(),
            "split-not-found".to_string(),
        ],
    };
    metastore
        .delete_splits(delete_splits_request)
        .await
        .unwrap();

    assert_eq!(
        metastore
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap()
            .len(),
        0
    );

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_split_update_timestamp<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let mut current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

    let index_id = append_random_suffix("split-update-timestamp");
    let index_uid = IndexUid::new_with_random_ulid(&index_id);
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let source_id = format!("{index_id}--source");
    let source_config = SourceConfig::for_test(&source_id, SourceParams::void());

    let split_id = format!("{index_id}--split");
    let split_metadata = SplitMetadata {
        split_id: split_id.clone(),
        index_uid: index_uid.clone(),
        create_timestamp: current_timestamp,
        ..Default::default()
    };

    // Create an index
    let create_index_request =
        CreateIndexRequest::try_from_index_and_source_configs(&index_config, &[source_config])
            .unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    // wait for 1s, stage split & check `update_timestamp`
    sleep(Duration::from_secs(1)).await;
    let stage_splits_request =
        StageSplitsRequest::try_from_splits_metadata(index_uid.clone(), [split_metadata.clone()])
            .unwrap();
    metastore.stage_splits(stage_splits_request).await.unwrap();

    sleep(Duration::from_secs(1)).await;
    let split_meta = metastore
        .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
        .await
        .unwrap()
        .collect_splits()
        .await
        .unwrap()[0]
        .clone();
    assert!(split_meta.update_timestamp > current_timestamp);
    assert!(split_meta.publish_timestamp.is_none());

    current_timestamp = split_meta.update_timestamp;

    // wait for 1s, publish split & check `update_timestamp`
    sleep(Duration::from_secs(1)).await;
    let publish_splits_request = PublishSplitsRequest {
        index_uid: Some(index_uid.clone()),
        staged_split_ids: vec![split_id.clone()],
        index_checkpoint_delta_json_opt: Some({
            let offsets = 0..5;
            let checkpoint_delta = IndexCheckpointDelta::for_test(&source_id, offsets);
            serde_json::to_string(&checkpoint_delta).unwrap()
        }),
        ..Default::default()
    };
    metastore
        .publish_splits(publish_splits_request)
        .await
        .unwrap();
    let split_meta = metastore
        .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
        .await
        .unwrap()
        .collect_splits()
        .await
        .unwrap()[0]
        .clone();
    assert!(split_meta.update_timestamp > current_timestamp);
    assert_eq!(
        split_meta.publish_timestamp,
        Some(split_meta.update_timestamp)
    );
    current_timestamp = split_meta.update_timestamp;

    // wait for 1s, mark split for deletion & check `update_timestamp`
    sleep(Duration::from_secs(1)).await;
    let mark_splits_for_deletion_request =
        MarkSplitsForDeletionRequest::new(index_uid.clone(), vec![split_id.clone()]);
    metastore
        .mark_splits_for_deletion(mark_splits_for_deletion_request)
        .await
        .unwrap();
    let split_meta = metastore
        .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
        .await
        .unwrap()
        .collect_splits()
        .await
        .unwrap()[0]
        .clone();
    assert!(split_meta.update_timestamp > current_timestamp);
    assert!(split_meta.publish_timestamp.is_some());

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_stage_splits<MetastoreToTest: MetastoreServiceExt + DefaultForTest>() {
    let mut metastore = MetastoreToTest::default_for_test().await;
    let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();
    let index_id = append_random_suffix("test-stage-splits");
    let index_uid = IndexUid::new_with_random_ulid(&index_id);
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let split_id_1 = format!("{index_id}--split-1");
    let split_metadata_1 = SplitMetadata {
        split_id: split_id_1.clone(),
        index_uid: index_uid.clone(),
        create_timestamp: current_timestamp,
        delete_opstamp: 20,
        node_id: "node-1".to_string(),
        ..Default::default()
    };
    let split_id_2 = format!("{index_id}--split-2");
    let split_metadata_2 = SplitMetadata {
        split_id: split_id_2.clone(),
        index_uid: index_uid.clone(),
        create_timestamp: current_timestamp,
        delete_opstamp: 10,
        node_id: "node-2".to_string(),
        ..Default::default()
    };

    // Stage a splits on a non-existent index
    let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
        IndexUid::new_with_random_ulid("index-not-found"),
        [split_metadata_1.clone()],
    )
    .unwrap();
    let error = metastore
        .stage_splits(stage_splits_request)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    // Stage a split on an index
    let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
        index_uid.clone(),
        [split_metadata_1.clone(), split_metadata_2.clone()],
    )
    .unwrap();
    metastore.stage_splits(stage_splits_request).await.unwrap();

    let query = ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Staged);
    let mut splits = metastore
        .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
        .await
        .unwrap()
        .collect_splits()
        .await
        .unwrap();

    assert_eq!(splits.len(), 2);
    splits.sort_unstable_by(|left, right| left.split_id().cmp(right.split_id()));

    assert_eq!(splits[0].split_id(), &split_id_1);
    assert_eq!(splits[0].split_metadata.node_id, "node-1");

    assert_eq!(splits[1].split_id(), &split_id_2);
    assert_eq!(splits[1].split_metadata.node_id, "node-2");

    // Stage a existent-staged-split on an index
    let stage_splits_request =
        StageSplitsRequest::try_from_splits_metadata(index_uid.clone(), [split_metadata_1.clone()])
            .unwrap();
    metastore
        .stage_splits(stage_splits_request)
        .await
        .expect("Pre-existing staged splits should be updated.");

    let publish_splits_request = PublishSplitsRequest {
        index_uid: Some(index_uid.clone()),
        staged_split_ids: vec![split_id_1.clone(), split_id_2.clone()],
        ..Default::default()
    };
    metastore
        .publish_splits(publish_splits_request)
        .await
        .unwrap();
    let stage_splits_request =
        StageSplitsRequest::try_from_splits_metadata(index_uid.clone(), [split_metadata_1.clone()])
            .unwrap();
    let error = metastore
        .stage_splits(stage_splits_request)
        .await
        .expect_err("Metastore should not allow splits which are not `Staged` to be overwritten.");
    assert!(matches!(
        error,
        MetastoreError::FailedPrecondition {
            entity: EntityKind::Splits { .. },
            ..
        }
    ),);

    let mark_splits_for_deletion_request =
        MarkSplitsForDeletionRequest::new(index_uid.clone(), vec![split_id_2.clone()]);
    metastore
        .mark_splits_for_deletion(mark_splits_for_deletion_request)
        .await
        .unwrap();
    let stage_splits_request =
        StageSplitsRequest::try_from_splits_metadata(index_uid.clone(), [split_metadata_2.clone()])
            .unwrap();
    let error = metastore
        .stage_splits(stage_splits_request)
        .await
        .expect_err("Metastore should not allow splits which are not `Staged` to be overwritten.");
    assert!(matches!(
        error,
        MetastoreError::FailedPrecondition {
            entity: EntityKind::Splits { .. },
            ..
        }
    ),);

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_update_splits_delete_opstamp<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;
    let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();
    let index_id = append_random_suffix("update-splits-delete-opstamp");
    let index_uid = IndexUid::new_with_random_ulid(&index_id);
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let split_id_1 = format!("{index_id}--split-1");
    let split_metadata_1 = SplitMetadata {
        split_id: split_id_1.clone(),
        index_uid: index_uid.clone(),
        create_timestamp: current_timestamp,
        delete_opstamp: 20,
        ..Default::default()
    };
    let split_id_2 = format!("{index_id}--split-2");
    let split_metadata_2 = SplitMetadata {
        split_id: split_id_2.clone(),
        index_uid: index_uid.clone(),
        create_timestamp: current_timestamp,
        delete_opstamp: 10,
        ..Default::default()
    };
    let split_id_3 = format!("{index_id}--split-3");
    let split_metadata_3 = SplitMetadata {
        split_id: split_id_3.clone(),
        index_uid: index_uid.clone(),
        create_timestamp: current_timestamp,
        delete_opstamp: 0,
        ..Default::default()
    };

    {
        info!("update splits delete opstamp on a non-existent index");
        let update_splits_delete_opstamp_request = UpdateSplitsDeleteOpstampRequest {
            index_uid: Some(IndexUid::new_with_random_ulid("index-not-found")),
            split_ids: vec![split_id_1.clone()],
            delete_opstamp: 10,
        };
        let metastore_err = metastore
            .update_splits_delete_opstamp(update_splits_delete_opstamp_request)
            .await
            .unwrap_err();
        error!(err=?metastore_err);
        assert!(matches!(
            metastore_err,
            MetastoreError::NotFound(EntityKind::Index { .. })
        ));
    }

    {
        info!("update splits delete opstamp on an index");
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid.clone(),
            [split_metadata_1, split_metadata_2, split_metadata_3],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();
        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id_1.clone(), split_id_2.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        let list_stale_splits_request = ListStaleSplitsRequest {
            index_uid: Some(index_uid.clone()),
            delete_opstamp: 100,
            num_splits: 2,
        };
        let splits = metastore
            .list_stale_splits(list_stale_splits_request)
            .await
            .unwrap()
            .deserialize_splits()
            .await
            .unwrap();
        assert_eq!(splits.len(), 2);

        let update_splits_delete_opstamp_request = UpdateSplitsDeleteOpstampRequest {
            index_uid: Some(index_uid.clone()),
            split_ids: vec![split_id_1.clone(), split_id_2.clone()],
            delete_opstamp: 100,
        };
        metastore
            .update_splits_delete_opstamp(update_splits_delete_opstamp_request)
            .await
            .unwrap();

        let list_stale_splits_request = ListStaleSplitsRequest {
            index_uid: Some(index_uid.clone()),
            delete_opstamp: 100,
            num_splits: 2,
        };
        let splits = metastore
            .list_stale_splits(list_stale_splits_request)
            .await
            .unwrap()
            .deserialize_splits()
            .await
            .unwrap();
        assert_eq!(splits.len(), 0);

        let list_stale_splits_request = ListStaleSplitsRequest {
            index_uid: Some(index_uid.clone()),
            delete_opstamp: 200,
            num_splits: 2,
        };
        let splits = metastore
            .list_stale_splits(list_stale_splits_request)
            .await
            .unwrap()
            .deserialize_splits()
            .await
            .unwrap();
        assert_eq!(splits.len(), 2);
        assert_eq!(splits[0].split_metadata.delete_opstamp, 100);
        assert_eq!(splits[1].split_metadata.delete_opstamp, 100);

        cleanup_index(&mut metastore, index_uid).await;
    }
}
