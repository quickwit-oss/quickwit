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

use std::num::NonZeroUsize;

use quickwit_common::rand::append_random_suffix;
use quickwit_config::{
    IndexConfig, SourceConfig, SourceInputFormat, SourceParams, TransformConfig,
};
use quickwit_proto::metastore::{
    AddSourceRequest, CreateIndexRequest, DeleteSourceRequest, EntityKind, IndexMetadataRequest,
    MetastoreError, PublishSplitsRequest, ResetSourceCheckpointRequest, SourceType,
    StageSplitsRequest, ToggleSourceRequest, UpdateSourceRequest,
};
use quickwit_proto::types::IndexUid;

use super::DefaultForTest;
use crate::checkpoint::SourceCheckpoint;
use crate::metastore::UpdateSourceRequestExt;
use crate::tests::cleanup_index;
use crate::{
    AddSourceRequestExt, CreateIndexRequestExt, IndexMetadataResponseExt, MetastoreServiceExt,
    SplitMetadata, StageSplitsRequestExt,
};

pub async fn test_metastore_add_source<MetastoreToTest: MetastoreServiceExt + DefaultForTest>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-add-source");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    let source_id = format!("{index_id}--source");

    let source = SourceConfig {
        source_id: source_id.to_string(),
        num_pipelines: NonZeroUsize::MIN,
        enabled: true,
        source_params: SourceParams::void(),
        transform_config: None,
        input_format: SourceInputFormat::Json,
    };

    assert_eq!(
        metastore
            .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap()
            .checkpoint
            .source_checkpoint(&source_id),
        None
    );

    let add_source_request =
        AddSourceRequest::try_from_source_config(index_uid.clone(), &source).unwrap();
    metastore.add_source(add_source_request).await.unwrap();

    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap();

    let sources = &index_metadata.sources;
    assert_eq!(sources.len(), 1);
    assert!(sources.contains_key(&source_id));
    assert_eq!(sources.get(&source_id).unwrap().source_id, source_id);
    assert_eq!(
        sources.get(&source_id).unwrap().source_type(),
        SourceType::Void
    );
    assert_eq!(
        index_metadata.checkpoint.source_checkpoint(&source_id),
        Some(&SourceCheckpoint::default())
    );

    assert!(matches!(
        metastore
            .add_source(
                AddSourceRequest::try_from_source_config(index_uid.clone(), &source).unwrap()
            )
            .await
            .unwrap_err(),
        MetastoreError::AlreadyExists(EntityKind::Source { .. })
    ));
    assert!(matches!(
        metastore
            .add_source(
                AddSourceRequest::try_from_source_config(
                    IndexUid::new_with_random_ulid("index-not-found"),
                    &source
                )
                .unwrap()
            )
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));
    assert!(matches!(
        metastore
            .add_source(
                AddSourceRequest::try_from_source_config(
                    IndexUid::new_with_random_ulid(&index_id),
                    &source
                )
                .unwrap()
            )
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));
    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_update_source<MetastoreToTest: MetastoreServiceExt + DefaultForTest>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-add-source");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    let source_id = format!("{index_id}--source");

    let mut source = SourceConfig {
        source_id: source_id.to_string(),
        num_pipelines: NonZeroUsize::MIN,
        enabled: true,
        source_params: SourceParams::void(),
        transform_config: None,
        input_format: SourceInputFormat::Json,
    };

    assert_eq!(
        metastore
            .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap()
            .checkpoint
            .source_checkpoint(&source_id),
        None
    );

    let add_source_request =
        AddSourceRequest::try_from_source_config(index_uid.clone(), &source).unwrap();
    metastore.add_source(add_source_request).await.unwrap();

    source.transform_config = Some(TransformConfig::new("del(.username)".to_string(), None));

    // Update the source twice with the same value to validate indempotency
    for _ in 0..2 {
        let update_source_request =
            UpdateSourceRequest::try_from_source_config(index_uid.clone(), &source).unwrap();
        metastore
            .update_source(update_source_request)
            .await
            .unwrap();

        let index_metadata = metastore
            .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap();

        let sources = &index_metadata.sources;
        assert_eq!(sources.len(), 1);
        assert!(sources.contains_key(&source_id));
        assert_eq!(sources.get(&source_id).unwrap().source_id, source_id);
        assert_eq!(
            sources.get(&source_id).unwrap().source_type(),
            SourceType::Void
        );
        assert_eq!(
            sources.get(&source_id).unwrap().transform_config,
            Some(TransformConfig::new("del(.username)".to_string(), None))
        );
        assert_eq!(
            index_metadata.checkpoint.source_checkpoint(&source_id),
            Some(&SourceCheckpoint::default())
        );
    }

    source.source_id = "unknown-src-id".to_string();
    assert!(matches!(
        metastore
            .update_source(
                UpdateSourceRequest::try_from_source_config(index_uid.clone(), &source).unwrap()
            )
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Source { .. })
    ));
    source.source_id = source_id;
    assert!(matches!(
        metastore
            .add_source(
                AddSourceRequest::try_from_source_config(
                    IndexUid::new_with_random_ulid("index-not-found"),
                    &source
                )
                .unwrap()
            )
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_toggle_source<MetastoreToTest: MetastoreServiceExt + DefaultForTest>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-toggle-source");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    let source_id = format!("{index_id}--source");
    let source = SourceConfig {
        source_id: source_id.to_string(),
        num_pipelines: NonZeroUsize::MIN,
        enabled: true,
        source_params: SourceParams::void(),
        transform_config: None,
        input_format: SourceInputFormat::Json,
    };
    let add_source_request =
        AddSourceRequest::try_from_source_config(index_uid.clone(), &source).unwrap();
    metastore.add_source(add_source_request).await.unwrap();
    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap();
    let source = index_metadata.sources.get(&source_id).unwrap();
    assert_eq!(source.enabled, true);

    // Disable source.
    metastore
        .toggle_source(ToggleSourceRequest {
            index_uid: index_uid.clone().into(),
            source_id: source.source_id.clone(),
            enable: false,
        })
        .await
        .unwrap();
    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap();
    let source = index_metadata.sources.get(&source_id).unwrap();
    assert_eq!(source.enabled, false);

    // Enable source.
    metastore
        .toggle_source(ToggleSourceRequest {
            index_uid: index_uid.clone().into(),
            source_id: source.source_id.clone(),
            enable: true,
        })
        .await
        .unwrap();
    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap();
    let source = index_metadata.sources.get(&source_id).unwrap();
    assert_eq!(source.enabled, true);

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_delete_source<MetastoreToTest: MetastoreServiceExt + DefaultForTest>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-delete-source");
    let index_uri = format!("ram:///indexes/{index_id}");
    let source_id = format!("{index_id}--source");

    let source = SourceConfig {
        source_id: source_id.to_string(),
        num_pipelines: NonZeroUsize::MIN,
        enabled: true,
        source_params: SourceParams::void(),
        transform_config: None,
        input_format: SourceInputFormat::Json,
    };

    let index_config = IndexConfig::for_test(&index_id, index_uri.as_str());

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();
    assert!(matches!(
        metastore
            .add_source(
                AddSourceRequest::try_from_source_config(
                    IndexUid::new_with_random_ulid("index-not-found"),
                    &source
                )
                .unwrap()
            )
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));
    assert!(matches!(
        metastore
            .add_source(
                AddSourceRequest::try_from_source_config(
                    IndexUid::new_with_random_ulid(&index_id),
                    &source
                )
                .unwrap()
            )
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    metastore
        .add_source(AddSourceRequest::try_from_source_config(index_uid.clone(), &source).unwrap())
        .await
        .unwrap();
    metastore
        .delete_source(DeleteSourceRequest {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
        })
        .await
        .unwrap();

    let sources = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap()
        .sources;
    assert!(sources.is_empty());

    assert!(matches!(
        metastore
            .delete_source(DeleteSourceRequest {
                index_uid: index_uid.clone().into(),
                source_id: source_id.to_string()
            })
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Source { .. })
    ));
    assert!(matches!(
        metastore
            .delete_source(DeleteSourceRequest {
                index_uid: Some(IndexUid::new_with_random_ulid("index-not-found")),
                source_id: source_id.to_string()
            })
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));
    assert!(matches!(
        metastore
            .delete_source(DeleteSourceRequest {
                index_uid: Some(IndexUid::new_with_random_ulid(&index_id)),
                source_id: source_id.to_string()
            })
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_reset_checkpoint<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-reset-checkpoint");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    let source_ids: Vec<String> = (0..2).map(|i| format!("{index_id}--source-{i}")).collect();
    let split_ids: Vec<String> = (0..2).map(|i| format!("{index_id}--split-{i}")).collect();

    for (source_id, split_id) in source_ids.iter().zip(split_ids.iter()) {
        let source = SourceConfig {
            source_id: source_id.clone(),
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::void(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        metastore
            .add_source(
                AddSourceRequest::try_from_source_config(index_uid.clone(), &source).unwrap(),
            )
            .await
            .unwrap();

        let split_metadata = SplitMetadata {
            split_id: split_id.clone(),
            index_uid: index_uid.clone(),
            ..Default::default()
        };
        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();
        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();
    }
    assert!(
        !metastore
            .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap()
            .checkpoint
            .is_empty()
    );

    metastore
        .reset_source_checkpoint(ResetSourceCheckpointRequest {
            index_uid: index_uid.clone().into(),
            source_id: source_ids[0].clone(),
        })
        .await
        .unwrap();

    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap();
    assert!(
        index_metadata
            .checkpoint
            .source_checkpoint(&source_ids[0])
            .is_none()
    );

    assert!(
        index_metadata
            .checkpoint
            .source_checkpoint(&source_ids[1])
            .is_some()
    );

    assert!(matches!(
        metastore
            .reset_source_checkpoint(ResetSourceCheckpointRequest {
                index_uid: Some(IndexUid::new_with_random_ulid("index-not-found")),
                source_id: source_ids[1].clone(),
            })
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    assert!(matches!(
        metastore
            .reset_source_checkpoint(ResetSourceCheckpointRequest {
                index_uid: Some(IndexUid::new_with_random_ulid(&index_id)),
                source_id: source_ids[1].to_string(),
            })
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    metastore
        .reset_source_checkpoint(ResetSourceCheckpointRequest {
            index_uid: index_uid.clone().into(),
            source_id: source_ids[1].to_string(),
        })
        .await
        .unwrap();

    assert!(
        metastore
            .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap()
            .checkpoint
            .is_empty()
    );

    cleanup_index(&mut metastore, index_uid).await;
}
