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

use std::num::NonZeroUsize;

use quickwit_common::rand::append_random_suffix;
use quickwit_config::{IndexConfig, SourceConfig, SourceInputFormat, SourceParams};
use quickwit_proto::metastore::{
    AddSourceRequest, CreateIndexRequest, DeleteSourceRequest, EntityKind, IndexMetadataRequest,
    MetastoreError, PublishSplitsRequest, ResetSourceCheckpointRequest, SourceType,
    StageSplitsRequest, ToggleSourceRequest,
};
use quickwit_proto::types::IndexUid;

use super::DefaultForTest;
use crate::checkpoint::SourceCheckpoint;
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

    let create_index_request =
        CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid
        .into();

    let source_id = format!("{index_id}--source");

    let source = SourceConfig {
        source_id: source_id.to_string(),
        max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
        desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
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
        AddSourceRequest::try_from_source_config(index_uid.clone(), source.clone()).unwrap();
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
                AddSourceRequest::try_from_source_config(index_uid.clone(), source.clone())
                    .unwrap()
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
                    source.clone()
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
                    source
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

    let create_index_request =
        CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid
        .into();

    let source_id = format!("{index_id}--source");
    let source = SourceConfig {
        source_id: source_id.to_string(),
        max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
        desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
        enabled: true,
        source_params: SourceParams::void(),
        transform_config: None,
        input_format: SourceInputFormat::Json,
    };
    let add_source_request =
        AddSourceRequest::try_from_source_config(index_uid.clone(), source.clone()).unwrap();
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
        max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
        desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
        enabled: true,
        source_params: SourceParams::void(),
        transform_config: None,
        input_format: SourceInputFormat::Json,
    };

    let index_config = IndexConfig::for_test(&index_id, index_uri.as_str());

    let create_index_request =
        CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid
        .into();
    assert!(matches!(
        metastore
            .add_source(
                AddSourceRequest::try_from_source_config(
                    IndexUid::new_with_random_ulid("index-not-found"),
                    source.clone()
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
                    source.clone()
                )
                .unwrap()
            )
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    metastore
        .add_source(
            AddSourceRequest::try_from_source_config(index_uid.clone(), source.clone()).unwrap(),
        )
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
                index_uid: IndexUid::new_with_random_ulid("index-not-found").to_string(),
                source_id: source_id.to_string()
            })
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));
    assert!(matches!(
        metastore
            .delete_source(DeleteSourceRequest {
                index_uid: IndexUid::new_with_random_ulid(&index_id).to_string(),
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

    let create_index_request =
        CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid
        .into();

    let source_ids: Vec<String> = (0..2).map(|i| format!("{index_id}--source-{i}")).collect();
    let split_ids: Vec<String> = (0..2).map(|i| format!("{index_id}--split-{i}")).collect();

    for (source_id, split_id) in source_ids.iter().zip(split_ids.iter()) {
        let source = SourceConfig {
            source_id: source_id.clone(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
            desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::void(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        metastore
            .add_source(
                AddSourceRequest::try_from_source_config(index_uid.clone(), source.clone())
                    .unwrap(),
            )
            .await
            .unwrap();

        let split_metadata = SplitMetadata {
            split_id: split_id.clone(),
            index_uid: index_uid.clone(),
            ..Default::default()
        };
        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), split_metadata).unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();
        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().to_string(),
            staged_split_ids: vec![split_id.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();
    }
    assert!(!metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap()
        .checkpoint
        .is_empty());

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
    assert!(index_metadata
        .checkpoint
        .source_checkpoint(&source_ids[0])
        .is_none());

    assert!(index_metadata
        .checkpoint
        .source_checkpoint(&source_ids[1])
        .is_some());

    assert!(matches!(
        metastore
            .reset_source_checkpoint(ResetSourceCheckpointRequest {
                index_uid: IndexUid::new_with_random_ulid("index-not-found").to_string(),
                source_id: source_ids[1].clone(),
            })
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    assert!(matches!(
        metastore
            .reset_source_checkpoint(ResetSourceCheckpointRequest {
                index_uid: IndexUid::new_with_random_ulid(&index_id).to_string(),
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

    assert!(metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap()
        .checkpoint
        .is_empty());

    cleanup_index(&mut metastore, index_uid).await;
}
