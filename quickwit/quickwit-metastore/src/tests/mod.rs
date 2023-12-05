// Copyright (C) 2023 Quickwit, Inc.
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

use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::try_join_all;
use itertools::Itertools;
use quickwit_common::rand::append_random_suffix;
use quickwit_config::{IndexConfig, SourceConfig, SourceInputFormat, SourceParams};
use quickwit_doc_mapper::tag_pruning::{no_tag, tag, TagFilterAst};
use quickwit_proto::metastore::{
    AddSourceRequest, CreateIndexRequest, DeleteIndexRequest, DeleteQuery, DeleteSourceRequest,
    DeleteSplitsRequest, EntityKind, IndexMetadataRequest, LastDeleteOpstampRequest,
    ListDeleteTasksRequest, ListIndexesMetadataRequest, ListSplitsRequest, ListStaleSplitsRequest,
    MarkSplitsForDeletionRequest, MetastoreError, MetastoreService, PublishSplitsRequest,
    ResetSourceCheckpointRequest, SourceType, StageSplitsRequest, ToggleSourceRequest,
    UpdateSplitsDeleteOpstampRequest,
};
use quickwit_proto::types::{IndexUid, Position};
use quickwit_query::query_ast::qast_json_helper;
use time::OffsetDateTime;
use tokio::time::sleep;
use tracing::{error, info};

pub(crate) mod shard;

use crate::checkpoint::{
    IndexCheckpointDelta, PartitionId, SourceCheckpoint, SourceCheckpointDelta,
};
use crate::{
    AddSourceRequestExt, CreateIndexRequestExt, IndexMetadataResponseExt,
    ListIndexesMetadataResponseExt, ListSplitsQuery, ListSplitsRequestExt, ListSplitsResponseExt,
    MetastoreServiceExt, Split, SplitMaturity, SplitMetadata, SplitState, StageSplitsRequestExt,
};

#[async_trait]
pub trait DefaultForTest {
    async fn default_for_test() -> Self;
}

fn collect_split_ids(splits: &[Split]) -> Vec<&str> {
    splits
        .iter()
        .map(|split| split.split_id())
        .sorted()
        .collect()
}

fn to_btree_set(tags: &[&str]) -> BTreeSet<String> {
    tags.iter().map(|tag| tag.to_string()).collect()
}

async fn cleanup_index(metastore: &mut dyn MetastoreService, index_uid: IndexUid) {
    // List all splits.
    let all_splits = metastore
        .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
        .await
        .unwrap()
        .deserialize_splits()
        .unwrap();

    if !all_splits.is_empty() {
        let all_split_ids: Vec<String> = all_splits
            .iter()
            .map(|split| split.split_id().to_string())
            .collect();

        // Mark splits for deletion.
        let mark_splits_for_deletion_request =
            MarkSplitsForDeletionRequest::new(index_uid.clone(), all_split_ids.clone());
        metastore
            .mark_splits_for_deletion(mark_splits_for_deletion_request)
            .await
            .unwrap();

        // Delete splits.
        let delete_splits_request = DeleteSplitsRequest {
            index_uid: index_uid.clone().into(),
            split_ids: all_split_ids,
        };
        metastore
            .delete_splits(delete_splits_request)
            .await
            .unwrap();
    }
    // Delete index.
    metastore
        .delete_index(DeleteIndexRequest {
            index_uid: index_uid.clone().into(),
        })
        .await
        .unwrap();
}

// Index API tests
//
//  - create_index
//  - index_exists
//  - index_metadata
//  - list_indexes
//  - delete_index

pub async fn test_metastore_create_index<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-create-index");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let create_index_request =
        CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request.clone())
        .await
        .unwrap()
        .index_uid
        .into();

    assert!(metastore.index_exists(&index_id).await.unwrap());

    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap();

    assert_eq!(index_metadata.index_id(), index_id);
    assert_eq!(index_metadata.index_uri(), &index_uri);

    let error = metastore
        .create_index(create_index_request)
        .await
        .unwrap_err();
    assert!(matches!(error, MetastoreError::AlreadyExists { .. }));

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_create_index_with_maximum_length<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix(format!("very-long-index-{}", "a".repeat(233)).as_str());
    assert_eq!(index_id.len(), 255);
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

    assert!(metastore.index_exists(&index_id).await.unwrap());

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_index_exists<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-index-exists");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);
    let create_index_request =
        CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();

    assert!(!metastore.index_exists(&index_id).await.unwrap());

    let index_uid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid
        .into();

    assert!(metastore.index_exists(&index_id).await.unwrap());

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_index_metadata<MetastoreToTest: MetastoreService + DefaultForTest>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-index-metadata");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let error = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    let create_index_request =
        CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid
        .into();

    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap();

    assert_eq!(index_metadata.index_id(), index_id);
    assert_eq!(index_metadata.index_uri(), &index_uri);

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_list_all_indexes<MetastoreToTest: MetastoreService + DefaultForTest>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id_prefix = append_random_suffix("test-list-all-indexes");
    let index_id_1 = format!("{index_id_prefix}-1");
    let index_uri_1 = format!("ram:///indexes/{index_id_1}");
    let index_config_1 = IndexConfig::for_test(&index_id_1, &index_uri_1);

    let index_id_2 = format!("{index_id_prefix}-2");
    let index_uri_2 = format!("ram:///indexes/{index_id_2}");
    let index_config_2 = IndexConfig::for_test(&index_id_2, &index_uri_2);
    let indexes_count = metastore
        .list_indexes_metadata(ListIndexesMetadataRequest::all())
        .await
        .unwrap()
        .deserialize_indexes_metadata()
        .unwrap()
        .into_iter()
        .filter(|index| index.index_id().starts_with(&index_id_prefix))
        .count();
    assert_eq!(indexes_count, 0);

    let index_uid_1 = metastore
        .create_index(CreateIndexRequest::try_from_index_config(index_config_1).unwrap())
        .await
        .unwrap()
        .index_uid
        .into();
    let index_uid_2 = metastore
        .create_index(CreateIndexRequest::try_from_index_config(index_config_2).unwrap())
        .await
        .unwrap()
        .index_uid
        .into();

    let indexes_count = metastore
        .list_indexes_metadata(ListIndexesMetadataRequest::all())
        .await
        .unwrap()
        .deserialize_indexes_metadata()
        .unwrap()
        .into_iter()
        .filter(|index| index.index_id().starts_with(&index_id_prefix))
        .count();
    assert_eq!(indexes_count, 2);

    cleanup_index(&mut metastore, index_uid_1).await;
    cleanup_index(&mut metastore, index_uid_2).await;
}

pub async fn test_metastore_list_indexes<MetastoreToTest: MetastoreService + DefaultForTest>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id_fragment = append_random_suffix("test-list-indexes");
    let index_id_1 = format!("prefix-1-{index_id_fragment}-suffix-1");
    let index_uri_1 = format!("ram:///indexes/{index_id_1}");
    let index_config_1 = IndexConfig::for_test(&index_id_1, &index_uri_1);

    let index_id_2 = format!("prefix-2-{index_id_fragment}-suffix-2");
    let index_uri_2 = format!("ram:///indexes/{index_id_2}");
    let index_config_2 = IndexConfig::for_test(&index_id_2, &index_uri_2);

    let index_id_3 = format!("prefix.3.{index_id_fragment}.3");
    let index_uri_3 = format!("ram:///indexes/{index_id_3}");
    let index_config_3 = IndexConfig::for_test(&index_id_3, &index_uri_3);

    let index_id_4 = format!("p-4-{index_id_fragment}-suffix-4");
    let index_uri_4 = format!("ram:///indexes/{index_id_4}");
    let index_config_4 = IndexConfig::for_test(&index_id_4, &index_uri_4);

    let index_id_patterns = vec![
        format!("prefix-*-{index_id_fragment}-suffix-*"),
        format!("prefix*{index_id_fragment}*suffix-*"),
    ];
    let indexes_count = metastore
        .list_indexes_metadata(ListIndexesMetadataRequest { index_id_patterns })
        .await
        .unwrap()
        .deserialize_indexes_metadata()
        .unwrap()
        .len();
    assert_eq!(indexes_count, 0);

    let index_uid_1 = metastore
        .create_index(CreateIndexRequest::try_from_index_config(index_config_1).unwrap())
        .await
        .unwrap()
        .index_uid
        .into();
    let index_uid_2 = metastore
        .create_index(CreateIndexRequest::try_from_index_config(index_config_2).unwrap())
        .await
        .unwrap()
        .index_uid
        .into();
    let index_uid_3 = metastore
        .create_index(CreateIndexRequest::try_from_index_config(index_config_3).unwrap())
        .await
        .unwrap()
        .index_uid
        .into();
    let index_uid_4 = metastore
        .create_index(CreateIndexRequest::try_from_index_config(index_config_4).unwrap())
        .await
        .unwrap()
        .index_uid
        .into();

    let index_id_patterns = vec![format!("prefix-*-{index_id_fragment}-suffix-*")];
    let indexes_count = metastore
        .list_indexes_metadata(ListIndexesMetadataRequest { index_id_patterns })
        .await
        .unwrap()
        .deserialize_indexes_metadata()
        .unwrap()
        .len();
    assert_eq!(indexes_count, 2);

    cleanup_index(&mut metastore, index_uid_1).await;
    cleanup_index(&mut metastore, index_uid_2).await;
    cleanup_index(&mut metastore, index_uid_3).await;
    cleanup_index(&mut metastore, index_uid_4).await;
}

pub async fn test_metastore_delete_index<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-delete-index");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let index_uid_not_existing = IndexUid::new_with_random_ulid("index-not-found");
    let error = metastore
        .delete_index(DeleteIndexRequest {
            index_uid: index_uid_not_existing.to_string(),
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    let error = metastore
        .delete_index(DeleteIndexRequest {
            index_uid: index_uid_not_existing.to_string(),
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    let create_index_request =
        CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid
        .into();

    metastore
        .delete_index(DeleteIndexRequest {
            index_uid: index_uid.clone().into(),
        })
        .await
        .unwrap();

    assert!(!metastore.index_exists(&index_id).await.unwrap());

    let split_id = format!("{index_id}--split");
    let split_metadata = SplitMetadata {
        split_id: split_id.clone(),
        index_uid: index_uid.clone(),
        ..Default::default()
    };

    let create_index_request = CreateIndexRequest::try_from_index_config(index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid
        .into();

    let stage_splits_request =
        StageSplitsRequest::try_from_split_metadata(index_uid.clone(), split_metadata).unwrap();
    metastore.stage_splits(stage_splits_request).await.unwrap();

    // TODO: We should not be able to delete an index that has remaining splits, at least not as
    // a default behavior. Let's implement the logic that allows this test to pass.
    // let error = metastore.delete_index(index_uid).await.unwrap_err();
    // assert!(matches!(error, MetastoreError::IndexNotEmpty { .. }));
    // let splits = metastore.list_all_splits(index_uid.clone()).await.unwrap();
    // assert_eq!(splits.len(), 1)

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_add_source<MetastoreToTest: MetastoreService + DefaultForTest>() {
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

pub async fn test_metastore_toggle_source<MetastoreToTest: MetastoreService + DefaultForTest>() {
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

pub async fn test_metastore_delete_source<MetastoreToTest: MetastoreService + DefaultForTest>() {
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

pub async fn test_metastore_reset_checkpoint<MetastoreToTest: MetastoreService + DefaultForTest>() {
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

pub async fn test_metastore_publish_splits_empty_splits_array_is_allowed<
    MetastoreToTest: MetastoreService + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-publish-splits-empty");
    let non_existent_index_uid = IndexUid::new_with_random_ulid(&index_id);
    let index_uri = format!("ram:///indexes/{index_id}");

    let source_id = format!("{index_id}--source");

    // Publish a split on a non-existent index
    {
        let publish_splits_request = PublishSplitsRequest {
            index_uid: non_existent_index_uid.to_string(),
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
        let create_index_request =
            CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid
            .into();
        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().to_string(),
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

pub async fn test_metastore_publish_splits<MetastoreToTest: MetastoreService + DefaultForTest>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

    let index_id = append_random_suffix("test-publish-splits");
    let index_uid = IndexUid::new_with_random_ulid(&index_id);
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let source_id = format!("{index_id}--source");

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
            index_uid: IndexUid::new_with_random_ulid("index-not-found").to_string(),
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
            index_uid: IndexUid::new_with_random_ulid(&index_id).to_string(),
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
            CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid
            .into();
        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().into(),
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
            CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid
            .into();

        let stage_splits_request = StageSplitsRequest::try_from_split_metadata(
            index_uid.clone(),
            split_metadata_1.clone(),
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().into(),
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
            CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid
            .into();

        let stage_splits_request = StageSplitsRequest::try_from_split_metadata(
            index_uid.clone(),
            split_metadata_1.clone(),
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().to_string(),
            staged_split_ids: vec![split_id_1.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().to_string(),
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
        println!("{:?}", error);
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
            CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid
            .into();

        let stage_splits_request = StageSplitsRequest::try_from_split_metadata(
            index_uid.clone(),
            split_metadata_1.clone(),
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().to_string(),
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
            index_uid: index_uid.clone().to_string(),
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
            CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid
            .into();

        let stage_splits_request = StageSplitsRequest::try_from_split_metadata(
            index_uid.clone(),
            split_metadata_1.clone(),
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().to_string(),
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
            CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid
            .into();

        let stage_splits_request = StageSplitsRequest::try_from_split_metadata(
            index_uid.clone(),
            split_metadata_1.clone(),
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().to_string(),
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
            index_uid: index_uid.clone().to_string(),
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
            CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid
            .into();

        let stage_splits_request = StageSplitsRequest::try_from_split_metadata(
            index_uid.clone(),
            split_metadata_1.clone(),
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().to_string(),
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
            index_uid: index_uid.clone().to_string(),
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
            CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid
            .into();

        let stage_splits_request = StageSplitsRequest::try_from_split_metadata(
            index_uid.clone(),
            split_metadata_1.clone(),
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let stage_splits_request = StageSplitsRequest::try_from_split_metadata(
            index_uid.clone(),
            split_metadata_2.clone(),
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().to_string(),
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
            CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid
            .into();

        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid.clone(),
            vec![split_metadata_1.clone(), split_metadata_2.clone()],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().to_string(),
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
            index_uid: index_uid.clone().to_string(),
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
            CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid
            .into();

        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid.clone(),
            vec![split_metadata_1.clone(), split_metadata_2.clone()],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().to_string(),
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

        let publish_splits_resquest = PublishSplitsRequest {
            index_uid: index_uid.clone().to_string(),
            staged_split_ids: vec![split_id_1.clone(), split_id_2.clone()],
            index_checkpoint_delta_json_opt: Some({
                let offsets = 30..31;
                let checkpoint_delta = IndexCheckpointDelta::for_test(&source_id, offsets);
                serde_json::to_string(&checkpoint_delta).unwrap()
            }),
            ..Default::default()
        };
        let error = metastore
            .publish_splits(publish_splits_resquest)
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
    MetastoreToTest: MetastoreService + DefaultForTest + Clone,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-publish-concurrency");
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

    let mut join_handles = Vec::with_capacity(10);

    for partition_id in 0..10 {
        let mut metastore_clone = metastore.clone();
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
                let stage_splits_request = StageSplitsRequest::try_from_split_metadata(
                    index_uid.clone(),
                    split_metadata.clone(),
                )
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
                    index_uid: index_uid.clone().into(),
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

pub async fn test_metastore_replace_splits<MetastoreToTest: MetastoreService + DefaultForTest>() {
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
            index_uid: IndexUid::new_with_random_ulid("index-not-found").to_string(),
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
            CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid
            .into();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().into(),
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
            CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid
            .into();

        let stage_splits_request = StageSplitsRequest::try_from_split_metadata(
            index_uid.clone(),
            split_metadata_1.clone(),
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().to_string(),
            staged_split_ids: vec![split_id_1.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        // TODO Source id
        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().to_string(),
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
            CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid
            .into();

        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid.clone(),
            vec![split_metadata_1.clone(), split_metadata_2.clone()],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().to_string(),
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
            index_uid: index_uid.clone().to_string(),
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
            CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid
            .into();

        let stage_splits_request = StageSplitsRequest::try_from_split_metadata(
            index_uid.clone(),
            split_metadata_1.clone(),
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().to_string(),
            staged_split_ids: vec![split_id_1.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        let stage_splits_request = StageSplitsRequest::try_from_split_metadata(
            index_uid.clone(),
            split_metadata_2.clone(),
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().to_string(),
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
            CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid
            .into();

        let stage_splits_request = StageSplitsRequest::try_from_split_metadata(
            index_uid.clone(),
            split_metadata_1.clone(),
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().to_string(),
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

        let stage_splits_request = StageSplitsRequest::try_from_split_metadata(
            index_uid.clone(),
            split_metadata_2.clone(),
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().to_string(),
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
            CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid
            .into();

        let stage_splits_request = StageSplitsRequest::try_from_split_metadata(
            index_uid.clone(),
            split_metadata_1.clone(),
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().to_string(),
            staged_split_ids: vec![split_id_1.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid.clone(),
            vec![split_metadata_2.clone(), split_metadata_3.clone()],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        // TODO Source id
        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().to_string(),
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
    MetastoreToTest: MetastoreService + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

    let index_id = append_random_suffix("test-mark-splits-for-deletion");
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

    let mark_splits_for_deletion_request =
        MarkSplitsForDeletionRequest::new("index-not-found:0".into(), Vec::new());
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
        StageSplitsRequest::try_from_split_metadata(index_uid.clone(), split_metadata_1.clone())
            .unwrap();
    metastore.stage_splits(stage_splits_request).await.unwrap();

    let split_id_2 = format!("{index_id}--split-2");
    let split_metadata_2 = SplitMetadata {
        split_id: split_id_2.clone(),
        index_uid: index_uid.clone(),
        create_timestamp: current_timestamp,
        ..Default::default()
    };
    let stage_splits_request =
        StageSplitsRequest::try_from_split_metadata(index_uid.clone(), split_metadata_2.clone())
            .unwrap();
    metastore.stage_splits(stage_splits_request).await.unwrap();
    let publish_splits_request = PublishSplitsRequest {
        index_uid: index_uid.clone().to_string(),
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
        StageSplitsRequest::try_from_split_metadata(index_uid.clone(), split_metadata_3.clone())
            .unwrap();
    metastore.stage_splits(stage_splits_request).await.unwrap();
    let publish_splits_request = PublishSplitsRequest {
        index_uid: index_uid.clone().to_string(),
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
        ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::MarkedForDeletion),
    )
    .unwrap();
    let marked_splits = metastore
        .list_splits(list_splits_request)
        .await
        .unwrap()
        .deserialize_splits()
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
        ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::MarkedForDeletion),
    )
    .unwrap();
    let mut marked_splits = metastore
        .list_splits(list_splits_request)
        .await
        .unwrap()
        .deserialize_splits()
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

pub async fn test_metastore_delete_splits<MetastoreToTest: MetastoreService + DefaultForTest>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-delete-splits");
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

    let delete_splits_request = DeleteSplitsRequest {
        index_uid: IndexUid::new_with_random_ulid("index-not-found").to_string(),
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
        index_uid: index_not_existing_uid.to_string(),
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
        index_uid: index_uid.clone().into(),
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
        StageSplitsRequest::try_from_split_metadata(index_uid.clone(), split_metadata_1.clone())
            .unwrap();
    metastore.stage_splits(stage_splits_request).await.unwrap();
    let publish_splits_request = PublishSplitsRequest {
        index_uid: index_uid.clone().to_string(),
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
        StageSplitsRequest::try_from_split_metadata(index_uid.clone(), split_metadata_2.clone())
            .unwrap();
    metastore.stage_splits(stage_splits_request).await.unwrap();

    let delete_splits_request = DeleteSplitsRequest {
        index_uid: index_uid.clone().into(),
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
            .deserialize_splits()
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
        index_uid: index_uid.clone().into(),
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
            .deserialize_splits()
            .unwrap()
            .len(),
        0
    );

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_list_all_splits<MetastoreToTest: MetastoreService + DefaultForTest>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-list-all-splits");
    let index_uid = IndexUid::new_with_random_ulid(&index_id);
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let split_id_1 = format!("{index_id}--split-1");
    let split_metadata_1 = SplitMetadata {
        split_id: split_id_1.clone(),
        index_uid: index_uid.clone(),
        ..Default::default()
    };
    let split_id_2 = format!("{index_id}--split-2");
    let split_metadata_2 = SplitMetadata {
        split_id: split_id_2.clone(),
        index_uid: index_uid.clone(),
        ..Default::default()
    };
    let split_id_3 = format!("{index_id}--split-3");
    let split_metadata_3 = SplitMetadata {
        split_id: split_id_3.clone(),
        index_uid: index_uid.clone(),
        ..Default::default()
    };
    let split_id_4 = format!("{index_id}--split-4");
    let split_metadata_4 = SplitMetadata {
        split_id: split_id_4.clone(),
        index_uid: index_uid.clone(),
        ..Default::default()
    };
    let split_id_5 = format!("{index_id}--split-5");
    let split_metadata_5 = SplitMetadata {
        split_id: split_id_5.clone(),
        index_uid: index_uid.clone(),
        ..Default::default()
    };
    let split_id_6 = format!("{index_id}--split-6");
    let split_metadata_6 = SplitMetadata {
        split_id: split_id_6.clone(),
        index_uid: index_uid.clone(),
        ..Default::default()
    };

    let error = metastore
        .list_splits(
            ListSplitsRequest::try_from_index_uid(IndexUid::new_with_random_ulid(
                "index-not-found",
            ))
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        // TODO: This discrepancy is tracked in #3760.
        MetastoreError::NotFound(EntityKind::Index { .. } | EntityKind::Indexes { .. })
    ));

    let create_index_request = CreateIndexRequest::try_from_index_config(index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid
        .into();

    let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
        index_uid.clone(),
        vec![
            split_metadata_1,
            split_metadata_2,
            split_metadata_3,
            split_metadata_4,
            split_metadata_5,
            split_metadata_6,
        ],
    )
    .unwrap();
    metastore.stage_splits(stage_splits_request).await.unwrap();

    let publish_splits_request = PublishSplitsRequest {
        index_uid: index_uid.clone().to_string(),
        staged_split_ids: vec![split_id_1.clone(), split_id_2.clone()],
        ..Default::default()
    };
    metastore
        .publish_splits(publish_splits_request)
        .await
        .unwrap();

    let mark_splits_for_deletion = MarkSplitsForDeletionRequest::new(
        index_uid.clone(),
        vec![split_id_3.clone(), split_id_4.clone()],
    );
    metastore
        .mark_splits_for_deletion(mark_splits_for_deletion)
        .await
        .unwrap();

    let splits = metastore
        .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
        .await
        .unwrap()
        .deserialize_splits()
        .unwrap();
    let split_ids = collect_split_ids(&splits);
    assert_eq!(
        split_ids,
        &[
            &split_id_1,
            &split_id_2,
            &split_id_3,
            &split_id_4,
            &split_id_5,
            &split_id_6
        ]
    );

    cleanup_index(&mut metastore, index_uid.clone()).await;
}

pub async fn test_metastore_list_splits<MetastoreToTest: MetastoreService + DefaultForTest>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

    let index_id = append_random_suffix("test-list-splits");
    let index_uid = IndexUid::new_with_random_ulid(&index_id);
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let split_id_1 = format!("{index_id}--split-1");
    let split_metadata_1 = SplitMetadata {
        split_id: split_id_1.clone(),
        index_uid: index_uid.clone(),
        time_range: Some(0..=99),
        create_timestamp: current_timestamp,
        maturity: SplitMaturity::Immature {
            maturation_period: Duration::from_secs(0),
        },
        tags: to_btree_set(&["tag!", "tag:foo", "$tag!", "$tag:bar"]),
        delete_opstamp: 3,
        ..Default::default()
    };

    let split_id_2 = format!("{index_id}--split-2");
    let split_metadata_2 = SplitMetadata {
        split_id: split_id_2.clone(),
        index_uid: index_uid.clone(),
        time_range: Some(100..=199),
        create_timestamp: current_timestamp,
        maturity: SplitMaturity::Immature {
            maturation_period: Duration::from_secs(10),
        },
        tags: to_btree_set(&["tag!", "$tag!", "$tag:bar"]),
        delete_opstamp: 1,
        ..Default::default()
    };

    let split_id_3 = format!("{index_id}--split-3");
    let split_metadata_3 = SplitMetadata {
        split_id: split_id_3.clone(),
        index_uid: index_uid.clone(),
        time_range: Some(200..=299),
        create_timestamp: current_timestamp,
        maturity: SplitMaturity::Immature {
            maturation_period: Duration::from_secs(20),
        },
        tags: to_btree_set(&["tag!", "tag:foo", "tag:baz", "$tag!"]),
        delete_opstamp: 5,
        ..Default::default()
    };

    let split_id_4 = format!("{index_id}--split-4");
    let split_metadata_4 = SplitMetadata {
        split_id: split_id_4.clone(),
        index_uid: index_uid.clone(),
        time_range: Some(300..=399),
        tags: to_btree_set(&["tag!", "tag:foo", "$tag!"]),
        delete_opstamp: 7,
        ..Default::default()
    };

    let split_id_5 = format!("{index_id}--split-5");
    let split_metadata_5 = SplitMetadata {
        split_id: split_id_5.clone(),
        index_uid: index_uid.clone(),
        time_range: None,
        create_timestamp: current_timestamp,
        tags: to_btree_set(&["tag!", "tag:baz", "tag:biz", "$tag!"]),
        delete_opstamp: 9,
        ..Default::default()
    };

    {
        let query =
            ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Staged);
        let error = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            // TODO: This discrepancy is tracked in #3760.
            MetastoreError::NotFound(EntityKind::Index { .. } | EntityKind::Indexes { .. })
        ));
    }
    {
        let create_index_request =
            CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid
            .into();

        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid.clone(),
            vec![
                split_metadata_1.clone(),
                split_metadata_2.clone(),
                split_metadata_3.clone(),
                split_metadata_4.clone(),
                split_metadata_5.clone(),
            ],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let query = ListSplitsQuery::for_index(index_uid.clone()).with_limit(3);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        assert_eq!(
            splits.len(),
            3,
            "Expected number of splits returned to match limit.",
        );

        let query = ListSplitsQuery::for_index(index_uid.clone()).with_offset(3);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        assert_eq!(
            splits.len(),
            2,
            "Expected 3 splits to be skipped out of the 5 provided splits.",
        );

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_time_range_start_gte(0)
            .with_time_range_end_lt(99);

        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids: Vec<&str> = splits
            .iter()
            .map(|split| split.split_id())
            .sorted()
            .collect();
        assert_eq!(split_ids, &[&split_id_1, &split_id_5]);

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_time_range_start_gte(200);

        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(split_ids, &[&split_id_3, &split_id_4, &split_id_5]);

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_time_range_end_lt(200);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(split_ids, &[&split_id_1, &split_id_2, &split_id_5]);

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_time_range_start_gte(0)
            .with_time_range_end_lt(100);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(split_ids, &[&split_id_1, &split_id_5]);

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_time_range_start_gte(0)
            .with_time_range_end_lt(101);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(split_ids, &[&split_id_1, &split_id_2, &split_id_5]);

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_time_range_start_gte(0)
            .with_time_range_end_lt(199);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(split_ids, &[&split_id_1, &split_id_2, &split_id_5]);

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_time_range_start_gte(0)
            .with_time_range_end_lt(200);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(split_ids, &[&split_id_1, &split_id_2, &split_id_5]);

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_time_range_start_gte(0)
            .with_time_range_end_lt(201);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(
            split_ids,
            &[&split_id_1, &split_id_2, &split_id_3, &split_id_5]
        );

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_time_range_start_gte(0)
            .with_time_range_end_lt(299);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(
            split_ids,
            &[&split_id_1, &split_id_2, &split_id_3, &split_id_5]
        );

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_time_range_start_gte(0)
            .with_time_range_end_lt(300);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(
            split_ids,
            &[&split_id_1, &split_id_2, &split_id_3, &split_id_5]
        );

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_time_range_start_gte(0)
            .with_time_range_end_lt(301);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(
            split_ids,
            &[
                &split_id_1,
                &split_id_2,
                &split_id_3,
                &split_id_4,
                &split_id_5
            ]
        );

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_time_range_start_gte(301)
            .with_time_range_end_lt(400);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(split_ids, &[&split_id_4, &split_id_5]);

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_time_range_start_gte(300)
            .with_time_range_end_lt(400);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(split_ids, &[&split_id_4, &split_id_5]);

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_time_range_start_gte(299)
            .with_time_range_end_lt(400);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(split_ids, &[&split_id_3, &split_id_4, &split_id_5]);

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_time_range_start_gte(201)
            .with_time_range_end_lt(400);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(split_ids, &[&split_id_3, &split_id_4, &split_id_5]);

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_time_range_start_gte(200)
            .with_time_range_end_lt(400);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(split_ids, &[&split_id_3, &split_id_4, &split_id_5]);

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_time_range_start_gte(199)
            .with_time_range_end_lt(400);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(
            split_ids,
            &[&split_id_2, &split_id_3, &split_id_4, &split_id_5]
        );

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_time_range_start_gte(101)
            .with_time_range_end_lt(400);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(
            split_ids,
            &[&split_id_2, &split_id_3, &split_id_4, &split_id_5]
        );

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_time_range_start_gte(101)
            .with_time_range_end_lt(400);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(
            split_ids,
            &[&split_id_2, &split_id_3, &split_id_4, &split_id_5]
        );

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_time_range_start_gte(100)
            .with_time_range_end_lt(400);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);

        assert_eq!(
            split_ids,
            &[&split_id_2, &split_id_3, &split_id_4, &split_id_5]
        );

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_time_range_start_gte(99)
            .with_time_range_end_lt(400);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(
            split_ids,
            &[
                &split_id_1,
                &split_id_2,
                &split_id_3,
                &split_id_4,
                &split_id_5
            ]
        );

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_time_range_start_gte(1000)
            .with_time_range_end_lt(1100);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(split_ids, &[&split_id_5]);

        // Artificially increase the create_timestamp
        sleep(Duration::from_secs(1)).await;
        // add a split without tag
        let split_id_6 = format!("{index_id}--split-6");
        let split_metadata_6 = SplitMetadata {
            split_id: split_id_6.clone(),
            index_uid: index_uid.clone(),
            time_range: None,
            create_timestamp: OffsetDateTime::now_utc().unix_timestamp(),
            ..Default::default()
        };
        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid.clone(),
            vec![split_metadata_6.clone()],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let query =
            ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Staged);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(
            split_ids,
            &[
                &split_id_1,
                &split_id_2,
                &split_id_3,
                &split_id_4,
                &split_id_5,
                &split_id_6,
            ]
        );

        let tag_filter_ast = TagFilterAst::Or(vec![
            TagFilterAst::Or(vec![no_tag("$tag!"), tag("$tag:bar")]),
            TagFilterAst::Or(vec![no_tag("tag!"), tag("tag:baz")]),
        ]);
        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::Staged)
            .with_tags_filter(tag_filter_ast);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(
            split_ids,
            &[
                &split_id_1,
                &split_id_2,
                &split_id_3,
                &split_id_5,
                &split_id_6,
            ]
        );

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_update_timestamp_gte(current_timestamp);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(
            split_ids,
            &[
                &split_id_1,
                &split_id_2,
                &split_id_3,
                &split_id_4,
                &split_id_5,
                &split_id_6,
            ]
        );

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_update_timestamp_gte(split_metadata_6.create_timestamp);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids: Vec<&String> = splits
            .iter()
            .map(|split| &split.split_metadata.split_id)
            .sorted()
            .collect();
        assert_eq!(split_ids, vec![&split_id_6]);

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_create_timestamp_lt(split_metadata_6.create_timestamp);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(
            split_ids,
            &[
                &split_id_1,
                &split_id_2,
                &split_id_3,
                &split_id_4,
                &split_id_5,
            ]
        );

        let query = ListSplitsQuery::for_index(index_uid.clone()).with_delete_opstamp_lt(6);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(
            split_ids,
            &[&split_id_1, &split_id_2, &split_id_3, &split_id_6,]
        );

        // Test maturity filter
        let maturity_evaluation_timestamp =
            OffsetDateTime::from_unix_timestamp(current_timestamp).unwrap();
        let query = ListSplitsQuery::for_index(index_uid.clone())
            .retain_mature(maturity_evaluation_timestamp);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(
            split_ids,
            &[&split_id_1, &split_id_4, &split_id_5, &split_id_6,]
        );

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .retain_immature(maturity_evaluation_timestamp);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(split_ids, &[&split_id_2, &split_id_3]);

        cleanup_index(&mut metastore, index_uid).await;
    }
}

pub async fn test_metastore_split_update_timestamp<
    MetastoreToTest: MetastoreService + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let mut current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

    let index_id = append_random_suffix("split-update-timestamp");
    let index_uid = IndexUid::new_with_random_ulid(&index_id);
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let source_id = format!("{index_id}--source");

    let split_id = format!("{index_id}--split");
    let split_metadata = SplitMetadata {
        split_id: split_id.clone(),
        index_uid: index_uid.clone(),
        create_timestamp: current_timestamp,
        ..Default::default()
    };

    // Create an index
    let create_index_request =
        CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid
        .into();

    // wait for 1s, stage split & check `update_timestamp`
    sleep(Duration::from_secs(1)).await;
    let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
        index_uid.clone(),
        vec![split_metadata.clone()],
    )
    .unwrap();
    metastore.stage_splits(stage_splits_request).await.unwrap();

    sleep(Duration::from_secs(1)).await;
    let split_meta = metastore
        .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
        .await
        .unwrap()
        .deserialize_splits()
        .unwrap()[0]
        .clone();
    assert!(split_meta.update_timestamp > current_timestamp);
    assert!(split_meta.publish_timestamp.is_none());

    current_timestamp = split_meta.update_timestamp;

    // wait for 1s, publish split & check `update_timestamp`
    sleep(Duration::from_secs(1)).await;
    let publish_splits_request = PublishSplitsRequest {
        index_uid: index_uid.clone().into(),
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
        .deserialize_splits()
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
        .deserialize_splits()
        .unwrap()[0]
        .clone();
    assert!(split_meta.update_timestamp > current_timestamp);
    assert!(split_meta.publish_timestamp.is_some());

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_create_delete_task<
    MetastoreToTest: MetastoreService + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;
    let index_id = append_random_suffix("add-delete-task");
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
    let delete_query = DeleteQuery {
        index_uid: index_uid.clone().into(),
        query_ast: qast_json_helper("my_field:my_value", &[]),
        start_timestamp: Some(1),
        end_timestamp: Some(2),
    };

    // Create a delete task on non-existing index.
    let error = metastore
        .create_delete_task(DeleteQuery {
            index_uid: IndexUid::new_with_random_ulid("does-not-exist").to_string(),
            ..delete_query.clone()
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    // Create a delete task on an index with wrong incarnation_id
    let error = metastore
        .create_delete_task(DeleteQuery {
            index_uid: IndexUid::from_parts(&index_id, "12345").to_string(),
            ..delete_query.clone()
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    // Create a delete task.
    let delete_task_1 = metastore
        .create_delete_task(delete_query.clone())
        .await
        .unwrap();
    assert!(delete_task_1.opstamp > 0);
    let delete_query_1 = delete_task_1.delete_query.unwrap();
    assert_eq!(delete_query_1.index_uid, delete_query.index_uid);
    assert_eq!(delete_query_1.start_timestamp, delete_query.start_timestamp);
    assert_eq!(delete_query_1.end_timestamp, delete_query.end_timestamp);
    let delete_task_2 = metastore
        .create_delete_task(delete_query.clone())
        .await
        .unwrap();
    assert!(delete_task_2.opstamp > delete_task_1.opstamp);

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_last_delete_opstamp<
    MetastoreToTest: MetastoreService + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;
    let index_id_1 = append_random_suffix("test-last-delete-opstamp-1");
    let index_uri_1 = format!("ram:///indexes/{index_id_1}");
    let index_config_1 = IndexConfig::for_test(&index_id_1, &index_uri_1);
    let index_id_2 = append_random_suffix("test-last-delete-opstamp-2");
    let index_uri_2 = format!("ram:///indexes/{index_id_2}");
    let index_config_2 = IndexConfig::for_test(&index_id_2, &index_uri_2);
    let index_uid_1: IndexUid = metastore
        .create_index(CreateIndexRequest::try_from_index_config(index_config_1.clone()).unwrap())
        .await
        .unwrap()
        .index_uid
        .into();
    let index_uid_2: IndexUid = metastore
        .create_index(CreateIndexRequest::try_from_index_config(index_config_2.clone()).unwrap())
        .await
        .unwrap()
        .index_uid
        .into();

    let delete_query_index_1 = DeleteQuery {
        index_uid: index_uid_1.to_string(),
        query_ast: qast_json_helper("my_field:my_value", &[]),
        start_timestamp: Some(1),
        end_timestamp: Some(2),
    };
    let delete_query_index_2 = DeleteQuery {
        index_uid: index_uid_2.to_string(),
        query_ast: qast_json_helper("my_field:my_value", &[]),
        start_timestamp: Some(1),
        end_timestamp: Some(2),
    };

    let last_opstamp_index_1_with_no_task = metastore
        .last_delete_opstamp(LastDeleteOpstampRequest {
            index_uid: index_uid_1.to_string(),
        })
        .await
        .unwrap()
        .last_delete_opstamp;
    assert_eq!(last_opstamp_index_1_with_no_task, 0);

    // Create a delete task.
    metastore
        .create_delete_task(delete_query_index_1.clone())
        .await
        .unwrap();
    let delete_task_2 = metastore
        .create_delete_task(delete_query_index_1.clone())
        .await
        .unwrap();
    let delete_task_3 = metastore
        .create_delete_task(delete_query_index_2.clone())
        .await
        .unwrap();

    let last_opstamp_index_1 = metastore
        .last_delete_opstamp(LastDeleteOpstampRequest {
            index_uid: index_uid_1.to_string(),
        })
        .await
        .unwrap()
        .last_delete_opstamp;
    let last_opstamp_index_2 = metastore
        .last_delete_opstamp(LastDeleteOpstampRequest {
            index_uid: index_uid_2.to_string(),
        })
        .await
        .unwrap()
        .last_delete_opstamp;
    assert_eq!(last_opstamp_index_1, delete_task_2.opstamp);
    assert_eq!(last_opstamp_index_2, delete_task_3.opstamp);
    cleanup_index(&mut metastore, index_uid_1).await;
    cleanup_index(&mut metastore, index_uid_2).await;
}

pub async fn test_metastore_delete_index_with_tasks<
    MetastoreToTest: MetastoreService + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;
    let index_id = append_random_suffix("delete-delete-tasks");
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
    let delete_query = DeleteQuery {
        index_uid: index_uid.clone().into(),
        query_ast: qast_json_helper("my_field:my_value", &[]),
        start_timestamp: Some(1),
        end_timestamp: Some(2),
    };
    let _ = metastore
        .create_delete_task(delete_query.clone())
        .await
        .unwrap();
    let _ = metastore
        .create_delete_task(delete_query.clone())
        .await
        .unwrap();

    metastore
        .delete_index(DeleteIndexRequest {
            index_uid: index_uid.clone().into(),
        })
        .await
        .unwrap();
}

pub async fn test_metastore_list_delete_tasks<
    MetastoreToTest: MetastoreService + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;
    let index_id_1 = append_random_suffix("test-list-delete-tasks-1");
    let index_uri_1 = format!("ram:///indexes/{index_id_1}");
    let index_config_1 = IndexConfig::for_test(&index_id_1, &index_uri_1);
    let index_id_2 = append_random_suffix("test-list-delete-tasks-2");
    let index_uri_2 = format!("ram:///indexes/{index_id_2}");
    let index_config_2 = IndexConfig::for_test(&index_id_2, &index_uri_2);
    let index_uid_1: IndexUid = metastore
        .create_index(CreateIndexRequest::try_from_index_config(index_config_1.clone()).unwrap())
        .await
        .unwrap()
        .index_uid
        .into();
    let index_uid_2: IndexUid = metastore
        .create_index(CreateIndexRequest::try_from_index_config(index_config_2.clone()).unwrap())
        .await
        .unwrap()
        .index_uid
        .into();
    let delete_query_index_1 = DeleteQuery {
        index_uid: index_uid_1.to_string(),
        query_ast: qast_json_helper("my_field:my_value", &[]),
        start_timestamp: Some(1),
        end_timestamp: Some(2),
    };
    let delete_query_index_2 = DeleteQuery {
        index_uid: index_uid_2.to_string(),
        query_ast: qast_json_helper("my_field:my_value", &[]),
        start_timestamp: Some(1),
        end_timestamp: Some(2),
    };

    // Create a delete task.
    let delete_task_1 = metastore
        .create_delete_task(delete_query_index_1.clone())
        .await
        .unwrap();
    let delete_task_2 = metastore
        .create_delete_task(delete_query_index_1.clone())
        .await
        .unwrap();
    let _ = metastore
        .create_delete_task(delete_query_index_2.clone())
        .await
        .unwrap();

    let all_index_id_1_delete_tasks = metastore
        .list_delete_tasks(ListDeleteTasksRequest::new(index_uid_1.clone(), 0))
        .await
        .unwrap()
        .delete_tasks;
    assert_eq!(all_index_id_1_delete_tasks.len(), 2);

    let recent_index_id_1_delete_tasks = metastore
        .list_delete_tasks(ListDeleteTasksRequest::new(
            index_uid_1.clone(),
            delete_task_1.opstamp,
        ))
        .await
        .unwrap()
        .delete_tasks;
    assert_eq!(recent_index_id_1_delete_tasks.len(), 1);
    assert_eq!(
        recent_index_id_1_delete_tasks[0].opstamp,
        delete_task_2.opstamp
    );
    cleanup_index(&mut metastore, index_uid_1).await;
    cleanup_index(&mut metastore, index_uid_2).await;
}

pub async fn test_metastore_list_stale_splits<
    MetastoreToTest: MetastoreService + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;
    let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();
    let index_id = append_random_suffix("test-list-stale-splits");
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
    let split_id_4 = format!("{index_id}--split-4");
    let split_metadata_4 = SplitMetadata {
        split_id: split_id_4.clone(),
        index_uid: index_uid.clone(),
        create_timestamp: current_timestamp,
        delete_opstamp: 20,
        ..Default::default()
    };
    // immature split
    let split_id_5 = format!("{index_id}--split-5");
    let split_metadata_5 = SplitMetadata {
        split_id: split_id_5.clone(),
        index_uid: index_uid.clone(),
        create_timestamp: current_timestamp,
        maturity: SplitMaturity::Immature {
            maturation_period: Duration::from_secs(100),
        },
        delete_opstamp: 0,
        ..Default::default()
    };

    let list_stale_splits_request = ListStaleSplitsRequest {
        index_uid: IndexUid::new_with_random_ulid("index-not-found").to_string(),
        delete_opstamp: 0,
        num_splits: 100,
    };
    let error = metastore
        .list_stale_splits(list_stale_splits_request)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    {
        info!("list stale splits on an index");
        let create_index_request =
            CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid
            .into();

        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid.clone(),
            vec![
                split_metadata_1.clone(),
                split_metadata_2.clone(),
                split_metadata_3.clone(),
                split_metadata_5.clone(),
            ],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        // Sleep for 1 second to have different publish timestamps.
        sleep(Duration::from_secs(1)).await;

        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid.clone(),
            vec![split_metadata_4.clone()],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();
        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().into(),
            staged_split_ids: vec![split_id_4.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();
        // Sleep for 1 second to have different publish timestamps.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().into(),
            staged_split_ids: vec![split_id_1.clone(), split_id_2.clone(), split_id_5.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();
        let list_stale_splits_request = ListStaleSplitsRequest {
            index_uid: index_uid.clone().into(),
            delete_opstamp: 100,
            num_splits: 1,
        };
        let splits = metastore
            .list_stale_splits(list_stale_splits_request)
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        assert_eq!(splits.len(), 1);
        assert_eq!(
            splits[0].split_metadata.delete_opstamp,
            split_metadata_2.delete_opstamp
        );

        let list_stale_splits_request = ListStaleSplitsRequest {
            index_uid: index_uid.clone().into(),
            delete_opstamp: 100,
            num_splits: 4,
        };
        let splits = metastore
            .list_stale_splits(list_stale_splits_request)
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        assert_eq!(splits.len(), 3);
        assert_eq!(splits[0].split_id(), split_metadata_2.split_id());
        assert_eq!(splits[1].split_id(), split_metadata_4.split_id());
        assert_eq!(splits[2].split_id(), split_metadata_1.split_id());
        assert_eq!(
            splits[2].split_metadata.delete_opstamp,
            split_metadata_1.delete_opstamp
        );

        let list_stale_splits_request = ListStaleSplitsRequest {
            index_uid: index_uid.clone().into(),
            delete_opstamp: 20,
            num_splits: 2,
        };
        let splits = metastore
            .list_stale_splits(list_stale_splits_request)
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        assert_eq!(splits.len(), 1);
        assert_eq!(
            splits[0].split_metadata.delete_opstamp,
            split_metadata_2.delete_opstamp
        );

        let list_stale_splits_request = ListStaleSplitsRequest {
            index_uid: index_uid.clone().into(),
            delete_opstamp: 10,
            num_splits: 2,
        };
        let splits = metastore
            .list_stale_splits(list_stale_splits_request)
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        assert!(splits.is_empty());
        cleanup_index(&mut metastore, index_uid).await;
    }
}

pub async fn test_metastore_update_splits_delete_opstamp<
    MetastoreToTest: MetastoreService + DefaultForTest,
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
            index_uid: IndexUid::new_with_random_ulid("index-not-found").to_string(),
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
            CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid
            .into();

        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid.clone(),
            vec![
                split_metadata_1.clone(),
                split_metadata_2.clone(),
                split_metadata_3.clone(),
            ],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();
        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.clone().into(),
            staged_split_ids: vec![split_id_1.clone(), split_id_2.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        let list_stale_splits_request = ListStaleSplitsRequest {
            index_uid: index_uid.clone().into(),
            delete_opstamp: 100,
            num_splits: 2,
        };
        let splits = metastore
            .list_stale_splits(list_stale_splits_request)
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        assert_eq!(splits.len(), 2);

        let update_splits_delete_opstamp_request = UpdateSplitsDeleteOpstampRequest {
            index_uid: index_uid.clone().into(),
            split_ids: vec![split_id_1.clone(), split_id_2.clone()],
            delete_opstamp: 100,
        };
        metastore
            .update_splits_delete_opstamp(update_splits_delete_opstamp_request)
            .await
            .unwrap();

        let list_stale_splits_request = ListStaleSplitsRequest {
            index_uid: index_uid.clone().into(),
            delete_opstamp: 100,
            num_splits: 2,
        };
        let splits = metastore
            .list_stale_splits(list_stale_splits_request)
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        assert_eq!(splits.len(), 0);

        let list_stale_splits_request = ListStaleSplitsRequest {
            index_uid: index_uid.clone().into(),
            delete_opstamp: 200,
            num_splits: 2,
        };
        let splits = metastore
            .list_stale_splits(list_stale_splits_request)
            .await
            .unwrap()
            .deserialize_splits()
            .unwrap();
        assert_eq!(splits.len(), 2);
        assert_eq!(splits[0].split_metadata.delete_opstamp, 100);
        assert_eq!(splits[1].split_metadata.delete_opstamp, 100);

        cleanup_index(&mut metastore, index_uid).await;
    }
}

pub async fn test_metastore_stage_splits<MetastoreToTest: MetastoreService + DefaultForTest>() {
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

    // Stage a splits on a non-existent index
    let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
        IndexUid::new_with_random_ulid("index-not-found"),
        vec![split_metadata_1.clone()],
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

    let create_index_request =
        CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid
        .into();

    // Stage a split on an index
    let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
        index_uid.clone(),
        vec![split_metadata_1.clone(), split_metadata_2.clone()],
    )
    .unwrap();
    metastore.stage_splits(stage_splits_request).await.unwrap();

    let query = ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Staged);
    let splits = metastore
        .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
        .await
        .unwrap()
        .deserialize_splits()
        .unwrap();
    let split_ids = collect_split_ids(&splits);
    assert_eq!(split_ids, &[&split_id_1, &split_id_2]);

    // Stage a existent-staged-split on an index
    let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
        index_uid.clone(),
        vec![split_metadata_1.clone()],
    )
    .unwrap();
    metastore
        .stage_splits(stage_splits_request)
        .await
        .expect("Pre-existing staged splits should be updated.");

    let publish_splits_request = PublishSplitsRequest {
        index_uid: index_uid.clone().into(),
        staged_split_ids: vec![split_id_1.clone(), split_id_2.clone()],
        ..Default::default()
    };
    metastore
        .publish_splits(publish_splits_request)
        .await
        .unwrap();
    let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
        index_uid.clone(),
        vec![split_metadata_1.clone()],
    )
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
    let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
        index_uid.clone(),
        vec![split_metadata_2.clone()],
    )
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

#[macro_export]
macro_rules! metastore_test_suite {
    ($metastore_type:ty) => {
        #[cfg(test)]
        mod common_tests {

            // Index API tests
            //
            //  - create_index
            //  - index_exists
            //  - index_metadata
            //  - list_indexes
            //  - delete_index

            #[tokio::test]
            async fn test_metastore_create_index() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_create_index::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_create_index_with_maximum_length() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_create_index_with_maximum_length::<$metastore_type>()
                    .await;
            }

            #[tokio::test]
            async fn test_metastore_index_exists() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_index_exists::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_index_metadata() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_index_metadata::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_list_indexes() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_list_indexes::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_list_all_indexes() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_list_all_indexes::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_delete_index() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_delete_index::<$metastore_type>().await;
            }

            // Split API tests
            //
            //  - stage_splits
            //  - publish_splits
            //  - mark_splits_for_deletion
            //  - delete_splits

            #[tokio::test]
            async fn test_metastore_publish_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_publish_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_publish_splits_concurrency() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_publish_splits_concurrency::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_publish_splits_empty_splits_array_is_allowed() {
                $crate::tests::test_metastore_publish_splits_empty_splits_array_is_allowed::<
                    $metastore_type,
                >()
                .await;
            }

            #[tokio::test]
            async fn test_metastore_replace_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_replace_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_mark_splits_for_deletion() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_mark_splits_for_deletion::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_delete_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_delete_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_list_all_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_list_all_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_list_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_list_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_split_update_timestamp() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_split_update_timestamp::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_add_source() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_add_source::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_toggle_source() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_toggle_source::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_delete_source() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_delete_source::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_reset_checkpoint() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_reset_checkpoint::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_create_delete_task() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_create_delete_task::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_last_delete_opstamp() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_last_delete_opstamp::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_delete_index_with_tasks() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_delete_index_with_tasks::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_list_delete_tasks() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_list_delete_tasks::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_list_stale_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_list_stale_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_update_splits_delete_opstamp() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_update_splits_delete_opstamp::<$metastore_type>()
                    .await;
            }

            #[tokio::test]
            async fn test_metastore_stage_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                $crate::tests::test_metastore_stage_splits::<$metastore_type>().await;
            }

            /// Shard API tests

            #[tokio::test]
            async fn test_metastore_open_shards() {
                $crate::tests::shard::test_metastore_open_shards::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_acquire_shards() {
                $crate::tests::shard::test_metastore_acquire_shards::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_list_shards() {
                $crate::tests::shard::test_metastore_list_shards::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_delete_shards() {
                $crate::tests::shard::test_metastore_delete_shards::<$metastore_type>().await;
            }
        }
    };
}
