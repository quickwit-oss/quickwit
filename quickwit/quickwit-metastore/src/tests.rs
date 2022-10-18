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

#[cfg(test)]
pub mod test_suite {
    use std::collections::{BTreeSet, HashSet};

    use async_trait::async_trait;
    use itertools::Itertools;
    use quickwit_common::rand::append_random_suffix;
    use quickwit_config::{SourceConfig, SourceParams};
    use quickwit_doc_mapper::tag_pruning::{no_tag, tag, TagFilterAst};
    use quickwit_proto::metastore_api::DeleteQuery;
    use time::OffsetDateTime;
    use tokio::time::{sleep, Duration};
    use tracing::{error, info};

    use crate::checkpoint::{IndexCheckpointDelta, PartitionId, Position, SourceCheckpoint};
    use crate::{IndexMetadata, Metastore, MetastoreError, SplitMetadata, SplitState};

    #[async_trait]
    pub trait DefaultForTest {
        async fn default_for_test() -> Self;
    }

    fn to_set(tags: &[&str]) -> BTreeSet<String> {
        tags.iter().map(ToString::to_string).collect()
    }

    fn to_hash_set(split_ids: &[&str]) -> std::collections::HashSet<String> {
        split_ids.iter().map(ToString::to_string).collect()
    }

    async fn cleanup_index(metastore: &dyn Metastore, index_id: &str) {
        // List all splits.
        let all_splits = metastore.list_all_splits(index_id).await.unwrap();

        if !all_splits.is_empty() {
            let all_split_ids = all_splits
                .iter()
                .map(|meta| meta.split_id())
                .collect::<Vec<_>>();

            // Mark splits for deletion.
            metastore
                .mark_splits_for_deletion(index_id, &all_split_ids)
                .await
                .unwrap();

            // Delete splits.
            metastore
                .delete_splits(index_id, &all_split_ids)
                .await
                .unwrap();
        }

        // Delete index.
        metastore.delete_index(index_id).await.unwrap();
    }

    pub async fn test_metastore_reset_checkpoint<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = append_random_suffix("test-metastore-reset-checkpoint");
        let index_uri = format!("ram://indexes/{index_id}");
        let index_metadata = IndexMetadata::for_test(&index_id, &index_uri);

        metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();

        let source_ids: Vec<String> = (0..2).map(|i| format!("{index_id}--source-{i}")).collect();
        let split_ids: Vec<String> = (0..2).map(|i| format!("{index_id}--split-{i}")).collect();

        for (source_id, split_id) in source_ids.iter().zip(split_ids.iter()) {
            let source = SourceConfig {
                source_id: source_id.clone(),
                num_pipelines: 1,
                enabled: true,
                source_params: SourceParams::void(),
            };
            metastore
                .add_source(&index_id, source.clone())
                .await
                .unwrap();

            let split_metadata = SplitMetadata {
                split_id: split_id.clone(),
                index_id: index_id.clone(),
                ..Default::default()
            };
            metastore
                .stage_split(&index_id, split_metadata)
                .await
                .unwrap();
            metastore
                .publish_splits(&index_id, &[split_id], &[], None)
                .await
                .unwrap();
        }
        assert!(!metastore
            .index_metadata(&index_id)
            .await
            .unwrap()
            .checkpoint
            .is_empty());

        metastore
            .reset_source_checkpoint(&index_id, &source_ids[0])
            .await
            .unwrap();

        let index_metadata = metastore.index_metadata(&index_id).await.unwrap();
        assert!(index_metadata
            .checkpoint
            .source_checkpoint(&source_ids[0])
            .is_none());

        assert!(index_metadata
            .checkpoint
            .source_checkpoint(&source_ids[1])
            .is_some());

        metastore
            .reset_source_checkpoint(&index_id, &source_ids[1])
            .await
            .unwrap();

        assert!(metastore
            .index_metadata(&index_id)
            .await
            .unwrap()
            .checkpoint
            .is_empty());

        cleanup_index(&metastore, &index_metadata.index_id).await;
    }

    pub async fn test_metastore_add_source<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = "test-metastore-add-source";
        let index_uri = format!("ram://indexes/{index_id}");
        let index_metadata = IndexMetadata::for_test(index_id, &index_uri);

        metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();

        let source_id = "test-metastore-add-source--void-source-id";

        let source = SourceConfig {
            source_id: source_id.to_string(),
            num_pipelines: 1,
            enabled: true,
            source_params: SourceParams::void(),
        };

        assert_eq!(
            metastore
                .index_metadata(index_id)
                .await
                .unwrap()
                .checkpoint
                .source_checkpoint(source_id),
            None
        );

        metastore
            .add_source(index_id, source.clone())
            .await
            .unwrap();

        let index_metadata = metastore.index_metadata(index_id).await.unwrap();

        let sources = index_metadata.sources;
        assert_eq!(sources.len(), 1);
        assert!(sources.contains_key(source_id));
        assert_eq!(sources.get(source_id).unwrap().source_id, source_id);
        assert_eq!(sources.get(source_id).unwrap().source_type(), "void");
        assert_eq!(
            index_metadata.checkpoint.source_checkpoint(source_id),
            Some(&SourceCheckpoint::default())
        );

        assert!(matches!(
            metastore
                .add_source(index_id, source.clone())
                .await
                .unwrap_err(),
            MetastoreError::SourceAlreadyExists { .. }
        ));
        assert!(matches!(
            metastore
                .add_source("index-id-does-not-exist", source)
                .await
                .unwrap_err(),
            MetastoreError::IndexDoesNotExist { .. }
        ));
        cleanup_index(&metastore, &index_metadata.index_id).await;
    }

    pub async fn test_metastore_toggle_source<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = "test-metastore-toggle-source";
        let index_uri = format!("ram://indexes/{index_id}");
        let index_metadata = IndexMetadata::for_test(index_id, &index_uri);

        metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();

        let source_id = "test-metastore-toggle-source--void-source-id";
        let source = SourceConfig {
            source_id: source_id.to_string(),
            num_pipelines: 1,
            enabled: true,
            source_params: SourceParams::void(),
        };
        metastore
            .add_source(index_id, source.clone())
            .await
            .unwrap();
        let index_metadata = metastore.index_metadata(index_id).await.unwrap();
        let source = index_metadata.sources.get(source_id).unwrap();
        assert_eq!(source.enabled, true);

        // Disable source.
        metastore
            .toggle_source(index_id, &source.source_id, false)
            .await
            .unwrap();
        let index_metadata = metastore.index_metadata(index_id).await.unwrap();
        let source = index_metadata.sources.get(source_id).unwrap();
        assert_eq!(source.enabled, false);

        // Enable source.
        metastore
            .toggle_source(index_id, &source.source_id, true)
            .await
            .unwrap();
        let index_metadata = metastore.index_metadata(index_id).await.unwrap();
        let source = index_metadata.sources.get(source_id).unwrap();
        assert_eq!(source.enabled, true);

        cleanup_index(&metastore, &index_metadata.index_id).await;
    }

    pub async fn test_metastore_delete_source<MetastoreToTest: Metastore + DefaultForTest>() {
        let _ = tracing_subscriber::fmt::try_init();
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = "test-metastore-delete-source";
        let index_uri = format!("ram://indexes/{index_id}");
        let source_id = "test-metastore-delete-source--void-source-id";

        let source = SourceConfig {
            source_id: source_id.to_string(),
            num_pipelines: 1,
            enabled: true,
            source_params: SourceParams::void(),
        };

        let mut index_metadata = IndexMetadata::for_test(index_id, index_uri.as_str());
        index_metadata.sources.insert(source_id.to_string(), source);

        metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        metastore.delete_source(index_id, source_id).await.unwrap();

        let sources = metastore.index_metadata(index_id).await.unwrap().sources;
        assert!(sources.is_empty());

        assert!(matches!(
            metastore
                .delete_source(index_id, source_id)
                .await
                .unwrap_err(),
            MetastoreError::SourceDoesNotExist { .. }
        ));
        assert!(matches!(
            metastore
                .delete_source("index-id-does-not-exist", source_id)
                .await
                .unwrap_err(),
            MetastoreError::IndexDoesNotExist { .. }
        ));

        cleanup_index(&metastore, &index_metadata.index_id).await;
    }

    pub async fn test_metastore_create_index<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = "create-index-index";
        let index_uri = format!("ram://indexes/{index_id}");
        let index_metadata = IndexMetadata::for_test(index_id, &index_uri);

        // Create an index
        metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();

        cleanup_index(&metastore, index_id).await;
    }

    pub async fn test_metastore_delete_index<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = "delete-index-index";
        let index_uri = format!("ram://indexes/{index_id}");
        let index_metadata = IndexMetadata::for_test(index_id, &index_uri);

        // Delete a non-existent index
        let result = metastore
            .delete_index("non-existent-index")
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));

        metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();

        // Delete an index
        metastore.delete_index(index_id).await.unwrap();
    }

    pub async fn test_metastore_index_metadata<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = "index-metadata-index";
        let index_uri = format!("ram://indexes/{index_id}");
        let index_metadata = IndexMetadata::for_test(index_id, &index_uri);

        // Get a non-existent index metadata
        let result = metastore
            .index_metadata("non-existent-index")
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));

        metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();

        // Get an index metadata
        let index_metadata = metastore.index_metadata(index_id).await.unwrap();
        assert_eq!(index_metadata.index_id, index_id);
        assert_eq!(index_metadata.index_uri, index_uri);

        cleanup_index(&metastore, index_id).await;
    }

    pub async fn test_metastore_stage_split<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

        let index_id = "stage-split-index";
        let index_uri = format!("ram://indexes/{index_id}");
        let index_metadata = IndexMetadata::for_test(index_id, &index_uri);

        let split_id = "stage-split-my-index-one";
        let split_metadata = SplitMetadata {
            split_id: split_id.to_string(),
            index_id: index_id.to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(0..=99),
            create_timestamp: current_timestamp,
            footer_offsets: 1000..2000,
            ..Default::default()
        };

        // Stage a split on a non-existent index
        let result = metastore
            .stage_split("non-existent-index", split_metadata.clone())
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));

        metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();

        // Stage a split on an index
        metastore
            .stage_split(index_id, split_metadata.clone())
            .await
            .unwrap();

        // Stage a existent-split on an index
        let result = metastore
            .stage_split(index_id, split_metadata.clone())
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::InternalError { .. }));

        cleanup_index(&metastore, index_id).await;
    }

    pub async fn test_metastore_publish_splits_empty_splits_array_is_allowed<
        MetastoreToTest: Metastore + DefaultForTest,
    >() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = "publish-splits-empty-index";
        let index_uri = format!("ram://indexes/{index_id}");
        let source_id = "publish-splits-empty-source";

        // Publish a split on a non-existent index
        {
            let result = metastore
                .publish_splits(
                    "non-existent-index",
                    &[],
                    &[],
                    {
                        let offsets = 1..10;
                        IndexCheckpointDelta::for_test(source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));
        }

        // Update the checkpoint, by publishing an empty array of splits with a non-empty
        // checkpoint. This operation is allowed and used in the Indexer.
        {
            let index_metadata = IndexMetadata::for_test(index_id, &index_uri);
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            let result = metastore
                .publish_splits(
                    index_id,
                    &[],
                    &[],
                    {
                        let offsets = 0..100;
                        IndexCheckpointDelta::for_test(source_id, offsets)
                    }
                    .into(),
                )
                .await;
            assert!(result.is_ok());

            let index_metadata = metastore.index_metadata(index_id).await.unwrap();
            let source_checkpoint = index_metadata
                .checkpoint
                .source_checkpoint(source_id)
                .unwrap();
            assert_eq!(source_checkpoint.num_partitions(), 1);
            assert_eq!(
                source_checkpoint
                    .position_for_partition(&PartitionId::default())
                    .unwrap(),
                &Position::from(100u64 - 1)
            );
            cleanup_index(&metastore, index_id).await;
        }
    }

    pub async fn test_metastore_publish_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

        let index_id = "publish-splits-index";
        let index_uri = format!("ram://indexes/{index_id}");
        let source_id = "publish-splits-source";
        let index_metadata = IndexMetadata::for_test(index_id, &index_uri);

        let split_id_1 = "publish-splits-index-one";
        let split_metadata_1 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_1.to_string(),
            index_id: index_id.to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(0..=99),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        let split_id_2 = "publish-splits-index-two";
        let split_metadata_2 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_2.to_string(),
            index_id: index_id.to_string(),
            num_docs: 5,
            uncompressed_docs_size_in_bytes: 6,
            time_range: Some(30..=99),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        // Publish a split on a non-existent index
        {
            let result = metastore
                .publish_splits(
                    "non-existent-index",
                    &["non-existent-split"],
                    &[],
                    {
                        let offsets = 1..10;
                        IndexCheckpointDelta::for_test(source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));
        }

        // Publish a non-existent split on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            let result = metastore
                .publish_splits(index_id, &["non-existent-split"], &[], None)
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, index_id).await;
        }

        // Publish a staged split on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(index_id, &[split_id_1], &[], None)
                .await
                .unwrap();

            cleanup_index(&metastore, index_id).await;
        }

        // Publish a published split on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(
                    index_id,
                    &[split_id_1],
                    &[],
                    {
                        let offsets = 1..12;
                        IndexCheckpointDelta::for_test(source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap();

            let publish_error = metastore
                .publish_splits(
                    index_id,
                    &[split_id_1],
                    &[],
                    {
                        let offsets = 1..12;
                        IndexCheckpointDelta::for_test(source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap_err();
            assert!(matches!(
                publish_error,
                MetastoreError::IncompatibleCheckpointDelta(_)
            ));

            cleanup_index(&metastore, index_id).await;
        }

        // Publish a non-staged split on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(
                    index_id,
                    &[split_id_1],
                    &[],
                    {
                        let offsets = 12..15;
                        IndexCheckpointDelta::for_test(source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap();

            metastore
                .mark_splits_for_deletion(index_id, &[split_id_1])
                .await
                .unwrap();

            let result = metastore
                .publish_splits(
                    index_id,
                    &[split_id_1],
                    &[],
                    {
                        let offsets = 15..18;
                        IndexCheckpointDelta::for_test(source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsNotStaged { .. }));

            cleanup_index(&metastore, index_id).await;
        }

        // Publish a staged split and non-existent split on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            let result = metastore
                .publish_splits(
                    index_id,
                    &[split_id_1, "non-existent-split"],
                    &[],
                    {
                        let offsets = 15..18;
                        IndexCheckpointDelta::for_test(source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, index_id).await;
        }

        // Publish a published split and non-existant split on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(
                    index_id,
                    &[split_id_1],
                    &[],
                    {
                        let offsets = 15..18;
                        IndexCheckpointDelta::for_test(source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap();

            let result = metastore
                .publish_splits(
                    index_id,
                    &[split_id_1, "non-existent-split"],
                    &[],
                    {
                        let offsets = 18..24;
                        IndexCheckpointDelta::for_test(source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, index_id).await;
        }

        // Publish a non-staged split and non-existant split on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(
                    index_id,
                    &[split_id_1],
                    &[],
                    {
                        let offsets = 18..24;
                        IndexCheckpointDelta::for_test(source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap();

            metastore
                .mark_splits_for_deletion(index_id, &[split_id_1])
                .await
                .unwrap();

            let result = metastore
                .publish_splits(
                    index_id,
                    &[split_id_1, "non-existent-split"],
                    &[],
                    {
                        let offsets = 24..26;
                        IndexCheckpointDelta::for_test(source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, index_id).await;
        }

        // Publish staged splits on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_2.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(
                    index_id,
                    &[split_id_1, split_id_2],
                    &[],
                    {
                        let offsets = 24..26;
                        IndexCheckpointDelta::for_test(source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap();

            cleanup_index(&metastore, index_id).await;
        }

        // Publish a staged split and published split on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_2.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(
                    index_id,
                    &[split_id_2],
                    &[],
                    {
                        let offsets = 26..28;
                        IndexCheckpointDelta::for_test(source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap();

            metastore
                .publish_splits(
                    index_id,
                    &[split_id_1, split_id_2],
                    &[],
                    {
                        let offsets = 28..30;
                        IndexCheckpointDelta::for_test(source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap();

            cleanup_index(&metastore, index_id).await;
        }

        // Publish published splits on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_2.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(
                    index_id,
                    &[split_id_1, split_id_2],
                    &[],
                    {
                        let offsets = 30..31;
                        IndexCheckpointDelta::for_test(source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap();

            let publish_error = metastore
                .publish_splits(
                    index_id,
                    &[split_id_1, split_id_2],
                    &[],
                    {
                        let offsets = 30..31;
                        IndexCheckpointDelta::for_test(source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap_err();
            assert!(matches!(
                publish_error,
                MetastoreError::IncompatibleCheckpointDelta(_)
            ));

            cleanup_index(&metastore, index_id).await;
        }
    }

    pub async fn test_metastore_replace_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

        let index_id = append_random_suffix("test-metastore-replace-splits");
        let index_uri = format!("ram://indexes/{index_id}");
        let index_metadata = IndexMetadata::for_test(&index_id, &index_uri);

        let split_id_1 = format!("{index_id}--split-one");
        let split_metadata_1 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_1.clone(),
            index_id: index_id.clone(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: None,
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        let split_id_2 = format!("{index_id}--split-two");
        let split_metadata_2 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_2.clone(),
            index_id: index_id.clone(),
            num_docs: 5,
            uncompressed_docs_size_in_bytes: 6,
            time_range: None,
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        let split_id_3 = format!("{index_id}--split-three");
        let split_metadata_3 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_3.clone(),
            index_id: index_id.clone(),
            num_docs: 5,
            uncompressed_docs_size_in_bytes: 6,
            time_range: None,
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        // Replace splits on a non-existent index
        {
            let error = metastore
                .publish_splits(
                    "non-existent-index",
                    &["non-existent-split-one"],
                    &["non-existent-split-two"],
                    None,
                )
                .await
                .unwrap_err();
            assert!(matches!(error, MetastoreError::IndexDoesNotExist { .. }));
        }

        // Replace a non-existent split on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            // TODO source id
            let result = metastore
                .publish_splits(
                    &index_id,
                    &["non-existent-split"],
                    &["non-existent-split-two"],
                    None,
                )
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, &index_id).await;
        }

        // Replace a publish split with a non existing split
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(&index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(&index_id, &[&split_id_1], &[], None)
                .await
                .unwrap();

            // TODO Source id
            let result = metastore
                .publish_splits(&index_id, &[&split_id_2], &[&split_id_1], None)
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, &index_id).await;
        }

        // Replace a publish split with a deleted split
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(&index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .stage_split(&index_id, split_metadata_2.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(&index_id, &[&split_id_1, &split_id_2], &[], None)
                .await
                .unwrap();

            metastore
                .mark_splits_for_deletion(&index_id, &[&split_id_2])
                .await
                .unwrap();

            // TODO source_id
            let result = metastore
                .publish_splits(&index_id, &[&split_id_2], &[&split_id_1], None)
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsNotStaged { .. }));

            cleanup_index(&metastore, &index_id).await;
        }

        // Replace a publish split with mixed splits
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(&index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(&index_id, &[&split_id_1], &[], None)
                .await
                .unwrap();

            metastore
                .stage_split(&index_id, split_metadata_2.clone())
                .await
                .unwrap();

            let result = metastore
                .publish_splits(&index_id, &[&split_id_2, &split_id_3], &[&split_id_1], None) // TODO source id
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, &index_id).await;
        }

        // Replace a deleted split with a new split
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(&index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(&index_id, &[&split_id_1], &[], None)
                .await
                .unwrap();

            metastore
                .mark_splits_for_deletion(&index_id, &[&split_id_1])
                .await
                .unwrap();

            metastore
                .stage_split(&index_id, split_metadata_2.clone())
                .await
                .unwrap();

            let error = metastore
                .publish_splits(&index_id, &[&split_id_2], &[&split_id_1], None)
                .await
                .unwrap_err();
            assert!(
                matches!(error, MetastoreError::SplitsNotDeletable { split_ids } if split_ids == vec![split_id_1.clone()])
            );

            cleanup_index(&metastore, &index_id).await;
        }

        // Replace a publish split with staged splits
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(&index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(&index_id, &[&split_id_1], &[], None)
                .await
                .unwrap();

            metastore
                .stage_split(&index_id, split_metadata_2.clone())
                .await
                .unwrap();

            metastore
                .stage_split(&index_id, split_metadata_3.clone())
                .await
                .unwrap();

            // TODO Source id
            metastore
                .publish_splits(&index_id, &[&split_id_2, &split_id_3], &[&split_id_1], None)
                .await
                .unwrap();

            cleanup_index(&metastore, &index_id).await;
        }
    }

    pub async fn test_metastore_mark_splits_for_deletion<
        MetastoreToTest: Metastore + DefaultForTest,
    >() {
        let _ = tracing_subscriber::fmt::try_init();
        let metastore = MetastoreToTest::default_for_test().await;

        let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

        let index_id = "mark-splits-as-deleted-my-index";
        let index_uri = format!("ram://indexes/{index_id}");
        let index_metadata = IndexMetadata::for_test(index_id, &index_uri);
        let split_id_1 = "mark-splits-as-deleted-my-index-one";
        let split_metadata_1 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_1.to_string(),
            index_id: index_id.to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(0..=99),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        // Mark a split for deletion on a non-existent index
        {
            let index_does_not_exist_error = metastore
                .mark_splits_for_deletion("non-existent-index", &["non-existent-split"])
                .await
                .unwrap_err();
            info!(err=?index_does_not_exist_error);
            assert!(matches!(
                index_does_not_exist_error,
                MetastoreError::IndexDoesNotExist { .. }
            ));
        }

        // Mark a non-existent split for deletion on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            let result = metastore
                .mark_splits_for_deletion(index_id, &["non-existent-split"])
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, index_id).await;
        }

        // Mark an existent split for deletion on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .mark_splits_for_deletion(index_id, &[split_id_1])
                .await
                .unwrap();

            cleanup_index(&metastore, index_id).await;
        }
    }

    pub async fn test_metastore_delete_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let _ = tracing_subscriber::fmt::try_init();
        let metastore = MetastoreToTest::default_for_test().await;

        let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

        let index_id = "delete-splits-index";
        let index_uri = format!("ram://indexes/{index_id}");
        let source_id = "delete-splits-source";
        let index_metadata = IndexMetadata::for_test(index_id, &index_uri);

        let split_id_1 = "delete-splits-index-one";
        let split_metadata_1 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_1.to_string(),
            index_id: index_id.to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(0..=99),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        {
            info!("Delete a split marked for deletion on a non-existent index");
            let result = metastore
                .delete_splits("non-existent-index", &["non-existent-split"])
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));
        }

        {
            info!("Delete a non-existent split marked for deletion on an index.");
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            let result = metastore
                .delete_splits(index_id, &["non-existent-split"])
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, index_id).await;
        }

        {
            info!("Delete a staged split on an index.");
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .delete_splits(index_id, &[split_id_1])
                .await
                .unwrap();

            cleanup_index(&metastore, index_id).await;
        }

        {
            info!("Delete a split that has been marked for deletion on an index");
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .mark_splits_for_deletion(index_id, &[split_id_1])
                .await
                .unwrap();

            metastore
                .delete_splits(index_id, &[split_id_1])
                .await
                .unwrap();

            cleanup_index(&metastore, index_id).await;
        }

        {
            info!("Delete a split that is not marked for deletion");
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(
                    index_id,
                    &[split_id_1],
                    &[],
                    {
                        let offsets = 0..5;
                        IndexCheckpointDelta::for_test(source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap();

            let metastore_err = metastore
                .delete_splits(index_id, &[split_id_1])
                .await
                .unwrap_err();
            assert!(matches!(
                metastore_err,
                MetastoreError::SplitsNotDeletable { .. }
            ));

            cleanup_index(&metastore, index_id).await;
        }
    }

    pub async fn test_metastore_list_all_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

        let index_id = "list-all-splits-index";
        let index_uri = format!("ram://indexes/{index_id}");
        let index_metadata = IndexMetadata::for_test(index_id, &index_uri);

        let split_id_1 = "list-all-splits-index-one";
        let split_metadata_1 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_1.to_string(),
            index_id: index_id.to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(0..=99),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        let split_metadata_2 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "list-all-splits-index-two".to_string(),
            index_id: index_id.to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(100..=199),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        let split_metadata_3 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "list-all-splits-index-three".to_string(),
            index_id: index_id.to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(200..=299),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        let split_metadata_4 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "list-all-splits-index-four".to_string(),
            index_id: index_id.to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(300..=399),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        let split_metadata_5 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "list-all-splits-index-five".to_string(),
            index_id: index_id.to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: None,
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        // List all splits on a non-existent index
        {
            let result = metastore
                .list_all_splits("non-existent-index")
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));
        }

        // List all splits on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_2.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_3.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_4.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_5.clone())
                .await
                .unwrap();

            let splits = metastore.list_all_splits(index_id).await.unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(split_ids.contains("list-all-splits-index-one"), true);
            assert_eq!(split_ids.contains("list-all-splits-index-two"), true);
            assert_eq!(split_ids.contains("list-all-splits-index-three"), true);
            assert_eq!(split_ids.contains("list-all-splits-index-four"), true);
            assert_eq!(split_ids.contains("list-all-splits-index-five"), true);

            cleanup_index(&metastore, index_id).await;
        }
    }

    pub async fn test_metastore_list_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let _ = tracing_subscriber::fmt::try_init();
        let metastore = MetastoreToTest::default_for_test().await;

        let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

        let index_id = "list-splits-index";
        let index_uri = format!("ram://indexes/{index_id}");
        let index_metadata = IndexMetadata::for_test(index_id, &index_uri);

        let split_id_1 = "list-splits-one";
        let split_metadata_1 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_1.to_string(),
            index_id: index_id.to_string(),
            partition_id: 3u64,
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(0..=99),
            create_timestamp: current_timestamp,
            tags: to_set(&["tag!", "tag:foo", "tag:bar"]),
            ..Default::default()
        };

        let split_metadata_2 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "list-splits-two".to_string(),
            index_id: index_id.to_string(),
            partition_id: 3u64,
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(100..=199),
            create_timestamp: current_timestamp,
            tags: to_set(&["tag!", "tag:bar"]),
            ..Default::default()
        };

        let split_metadata_3 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "list-splits-three".to_string(),
            index_id: index_id.to_string(),
            partition_id: 3u64,
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(200..=299),
            create_timestamp: current_timestamp,
            tags: to_set(&["tag!", "tag:foo", "tag:baz"]),
            ..Default::default()
        };

        let split_metadata_4 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "list-splits-four".to_string(),
            index_id: index_id.to_string(),
            partition_id: 3u64,
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(300..=399),
            create_timestamp: current_timestamp,
            tags: to_set(&["tag!", "tag:foo"]),
            ..Default::default()
        };

        let split_metadata_5 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "list-splits-five".to_string(),
            index_id: index_id.to_string(),
            partition_id: 3u64,
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: None,
            create_timestamp: current_timestamp,
            tags: to_set(&["tag!", "tag:baz", "tag:biz"]),
            ..Default::default()
        };

        {
            info!("List all splits on a non-existent index");
            let metastore_err = metastore
                .list_splits("non-existent-index", SplitState::Staged, None, None)
                .await
                .unwrap_err();
            error!(err=?metastore_err);
            assert!(matches!(
                metastore_err,
                MetastoreError::IndexDoesNotExist { .. }
            ));
        }

        {
            info!("List all splits on an index");
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_2.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_3.clone())
                .await
                .unwrap();

            info!("stage split 4");
            metastore
                .stage_split(index_id, split_metadata_4.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_5.clone())
                .await
                .unwrap();

            let splits = metastore
                .list_splits(index_id, SplitState::Staged, Some(0i64..99i64), None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-one", "list-splits-five"])
            );

            let time_range_opt = Some(200..i64::MAX);
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, time_range_opt, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-three", "list-splits-four", "list-splits-five"])
            );
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, Some(i64::MIN..200), None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-one", "list-splits-two", "list-splits-five"])
            );

            let splits = metastore
                .list_splits(index_id, SplitState::Staged, Some(0..100), None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-one", "list-splits-five"])
            );

            let splits = metastore
                .list_splits(index_id, SplitState::Staged, Some(0..101), None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-one", "list-splits-two", "list-splits-five"])
            );

            let splits = metastore
                .list_splits(index_id, SplitState::Staged, Some(0..199), None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-one", "list-splits-two", "list-splits-five"])
            );

            let splits = metastore
                .list_splits(index_id, SplitState::Staged, Some(0..200), None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-one", "list-splits-two", "list-splits-five"])
            );

            let splits = metastore
                .list_splits(index_id, SplitState::Staged, Some(0..201), None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&[
                    "list-splits-one",
                    "list-splits-two",
                    "list-splits-three",
                    "list-splits-five"
                ])
            );

            let splits = metastore
                .list_splits(index_id, SplitState::Staged, Some(0..299), None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&[
                    "list-splits-one",
                    "list-splits-two",
                    "list-splits-three",
                    "list-splits-five"
                ])
            );

            let splits = metastore
                .list_splits(index_id, SplitState::Staged, Some(0..300), None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&[
                    "list-splits-one",
                    "list-splits-two",
                    "list-splits-three",
                    "list-splits-five"
                ])
            );

            let splits = metastore
                .list_splits(index_id, SplitState::Staged, Some(0..301), None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&[
                    "list-splits-one",
                    "list-splits-two",
                    "list-splits-three",
                    "list-splits-four",
                    "list-splits-five"
                ])
            );

            let splits = metastore
                .list_splits(index_id, SplitState::Staged, Some(301..400), None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();

            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-four", "list-splits-five"])
            );
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, Some(300..400), None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();

            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-four", "list-splits-five"])
            );
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, Some(299..400), None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-three", "list-splits-four", "list-splits-five"])
            );

            let splits = metastore
                .list_splits(index_id, SplitState::Staged, Some(201..400), None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-three", "list-splits-four", "list-splits-five"])
            );

            let splits = metastore
                .list_splits(index_id, SplitState::Staged, Some(200..400), None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();

            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-three", "list-splits-four", "list-splits-five"])
            );

            let splits = metastore
                .list_splits(index_id, SplitState::Staged, Some(199..400), None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&[
                    "list-splits-two",
                    "list-splits-three",
                    "list-splits-four",
                    "list-splits-five"
                ])
            );

            let splits = metastore
                .list_splits(index_id, SplitState::Staged, Some(101..400), None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&[
                    "list-splits-two",
                    "list-splits-three",
                    "list-splits-four",
                    "list-splits-five"
                ])
            );

            let splits = metastore
                .list_splits(index_id, SplitState::Staged, Some(101..400), None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&[
                    "list-splits-two",
                    "list-splits-three",
                    "list-splits-four",
                    "list-splits-five"
                ])
            );

            let splits = metastore
                .list_splits(index_id, SplitState::Staged, Some(100..400), None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();

            assert_eq!(
                split_ids,
                to_hash_set(&[
                    "list-splits-two",
                    "list-splits-three",
                    "list-splits-four",
                    "list-splits-five"
                ])
            );

            let splits = metastore
                .list_splits(index_id, SplitState::Staged, Some(99..400), None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&[
                    "list-splits-one",
                    "list-splits-two",
                    "list-splits-three",
                    "list-splits-four",
                    "list-splits-five"
                ])
            );

            let splits = metastore
                .list_splits(index_id, SplitState::Staged, Some(1000..1100), None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(split_ids, to_hash_set(&["list-splits-five"]));

            // add a split without tag
            let split_metadata_6 = SplitMetadata {
                footer_offsets: 1000..2000,
                split_id: "list-splits-six".to_string(),
                index_id: index_id.to_string(),
                partition_id: 3u64,
                num_docs: 1,
                uncompressed_docs_size_in_bytes: 2,
                time_range: None,
                create_timestamp: current_timestamp,
                tags: to_set(&[]),
                ..Default::default()
            };
            metastore
                .stage_split(index_id, split_metadata_6.clone())
                .await
                .unwrap();

            let splits = metastore
                .list_splits(index_id, SplitState::Staged, None, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&[
                    "list-splits-one",
                    "list-splits-two",
                    "list-splits-three",
                    "list-splits-four",
                    "list-splits-five",
                    "list-splits-six",
                ])
            );

            let tag_filter_ast = TagFilterAst::Or(vec![
                TagFilterAst::Or(vec![no_tag("tag!"), tag("tag:bar")]),
                TagFilterAst::Or(vec![no_tag("tag!"), tag("tag:baz")]),
            ]);
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, None, Some(tag_filter_ast))
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|meta| meta.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&[
                    "list-splits-one",
                    "list-splits-two",
                    "list-splits-three",
                    "list-splits-five",
                    "list-splits-six",
                ])
            );
            cleanup_index(&metastore, index_id).await;
        }
    }

    pub async fn test_metastore_split_update_timestamp<
        MetastoreToTest: Metastore + DefaultForTest,
    >() {
        let metastore = MetastoreToTest::default_for_test().await;

        let mut current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

        let index_id = "split-update-timestamp-index";
        let index_uri = format!("ram://indexes/{index_id}");
        let source_id = "split-update-timestamp-source";
        let index_metadata = IndexMetadata::for_test(index_id, &index_uri);

        let split_id = "split-update-timestamp-one";
        let split_metadata = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id.to_string(),
            index_id: index_id.to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(0..=99),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        // Create an index
        metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();

        // wait for 1s, stage split & check `update_timestamp`
        sleep(Duration::from_secs(1)).await;
        metastore
            .stage_split(index_id, split_metadata.clone())
            .await
            .unwrap();
        let split_meta = metastore.list_all_splits(index_id).await.unwrap()[0].clone();
        assert!(split_meta.update_timestamp > current_timestamp);
        assert!(split_meta.publish_timestamp.is_none());
        assert!(
            metastore
                .index_metadata(index_id)
                .await
                .unwrap()
                .update_timestamp
                > current_timestamp
        );

        current_timestamp = split_meta.update_timestamp;

        // wait for 1s, publish split & check `update_timestamp`
        sleep(Duration::from_secs(1)).await;
        metastore
            .publish_splits(
                index_id,
                &[split_id],
                &[],
                {
                    let offsets = 0..5;
                    IndexCheckpointDelta::for_test(source_id, offsets)
                }
                .into(),
            )
            .await
            .unwrap();
        let split_meta = metastore.list_all_splits(index_id).await.unwrap()[0].clone();
        assert!(split_meta.update_timestamp > current_timestamp);
        assert_eq!(
            split_meta.publish_timestamp,
            Some(split_meta.update_timestamp)
        );
        assert!(
            metastore
                .index_metadata(index_id)
                .await
                .unwrap()
                .update_timestamp
                > current_timestamp
        );

        // wait a sec & re-publish and check publish_timestamp has not changed
        let last_publish_timestamp_opt = split_meta.publish_timestamp;
        sleep(Duration::from_secs(1)).await;
        metastore
            .publish_splits(
                index_id,
                &[split_id],
                &[],
                {
                    let offsets = 5..12;
                    IndexCheckpointDelta::for_test(source_id, offsets)
                }
                .into(),
            )
            .await
            .unwrap();
        let split_meta = metastore.list_all_splits(index_id).await.unwrap()[0].clone();
        assert_eq!(split_meta.publish_timestamp, last_publish_timestamp_opt);

        current_timestamp = split_meta.update_timestamp;

        // wait for 1s, mark split for deletion & check `update_timestamp`
        sleep(Duration::from_secs(1)).await;
        metastore
            .mark_splits_for_deletion(index_id, &[split_id])
            .await
            .unwrap();
        let split_meta = metastore.list_all_splits(index_id).await.unwrap()[0].clone();
        assert!(split_meta.update_timestamp > current_timestamp);
        assert!(split_meta.publish_timestamp.is_some());
        assert!(
            metastore
                .index_metadata(index_id)
                .await
                .unwrap()
                .update_timestamp
                > current_timestamp
        );

        current_timestamp = split_meta.update_timestamp;

        // wait for 1s, delete split & check the index `update_timestamp`
        sleep(Duration::from_secs(1)).await;
        metastore
            .delete_splits(index_id, &[split_id])
            .await
            .unwrap();
        assert!(
            metastore
                .index_metadata(index_id)
                .await
                .unwrap()
                .update_timestamp
                > current_timestamp
        );

        cleanup_index(&metastore, index_id).await;
    }

    pub async fn test_metastore_list_indexes<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id_1 = "index-metadata-list-indexes-1";
        let index_uri_1 = format!("ram:///indexes/{index_id_1}");
        let index_metadata_1 = IndexMetadata::for_test(index_id_1, &index_uri_1);
        let index_id_2 = "index-metadata-list-indexes-2";
        let index_uri_2 = format!("ram:///indexes/{index_id_2}");
        let index_metadata_2 = IndexMetadata::for_test(index_id_2, &index_uri_2);
        let index_ids = vec![index_id_1, index_id_2];

        // Get a non-existent index metadata
        let result = metastore
            .list_indexes_metadatas()
            .await
            .unwrap()
            .into_iter()
            .filter(|index_metadata| index_ids.contains(&index_metadata.index_id.as_str()))
            .collect_vec();
        assert!(result.is_empty());

        metastore
            .create_index(index_metadata_1.clone())
            .await
            .unwrap();
        metastore
            .create_index(index_metadata_2.clone())
            .await
            .unwrap();

        // Get list indexes metadata.
        let result = metastore
            .list_indexes_metadatas()
            .await
            .unwrap()
            .into_iter()
            .filter(|index_metadata| index_ids.contains(&index_metadata.index_id.as_str()))
            .collect_vec();
        assert_eq!(2, result.len());

        cleanup_index(&metastore, index_id_1).await;
        cleanup_index(&metastore, index_id_2).await;

        // Check that no index is left.
        let result = metastore
            .list_indexes_metadatas()
            .await
            .unwrap()
            .into_iter()
            .filter(|index_metadata| index_ids.contains(&index_metadata.index_id.as_str()))
            .collect_vec();
        assert!(result.is_empty());
    }

    pub async fn test_metastore_create_delete_task<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;
        let index_id = "add-delete-task";
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_metadata = IndexMetadata::for_test(index_id, &index_uri);
        let delete_query = DeleteQuery {
            index_id: index_id.to_string(),
            query: "my_field:my_value".to_string(),
            start_timestamp: Some(1),
            end_timestamp: Some(2),
            search_fields: Vec::new(),
        };
        metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();

        // Create a delete task.
        let delete_task_1 = metastore
            .create_delete_task(delete_query.clone())
            .await
            .unwrap();
        assert!(delete_task_1.opstamp > 0);
        let delete_query_1 = delete_task_1.delete_query.unwrap();
        assert_eq!(delete_query_1.index_id, delete_query.index_id);
        assert_eq!(delete_query_1.start_timestamp, delete_query.start_timestamp);
        assert_eq!(delete_query_1.end_timestamp, delete_query.end_timestamp);
        let delete_task_2 = metastore
            .create_delete_task(delete_query.clone())
            .await
            .unwrap();
        assert!(delete_task_2.opstamp > delete_task_1.opstamp);

        cleanup_index(&metastore, index_id).await;
    }

    pub async fn test_metastore_last_delete_opstamp<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;
        let index_id_1 = "add-delete-task-1";
        let index_uri_1 = format!("ram:///indexes/{index_id_1}");
        let index_metadata_1 = IndexMetadata::for_test(index_id_1, &index_uri_1);
        let delete_query_index_1 = DeleteQuery {
            index_id: index_id_1.to_string(),
            query: "my_field:my_value".to_string(),
            start_timestamp: Some(1),
            end_timestamp: Some(2),
            search_fields: Vec::new(),
        };
        let index_id_2 = "add-delete-task-2";
        let index_uri_2 = format!("ram:///indexes/{index_id_2}");
        let index_metadata_2 = IndexMetadata::for_test(index_id_2, &index_uri_2);
        let delete_query_index_2 = DeleteQuery {
            index_id: index_id_2.to_string(),
            query: "my_field:my_value".to_string(),
            start_timestamp: Some(1),
            end_timestamp: Some(2),
            search_fields: Vec::new(),
        };
        metastore
            .create_index(index_metadata_1.clone())
            .await
            .unwrap();
        metastore
            .create_index(index_metadata_2.clone())
            .await
            .unwrap();

        let last_opstamp_index_1_with_no_task =
            metastore.last_delete_opstamp(index_id_1).await.unwrap();
        assert_eq!(last_opstamp_index_1_with_no_task, 0);

        // Create a delete task.
        let _ = metastore
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

        let last_opstamp_index_1 = metastore.last_delete_opstamp(index_id_1).await.unwrap();
        let last_opstamp_index_2 = metastore.last_delete_opstamp(index_id_2).await.unwrap();
        assert_eq!(last_opstamp_index_1, delete_task_2.opstamp);
        assert_eq!(last_opstamp_index_2, delete_task_3.opstamp);
        cleanup_index(&metastore, index_id_1).await;
        cleanup_index(&metastore, index_id_2).await;
    }

    pub async fn test_metastore_delete_index_with_tasks<
        MetastoreToTest: Metastore + DefaultForTest,
    >() {
        let metastore = MetastoreToTest::default_for_test().await;
        let index_id = "delete-delete-tasks";
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_metadata = IndexMetadata::for_test(index_id, &index_uri);
        let delete_query = DeleteQuery {
            index_id: index_id.to_string(),
            query: "my_field:my_value".to_string(),
            start_timestamp: Some(1),
            end_timestamp: Some(2),
            search_fields: Vec::new(),
        };
        metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        let _ = metastore
            .create_delete_task(delete_query.clone())
            .await
            .unwrap();
        let _ = metastore
            .create_delete_task(delete_query.clone())
            .await
            .unwrap();

        metastore.delete_index(index_id).await.unwrap();
    }

    pub async fn test_metastore_list_delete_tasks<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;
        let index_id_1 = "list_delete_tasks-1";
        let index_uri_1 = format!("ram:///indexes/{index_id_1}");
        let index_metadata_1 = IndexMetadata::for_test(index_id_1, &index_uri_1);
        let delete_query_index_1 = DeleteQuery {
            index_id: index_id_1.to_string(),
            query: "my_field:my_value".to_string(),
            start_timestamp: Some(1),
            end_timestamp: Some(2),
            search_fields: Vec::new(),
        };
        let index_id_2 = "list_delete_tasks-2";
        let index_uri_2 = format!("ram:///indexes/{index_id_2}");
        let index_metadata_2 = IndexMetadata::for_test(index_id_2, &index_uri_2);
        let delete_query_index_2 = DeleteQuery {
            index_id: index_id_2.to_string(),
            query: "my_field:my_value".to_string(),
            start_timestamp: Some(1),
            end_timestamp: Some(2),
            search_fields: Vec::new(),
        };
        metastore
            .create_index(index_metadata_1.clone())
            .await
            .unwrap();
        metastore
            .create_index(index_metadata_2.clone())
            .await
            .unwrap();

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

        let all_index_id_1_delete_tasks = metastore.list_delete_tasks(index_id_1, 0).await.unwrap();
        assert_eq!(all_index_id_1_delete_tasks.len(), 2);

        let recent_index_id_1_delete_tasks = metastore
            .list_delete_tasks(index_id_1, delete_task_1.opstamp)
            .await
            .unwrap();
        assert_eq!(recent_index_id_1_delete_tasks.len(), 1);
        assert_eq!(
            recent_index_id_1_delete_tasks[0].opstamp,
            delete_task_2.opstamp
        );
        cleanup_index(&metastore, index_id_1).await;
        cleanup_index(&metastore, index_id_2).await;
    }

    pub async fn test_metastore_list_stale_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;
        let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();
        let index_id = "list-stale-splits";
        let index_uri = format!("ram://indexes/{index_id}");
        let index_metadata = IndexMetadata::for_test(index_id, &index_uri);

        let split_id_1 = "list-stale-splits-one";
        let split_metadata_1 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_1.to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            create_timestamp: current_timestamp,
            delete_opstamp: 20,
            ..Default::default()
        };
        let split_id_2 = "list-stale-splits-two";
        let split_metadata_2 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_2.to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            create_timestamp: current_timestamp,
            delete_opstamp: 10,
            ..Default::default()
        };
        let split_id_3 = "list-stale-splits-three";
        let split_metadata_3 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_3.to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            create_timestamp: current_timestamp,
            delete_opstamp: 0,
            ..Default::default()
        };

        {
            info!("List stale splits on a non-existent index");
            let metastore_err = metastore
                .list_stale_splits("non-existent-index", 0, 10)
                .await
                .unwrap_err();
            error!(err=?metastore_err);
            assert!(matches!(
                metastore_err,
                MetastoreError::IndexDoesNotExist { .. }
            ));
        }

        {
            info!("List stale splits on an index");
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_2.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_3.clone())
                .await
                .unwrap();
            metastore
                .publish_splits(index_id, &[split_id_1, split_id_2], &[], None)
                .await
                .unwrap();

            let splits = metastore.list_stale_splits(index_id, 100, 1).await.unwrap();
            assert_eq!(splits.len(), 1);
            assert_eq!(
                splits[0].split_metadata.delete_opstamp,
                split_metadata_2.delete_opstamp
            );

            let splits = metastore.list_stale_splits(index_id, 100, 2).await.unwrap();
            assert_eq!(splits.len(), 2);
            assert_eq!(
                splits[1].split_metadata.delete_opstamp,
                split_metadata_1.delete_opstamp
            );

            let splits = metastore.list_stale_splits(index_id, 20, 2).await.unwrap();
            assert_eq!(splits.len(), 1);
            assert_eq!(
                splits[0].split_metadata.delete_opstamp,
                split_metadata_2.delete_opstamp
            );

            let splits = metastore.list_stale_splits(index_id, 10, 2).await.unwrap();
            assert!(splits.is_empty());
            cleanup_index(&metastore, index_id).await;
        }
    }

    pub async fn test_metastore_update_splits_delete_opstamp<
        MetastoreToTest: Metastore + DefaultForTest,
    >() {
        let metastore = MetastoreToTest::default_for_test().await;
        let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();
        let index_id = "udpate-splits-delete-opstamp";
        let index_uri = format!("ram://indexes/{index_id}");
        let index_metadata = IndexMetadata::for_test(index_id, &index_uri);

        let split_id_1 = "list-stale-splits-one";
        let split_metadata_1 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_1.to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            create_timestamp: current_timestamp,
            delete_opstamp: 20,
            ..Default::default()
        };
        let split_id_2 = "list-stale-splits-two";
        let split_metadata_2 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_2.to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            create_timestamp: current_timestamp,
            delete_opstamp: 10,
            ..Default::default()
        };
        let split_id_3 = "list-stale-splits-three";
        let split_metadata_3 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_3.to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            create_timestamp: current_timestamp,
            delete_opstamp: 0,
            ..Default::default()
        };

        {
            info!("Update splits delete opstamp on a non-existent index.");
            let metastore_err = metastore
                .update_splits_delete_opstamp("non-existent-index", &[split_id_1], 10)
                .await
                .unwrap_err();
            error!(err=?metastore_err);
            assert!(matches!(
                metastore_err,
                MetastoreError::IndexDoesNotExist { .. }
            ));
        }

        {
            info!("Update splits delete opstamp on an index.");
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_2.clone())
                .await
                .unwrap();
            metastore
                .stage_split(index_id, split_metadata_3.clone())
                .await
                .unwrap();
            metastore
                .publish_splits(index_id, &[split_id_1, split_id_2], &[], None)
                .await
                .unwrap();

            let splits = metastore.list_stale_splits(index_id, 100, 2).await.unwrap();
            assert_eq!(splits.len(), 2);

            metastore
                .update_splits_delete_opstamp(index_id, &[split_id_1, split_id_2], 100)
                .await
                .unwrap();

            let splits = metastore.list_stale_splits(index_id, 100, 2).await.unwrap();
            assert_eq!(splits.len(), 0);

            let splits = metastore.list_stale_splits(index_id, 200, 2).await.unwrap();
            assert_eq!(splits.len(), 2);
            assert_eq!(splits[0].split_metadata.delete_opstamp, 100);
            assert_eq!(splits[1].split_metadata.delete_opstamp, 100);

            cleanup_index(&metastore, index_id).await;
        }
    }
}

macro_rules! metastore_test_suite {
    ($metastore_type:ty) => {
        #[cfg(test)]
        mod common_tests {
            #[tokio::test]
            async fn test_metastore_create_index() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_create_index::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_delete_index() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_delete_index::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_index_metadata() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_index_metadata::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_list_indexes() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_list_indexes::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_stage_split() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_stage_split::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_publish_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_publish_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_publish_splits_empty_splits_array_is_allowed() {
                crate::tests::test_suite::test_metastore_publish_splits_empty_splits_array_is_allowed::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_replace_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_replace_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_mark_splits_for_deletion() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_mark_splits_for_deletion::<$metastore_type>()
                .await;
            }

            #[tokio::test]
            async fn test_metastore_delete_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_delete_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_list_all_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_list_all_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_list_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_list_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_split_update_timestamp() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_split_update_timestamp::<$metastore_type>(
                )
                .await;
            }

            #[tokio::test]
            async fn test_metastore_add_source() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_add_source::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_toggle_source() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_toggle_source::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_delete_source() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_delete_source::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_reset_checkpoint() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_reset_checkpoint::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_create_delete_task() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_create_delete_task::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_last_delete_opstamp() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_last_delete_opstamp::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_delete_index_with_tasks() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_delete_index_with_tasks::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_list_delete_tasks() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_list_delete_tasks::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_list_stale_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_list_stale_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_update_splits_delete_opstamp() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_update_splits_delete_opstamp::<$metastore_type>().await;
            }
        }
    }
}
