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

#[cfg(test)]
pub mod test_suite {
    use std::collections::BTreeSet;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use futures::future::try_join_all;
    use itertools::Itertools;
    use quickwit_common::rand::append_random_suffix;
    use quickwit_config::{IndexConfig, SourceConfig, SourceParams};
    use quickwit_doc_mapper::tag_pruning::{no_tag, tag, TagFilterAst};
    use quickwit_proto::metastore_api::DeleteQuery;
    use time::OffsetDateTime;
    use tokio::time::sleep;
    use tracing::{error, info};

    use crate::checkpoint::{
        IndexCheckpointDelta, PartitionId, Position, SourceCheckpoint, SourceCheckpointDelta,
    };
    use crate::{ListSplitsQuery, Metastore, MetastoreError, Split, SplitMetadata, SplitState};

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

    async fn cleanup_index(metastore: &dyn Metastore, index_id: &str) {
        // List all splits.
        let all_splits = metastore.list_all_splits(index_id).await.unwrap();

        if !all_splits.is_empty() {
            let all_split_ids: Vec<&str> =
                all_splits.iter().map(|split| split.split_id()).collect();

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

    // Index API tests
    //
    //  - create_index
    //  - index_exists
    //  - index_metadata
    //  - list_indexes
    //  - delete_index

    pub async fn test_metastore_create_index<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = append_random_suffix("test-create-index");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        metastore.create_index(index_config.clone()).await.unwrap();

        assert!(metastore.index_exists(&index_id).await.unwrap());

        let index_metadata = metastore.index_metadata(&index_id).await.unwrap();

        assert_eq!(index_metadata.index_id(), index_id);
        assert_eq!(index_metadata.index_uri(), &index_uri);

        let error = metastore.create_index(index_config).await.unwrap_err();
        assert!(matches!(error, MetastoreError::IndexAlreadyExists { .. }));

        cleanup_index(&metastore, &index_id).await;
    }

    pub async fn test_metastore_index_exists<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = append_random_suffix("test-index-exists");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        assert!(!metastore.index_exists(&index_id).await.unwrap());

        metastore.create_index(index_config).await.unwrap();

        assert!(metastore.index_exists(&index_id).await.unwrap());

        cleanup_index(&metastore, &index_id).await;
    }

    pub async fn test_metastore_index_metadata<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = append_random_suffix("test-index-metadata");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        let error = metastore
            .index_metadata("index-not-found")
            .await
            .unwrap_err();
        assert!(matches!(error, MetastoreError::IndexDoesNotExist { .. }));

        metastore.create_index(index_config.clone()).await.unwrap();

        let index_metadata = metastore.index_metadata(&index_id).await.unwrap();

        assert_eq!(index_metadata.index_id(), index_id);
        assert_eq!(index_metadata.index_uri(), &index_uri);

        cleanup_index(&metastore, &index_id).await;
    }

    pub async fn test_metastore_list_indexes<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id_suffix = append_random_suffix("test-list-indexes");
        let index_id_1 = format!("{index_id_suffix}-1");
        let index_uri_1 = format!("ram:///indexes/{index_id_1}");
        let index_config_1 = IndexConfig::for_test(&index_id_1, &index_uri_1);

        let index_id_2 = format!("{index_id_suffix}-2");
        let index_uri_2 = format!("ram:///indexes/{index_id_2}");
        let index_config_2 = IndexConfig::for_test(&index_id_2, &index_uri_2);

        let indexes_count = metastore
            .list_indexes_metadatas()
            .await
            .unwrap()
            .into_iter()
            .filter(|index| index.index_id().starts_with(&index_id_suffix))
            .count();
        assert_eq!(indexes_count, 0);

        metastore.create_index(index_config_1).await.unwrap();
        metastore.create_index(index_config_2).await.unwrap();

        let indexes_count = metastore
            .list_indexes_metadatas()
            .await
            .unwrap()
            .into_iter()
            .filter(|index| index.index_id().starts_with(&index_id_suffix))
            .count();
        assert_eq!(indexes_count, 2);

        cleanup_index(&metastore, &index_id_1).await;
        cleanup_index(&metastore, &index_id_2).await;
    }

    pub async fn test_metastore_delete_index<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = append_random_suffix("test-delete-index");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        let error = metastore.delete_index("index-not-found").await.unwrap_err();
        assert!(matches!(error, MetastoreError::IndexDoesNotExist { .. }));

        metastore.create_index(index_config.clone()).await.unwrap();

        metastore.delete_index(&index_id).await.unwrap();

        assert!(!metastore.index_exists(&index_id).await.unwrap());

        let split_id = format!("{index_id}--split");
        let split_metadata = SplitMetadata {
            split_id: split_id.clone(),
            index_id: index_id.clone(),
            ..Default::default()
        };

        metastore.create_index(index_config).await.unwrap();

        metastore
            .stage_splits(&index_id, vec![split_metadata])
            .await
            .unwrap();

        // TODO: We should not be able to delete an index that has remaining splits, at least not as
        // a default behavior. Let's implement the logic that allows this test to pass.
        // let error = metastore.delete_index(&index_id).await.unwrap_err();
        // assert!(matches!(error, MetastoreError::IndexNotEmpty { .. }));
        // let splits = metastore.list_all_splits(&index_id).await.unwrap();
        // assert_eq!(splits.len(), 1)

        cleanup_index(&metastore, &index_id).await;
    }

    pub async fn test_metastore_add_source<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = append_random_suffix("test-add-source");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        metastore.create_index(index_config.clone()).await.unwrap();

        let source_id = format!("{index_id}--source");

        let source = SourceConfig {
            source_id: source_id.to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
            desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::void(),
            transform_config: None,
        };

        assert_eq!(
            metastore
                .index_metadata(&index_id)
                .await
                .unwrap()
                .checkpoint
                .source_checkpoint(&source_id),
            None
        );

        metastore
            .add_source(&index_id, source.clone())
            .await
            .unwrap();

        let index_metadata = metastore.index_metadata(&index_id).await.unwrap();

        let sources = &index_metadata.sources;
        assert_eq!(sources.len(), 1);
        assert!(sources.contains_key(&source_id));
        assert_eq!(sources.get(&source_id).unwrap().source_id, source_id);
        assert_eq!(sources.get(&source_id).unwrap().source_type(), "void");
        assert_eq!(
            index_metadata.checkpoint.source_checkpoint(&source_id),
            Some(&SourceCheckpoint::default())
        );

        assert!(matches!(
            metastore
                .add_source(&index_id, source.clone())
                .await
                .unwrap_err(),
            MetastoreError::SourceAlreadyExists { .. }
        ));
        assert!(matches!(
            metastore
                .add_source("index-not-found", source)
                .await
                .unwrap_err(),
            MetastoreError::IndexDoesNotExist { .. }
        ));
        cleanup_index(&metastore, index_metadata.index_id()).await;
    }

    pub async fn test_metastore_toggle_source<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = append_random_suffix("test-toggle-source");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        metastore.create_index(index_config.clone()).await.unwrap();

        let source_id = format!("{index_id}--source");
        let source = SourceConfig {
            source_id: source_id.to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
            desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::void(),
            transform_config: None,
        };
        metastore
            .add_source(&index_id, source.clone())
            .await
            .unwrap();
        let index_metadata = metastore.index_metadata(&index_id).await.unwrap();
        let source = index_metadata.sources.get(&source_id).unwrap();
        assert_eq!(source.enabled, true);

        // Disable source.
        metastore
            .toggle_source(&index_id, &source.source_id, false)
            .await
            .unwrap();
        let index_metadata = metastore.index_metadata(&index_id).await.unwrap();
        let source = index_metadata.sources.get(&source_id).unwrap();
        assert_eq!(source.enabled, false);

        // Enable source.
        metastore
            .toggle_source(&index_id, &source.source_id, true)
            .await
            .unwrap();
        let index_metadata = metastore.index_metadata(&index_id).await.unwrap();
        let source = index_metadata.sources.get(&source_id).unwrap();
        assert_eq!(source.enabled, true);

        cleanup_index(&metastore, index_metadata.index_id()).await;
    }

    pub async fn test_metastore_delete_source<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

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
        };

        let index_config = IndexConfig::for_test(&index_id, index_uri.as_str());

        metastore.create_index(index_config.clone()).await.unwrap();
        metastore.add_source(&index_id, source).await.unwrap();
        metastore
            .delete_source(&index_id, &source_id)
            .await
            .unwrap();

        let sources = metastore.index_metadata(&index_id).await.unwrap().sources;
        assert!(sources.is_empty());

        assert!(matches!(
            metastore
                .delete_source(&index_id, &source_id)
                .await
                .unwrap_err(),
            MetastoreError::SourceDoesNotExist { .. }
        ));
        assert!(matches!(
            metastore
                .delete_source("index-not-found", &source_id)
                .await
                .unwrap_err(),
            MetastoreError::IndexDoesNotExist { .. }
        ));

        cleanup_index(&metastore, &index_config.index_id).await;
    }

    pub async fn test_metastore_reset_checkpoint<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = append_random_suffix("test-reset-checkpoint");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        metastore.create_index(index_config.clone()).await.unwrap();

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
                .stage_splits(&index_id, vec![split_metadata])
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

        cleanup_index(&metastore, index_metadata.index_id()).await;
    }

    pub async fn test_metastore_publish_splits_empty_splits_array_is_allowed<
        MetastoreToTest: Metastore + DefaultForTest,
    >() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = append_random_suffix("test-publish-splits-empty");
        let index_uri = format!("ram:///indexes/{index_id}");

        let source_id = format!("{index_id}--source");

        // Publish a split on a non-existent index
        {
            let error = metastore
                .publish_splits(
                    "index-not-found",
                    &[],
                    &[],
                    {
                        let offsets = 1..10;
                        IndexCheckpointDelta::for_test(&source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap_err();
            assert!(matches!(error, MetastoreError::IndexDoesNotExist { .. }));
        }

        // Update the checkpoint, by publishing an empty array of splits with a non-empty
        // checkpoint. This operation is allowed and used in the Indexer.
        {
            let index_config = IndexConfig::for_test(&index_id, &index_uri);
            metastore.create_index(index_config.clone()).await.unwrap();

            metastore
                .publish_splits(
                    &index_id,
                    &[],
                    &[],
                    {
                        let offsets = 0..100;
                        IndexCheckpointDelta::for_test(&source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap();

            let index_metadata = metastore.index_metadata(&index_id).await.unwrap();
            let source_checkpoint = index_metadata
                .checkpoint
                .source_checkpoint(&source_id)
                .unwrap();
            assert_eq!(source_checkpoint.num_partitions(), 1);
            assert_eq!(
                source_checkpoint
                    .position_for_partition(&PartitionId::default())
                    .unwrap(),
                &Position::from(100u64 - 1)
            );
            cleanup_index(&metastore, &index_id).await;
        }
    }

    pub async fn test_metastore_publish_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

        let index_id = append_random_suffix("test-publish-splits");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        let source_id = format!("{index_id}--source");

        let split_id_1 = format!("{index_id}--split-1");
        let split_metadata_1 = SplitMetadata {
            split_id: split_id_1.clone(),
            index_id: index_id.clone(),
            time_range: Some(0..=99),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        let split_id_2 = format!("{index_id}--split-2");
        let split_metadata_2 = SplitMetadata {
            split_id: split_id_2.clone(),
            index_id: index_id.clone(),
            time_range: Some(30..=99),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        // Publish a split on a non-existent index
        {
            let error = metastore
                .publish_splits(
                    "index-not-found",
                    &["split-not-found"],
                    &[],
                    {
                        let offsets = 1..10;
                        IndexCheckpointDelta::for_test(&source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap_err();
            assert!(matches!(error, MetastoreError::IndexDoesNotExist { .. }));
        }

        // Publish a non-existent split on an index
        {
            metastore.create_index(index_config.clone()).await.unwrap();

            let error = metastore
                .publish_splits(&index_id, &["split-not-found"], &[], None)
                .await
                .unwrap_err();
            assert!(matches!(error, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, &index_id).await;
        }

        // Publish a staged split on an index
        {
            metastore.create_index(index_config.clone()).await.unwrap();

            metastore
                .stage_splits(&index_id, vec![split_metadata_1.clone()])
                .await
                .unwrap();

            metastore
                .publish_splits(&index_id, &[&split_id_1], &[], None)
                .await
                .unwrap();

            cleanup_index(&metastore, &index_id).await;
        }

        // Publish a published split on an index
        {
            metastore.create_index(index_config.clone()).await.unwrap();

            metastore
                .stage_splits(&index_id, vec![split_metadata_1.clone()])
                .await
                .unwrap();

            metastore
                .publish_splits(
                    &index_id,
                    &[&split_id_1],
                    &[],
                    {
                        let offsets = 1..12;
                        IndexCheckpointDelta::for_test(&source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap();

            let error = metastore
                .publish_splits(
                    &index_id,
                    &[&split_id_1],
                    &[],
                    {
                        let offsets = 1..12;
                        IndexCheckpointDelta::for_test(&source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                MetastoreError::IncompatibleCheckpointDelta(_)
            ));

            cleanup_index(&metastore, &index_id).await;
        }

        // Publish a non-staged split on an index
        {
            metastore.create_index(index_config.clone()).await.unwrap();

            metastore
                .stage_splits(&index_id, vec![split_metadata_1.clone()])
                .await
                .unwrap();

            metastore
                .publish_splits(
                    &index_id,
                    &[&split_id_1],
                    &[],
                    {
                        let offsets = 12..15;
                        IndexCheckpointDelta::for_test(&source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap();

            metastore
                .mark_splits_for_deletion(&index_id, &[&split_id_1])
                .await
                .unwrap();

            let error = metastore
                .publish_splits(
                    &index_id,
                    &[&split_id_1],
                    &[],
                    {
                        let offsets = 15..18;
                        IndexCheckpointDelta::for_test(&source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap_err();
            assert!(matches!(error, MetastoreError::SplitsNotStaged { .. }));

            cleanup_index(&metastore, &index_id).await;
        }

        // Publish a staged split and non-existent split on an index
        {
            metastore.create_index(index_config.clone()).await.unwrap();

            metastore
                .stage_splits(&index_id, vec![split_metadata_1.clone()])
                .await
                .unwrap();

            let error = metastore
                .publish_splits(
                    &index_id,
                    &[&split_id_1, "split-not-found"],
                    &[],
                    {
                        let offsets = 15..18;
                        IndexCheckpointDelta::for_test(&source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap_err();
            assert!(matches!(error, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, &index_id).await;
        }

        // Publish a published split and non-existent split on an index
        {
            metastore.create_index(index_config.clone()).await.unwrap();

            metastore
                .stage_splits(&index_id, vec![split_metadata_1.clone()])
                .await
                .unwrap();

            metastore
                .publish_splits(
                    &index_id,
                    &[&split_id_1],
                    &[],
                    {
                        let offsets = 15..18;
                        IndexCheckpointDelta::for_test(&source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap();

            let error = metastore
                .publish_splits(
                    &index_id,
                    &[&split_id_1, "split-not-found"],
                    &[],
                    {
                        let offsets = 18..24;
                        IndexCheckpointDelta::for_test(&source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap_err();
            assert!(matches!(error, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, &index_id).await;
        }

        // Publish a non-staged split and non-existent split on an index
        {
            metastore.create_index(index_config.clone()).await.unwrap();

            metastore
                .stage_splits(&index_id, vec![split_metadata_1.clone()])
                .await
                .unwrap();

            metastore
                .publish_splits(
                    &index_id,
                    &[&split_id_1],
                    &[],
                    {
                        let offsets = 18..24;
                        IndexCheckpointDelta::for_test(&source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap();

            metastore
                .mark_splits_for_deletion(&index_id, &[&split_id_1])
                .await
                .unwrap();

            let error = metastore
                .publish_splits(
                    &index_id,
                    &[&split_id_1, "split-not-found"],
                    &[],
                    {
                        let offsets = 24..26;
                        IndexCheckpointDelta::for_test(&source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap_err();
            assert!(matches!(error, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, &index_id).await;
        }

        // Publish staged splits on an index
        {
            metastore.create_index(index_config.clone()).await.unwrap();

            metastore
                .stage_splits(&index_id, vec![split_metadata_1.clone()])
                .await
                .unwrap();

            metastore
                .stage_splits(&index_id, vec![split_metadata_2.clone()])
                .await
                .unwrap();

            metastore
                .publish_splits(
                    &index_id,
                    &[&split_id_1, &split_id_2],
                    &[],
                    {
                        let offsets = 24..26;
                        IndexCheckpointDelta::for_test(&source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap();

            cleanup_index(&metastore, &index_id).await;
        }

        // Publish a staged split and published split on an index
        {
            metastore.create_index(index_config.clone()).await.unwrap();

            metastore
                .stage_splits(
                    &index_id,
                    vec![split_metadata_1.clone(), split_metadata_2.clone()],
                )
                .await
                .unwrap();

            metastore
                .publish_splits(
                    &index_id,
                    &[&split_id_2],
                    &[],
                    {
                        let offsets = 26..28;
                        IndexCheckpointDelta::for_test(&source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap();

            let error = metastore
                .publish_splits(
                    &index_id,
                    &[&split_id_1, &split_id_2],
                    &[],
                    {
                        let offsets = 28..30;
                        IndexCheckpointDelta::for_test(&source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap_err();
            assert!(matches!(error, MetastoreError::SplitsNotStaged { .. }));

            cleanup_index(&metastore, &index_id).await;
        }

        // Publish published splits on an index
        {
            metastore.create_index(index_config.clone()).await.unwrap();

            metastore
                .stage_splits(
                    &index_id,
                    vec![split_metadata_1.clone(), split_metadata_2.clone()],
                )
                .await
                .unwrap();

            metastore
                .publish_splits(
                    &index_id,
                    &[&split_id_1, &split_id_2],
                    &[],
                    {
                        let offsets = 30..31;
                        IndexCheckpointDelta::for_test(&source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap();

            let error = metastore
                .publish_splits(
                    &index_id,
                    &[&split_id_1, &split_id_2],
                    &[],
                    {
                        let offsets = 30..31;
                        IndexCheckpointDelta::for_test(&source_id, offsets)
                    }
                    .into(),
                )
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                MetastoreError::IncompatibleCheckpointDelta(_)
            ));

            cleanup_index(&metastore, &index_id).await;
        }
    }

    pub async fn test_metastore_publish_splits_concurrency<
        MetastoreToTest: Metastore + DefaultForTest,
    >() {
        let metastore: Arc<dyn Metastore> = Arc::new(MetastoreToTest::default_for_test().await);

        let index_id = append_random_suffix("test-publish-concurrency");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        metastore.create_index(index_config).await.unwrap();

        let source_id = format!("{index_id}--source");

        let mut join_handles = Vec::with_capacity(10);

        for partition_id in 0..10 {
            let metastore = metastore.clone();
            let index_id = index_id.clone();
            let source_id = source_id.clone();

            let join_handle = tokio::spawn(async move {
                let split_id = format!("{index_id}--split-{partition_id}");
                let split_metadata = SplitMetadata {
                    split_id: split_id.clone(),
                    index_id: index_id.clone(),
                    ..Default::default()
                };
                metastore
                    .stage_splits(&index_id, vec![split_metadata])
                    .await
                    .unwrap();
                let source_delta = SourceCheckpointDelta::from_partition_delta(
                    PartitionId::from(partition_id as u64),
                    Position::Beginning,
                    Position::from(partition_id as u64),
                )
                .unwrap();
                let checkpoint_delta = IndexCheckpointDelta {
                    source_id,
                    source_delta,
                };
                metastore
                    .publish_splits(&index_id, &[&split_id], &[], Some(checkpoint_delta))
                    .await
                    .unwrap();
            });
            join_handles.push(join_handle);
        }
        try_join_all(join_handles).await.unwrap();

        let index_metadata = metastore.index_metadata(&index_id).await.unwrap();
        let source_checkpoint = index_metadata
            .checkpoint
            .source_checkpoint(&source_id)
            .unwrap();

        assert_eq!(source_checkpoint.num_partitions(), 10);

        cleanup_index(metastore.as_ref(), &index_id).await
    }

    pub async fn test_metastore_replace_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

        let index_id = append_random_suffix("test-replace-splits");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        let split_id_1 = format!("{index_id}--split-1");
        let split_metadata_1 = SplitMetadata {
            split_id: split_id_1.clone(),
            index_id: index_id.clone(),
            time_range: None,
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        let split_id_2 = format!("{index_id}--split-2");
        let split_metadata_2 = SplitMetadata {
            split_id: split_id_2.clone(),
            index_id: index_id.clone(),
            time_range: None,
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        let split_id_3 = format!("{index_id}--split-3");
        let split_metadata_3 = SplitMetadata {
            split_id: split_id_3.clone(),
            index_id: index_id.clone(),
            time_range: None,
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        // Replace splits on a non-existent index
        {
            let error = metastore
                .publish_splits(
                    "index-not-found",
                    &["split-not-found-1"],
                    &["split-not-found-2"],
                    None,
                )
                .await
                .unwrap_err();
            assert!(matches!(error, MetastoreError::IndexDoesNotExist { .. }));
        }

        // Replace a non-existent split on an index
        {
            metastore.create_index(index_config.clone()).await.unwrap();

            // TODO source id
            let error = metastore
                .publish_splits(
                    &index_id,
                    &["split-not-found-1"],
                    &["split-not-found-2"],
                    None,
                )
                .await
                .unwrap_err();
            assert!(matches!(error, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, &index_id).await;
        }

        // Replace a publish split with a non existing split
        {
            metastore.create_index(index_config.clone()).await.unwrap();

            metastore
                .stage_splits(&index_id, vec![split_metadata_1.clone()])
                .await
                .unwrap();

            metastore
                .publish_splits(&index_id, &[&split_id_1], &[], None)
                .await
                .unwrap();

            // TODO Source id
            let error = metastore
                .publish_splits(&index_id, &[&split_id_2], &[&split_id_1], None)
                .await
                .unwrap_err();
            assert!(matches!(error, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, &index_id).await;
        }

        // Replace a publish split with a deleted split
        {
            metastore.create_index(index_config.clone()).await.unwrap();

            metastore
                .stage_splits(
                    &index_id,
                    vec![split_metadata_1.clone(), split_metadata_2.clone()],
                )
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
            let error = metastore
                .publish_splits(&index_id, &[&split_id_2], &[&split_id_1], None)
                .await
                .unwrap_err();
            assert!(matches!(error, MetastoreError::SplitsNotStaged { .. }));

            cleanup_index(&metastore, &index_id).await;
        }

        // Replace a publish split with mixed splits
        {
            metastore.create_index(index_config.clone()).await.unwrap();

            metastore
                .stage_splits(&index_id, vec![split_metadata_1.clone()])
                .await
                .unwrap();

            metastore
                .publish_splits(&index_id, &[&split_id_1], &[], None)
                .await
                .unwrap();

            metastore
                .stage_splits(&index_id, vec![split_metadata_2.clone()])
                .await
                .unwrap();

            let error = metastore
                .publish_splits(&index_id, &[&split_id_2, &split_id_3], &[&split_id_1], None) // TODO source id
                .await
                .unwrap_err();
            assert!(matches!(error, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, &index_id).await;
        }

        // Replace a deleted split with a new split
        {
            metastore.create_index(index_config.clone()).await.unwrap();

            metastore
                .stage_splits(&index_id, vec![split_metadata_1.clone()])
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
                .stage_splits(&index_id, vec![split_metadata_2.clone()])
                .await
                .unwrap();

            let error = metastore
                .publish_splits(&index_id, &[&split_id_2], &[&split_id_1], None)
                .await
                .unwrap_err();
            assert!(
                matches!(error, MetastoreError::SplitsNotDeletable { split_ids } if split_ids == [split_id_1.clone()])
            );

            cleanup_index(&metastore, &index_id).await;
        }

        // Replace a publish split with staged splits
        {
            metastore.create_index(index_config.clone()).await.unwrap();

            metastore
                .stage_splits(&index_id, vec![split_metadata_1.clone()])
                .await
                .unwrap();

            metastore
                .publish_splits(&index_id, &[&split_id_1], &[], None)
                .await
                .unwrap();

            metastore
                .stage_splits(
                    &index_id,
                    vec![split_metadata_2.clone(), split_metadata_3.clone()],
                )
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
        let metastore = MetastoreToTest::default_for_test().await;

        let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

        let index_id = append_random_suffix("test-mark-splits-for-deletion");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        metastore.create_index(index_config).await.unwrap();

        let error = metastore
            .mark_splits_for_deletion("index-not-found", &[])
            .await
            .unwrap_err();
        assert!(matches!(error, MetastoreError::IndexDoesNotExist { .. }));

        metastore
            .mark_splits_for_deletion(&index_id, &["split-not-found"])
            .await
            .unwrap();

        let split_id_1 = format!("{index_id}--split-1");
        let split_metadata_1 = SplitMetadata {
            split_id: split_id_1.clone(),
            index_id: index_id.clone(),
            create_timestamp: current_timestamp,
            ..Default::default()
        };
        metastore
            .stage_splits(&index_id, vec![split_metadata_1])
            .await
            .unwrap();

        let split_id_2 = format!("{index_id}--split-2");
        let split_metadata_2 = SplitMetadata {
            split_id: split_id_2.clone(),
            index_id: index_id.clone(),
            create_timestamp: current_timestamp,
            ..Default::default()
        };
        metastore
            .stage_splits(&index_id, vec![split_metadata_2])
            .await
            .unwrap();
        metastore
            .publish_splits(&index_id, &[&split_id_2], &[], None)
            .await
            .unwrap();

        let split_id_3 = format!("{index_id}--split-3");
        let split_metadata_3 = SplitMetadata {
            split_id: split_id_3.clone(),
            index_id: index_id.clone(),
            create_timestamp: current_timestamp,
            ..Default::default()
        };
        metastore
            .stage_splits(&index_id, vec![split_metadata_3])
            .await
            .unwrap();
        metastore
            .publish_splits(&index_id, &[&split_id_3], &[], None)
            .await
            .unwrap();

        // Sleep for 1s so we can observe the timestamp update.
        sleep(Duration::from_secs(1)).await;

        metastore
            .mark_splits_for_deletion(&index_id, &[&split_id_3])
            .await
            .unwrap();

        let marked_splits = metastore
            .list_splits(
                ListSplitsQuery::for_index(&index_id)
                    .with_split_state(SplitState::MarkedForDeletion),
            )
            .await
            .unwrap();

        assert_eq!(marked_splits.len(), 1);
        assert_eq!(marked_splits[0].split_id(), split_id_3);

        let split_3_update_timestamp = marked_splits[0].update_timestamp;
        assert!(current_timestamp < split_3_update_timestamp);

        // Sleep for 1s so we can observe the timestamp update.
        sleep(Duration::from_secs(1)).await;

        metastore
            .mark_splits_for_deletion(
                &index_id,
                &[&split_id_1, &split_id_2, &split_id_3, "split-not-found"],
            )
            .await
            .unwrap();

        let mut marked_splits = metastore
            .list_splits(
                ListSplitsQuery::for_index(&index_id)
                    .with_split_state(SplitState::MarkedForDeletion),
            )
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

        cleanup_index(&metastore, &index_id).await;
    }

    pub async fn test_metastore_delete_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = append_random_suffix("test-delete-splits");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        metastore.create_index(index_config).await.unwrap();

        let error = metastore
            .delete_splits("index-not-found", &[])
            .await
            .unwrap_err();

        assert!(matches!(error, MetastoreError::IndexDoesNotExist { .. }));

        metastore
            .delete_splits(&index_id, &["split-not-found"])
            .await
            .unwrap();

        let split_id_1 = format!("{index_id}--split-1");
        let split_metadata_1 = SplitMetadata {
            split_id: split_id_1.clone(),
            index_id: index_id.clone(),
            ..Default::default()
        };
        metastore
            .stage_splits(&index_id, vec![split_metadata_1])
            .await
            .unwrap();
        metastore
            .publish_splits(&index_id, &[&split_id_1], &[], None)
            .await
            .unwrap();

        let split_id_2 = format!("{index_id}--split-2");
        let split_metadata_2 = SplitMetadata {
            split_id: split_id_2.clone(),
            index_id: index_id.clone(),
            ..Default::default()
        };
        metastore
            .stage_splits(&index_id, vec![split_metadata_2])
            .await
            .unwrap();

        let error = metastore
            .delete_splits(&index_id, &[&split_id_1, &split_id_2])
            .await
            .unwrap_err();

        assert!(matches!(error, MetastoreError::SplitsNotDeletable { .. }));

        assert_eq!(metastore.list_all_splits(&index_id).await.unwrap().len(), 2);

        metastore
            .mark_splits_for_deletion(&index_id, &[&split_id_1, &split_id_2])
            .await
            .unwrap();

        metastore
            .delete_splits(&index_id, &[&split_id_1, &split_id_2, "split-not-found"])
            .await
            .unwrap();

        assert_eq!(metastore.list_all_splits(&index_id).await.unwrap().len(), 0);

        cleanup_index(&metastore, &index_id).await;
    }

    pub async fn test_metastore_list_all_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = append_random_suffix("test-list-all-splits");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        let split_id_1 = format!("{index_id}--split-1");
        let split_metadata_1 = SplitMetadata {
            split_id: split_id_1.clone(),
            index_id: index_id.clone(),
            ..Default::default()
        };
        let split_id_2 = format!("{index_id}--split-2");
        let split_metadata_2 = SplitMetadata {
            split_id: split_id_2.clone(),
            index_id: index_id.clone(),
            ..Default::default()
        };
        let split_id_3 = format!("{index_id}--split-3");
        let split_metadata_3 = SplitMetadata {
            split_id: split_id_3.clone(),
            index_id: index_id.clone(),
            ..Default::default()
        };
        let split_id_4 = format!("{index_id}--split-4");
        let split_metadata_4 = SplitMetadata {
            split_id: split_id_4.clone(),
            index_id: index_id.clone(),
            ..Default::default()
        };
        let split_id_5 = format!("{index_id}--split-5");
        let split_metadata_5 = SplitMetadata {
            split_id: split_id_5.clone(),
            index_id: index_id.clone(),
            ..Default::default()
        };
        let split_id_6 = format!("{index_id}--split-6");
        let split_metadata_6 = SplitMetadata {
            split_id: split_id_6.clone(),
            index_id: index_id.clone(),
            ..Default::default()
        };

        let error = metastore
            .list_all_splits("index-not-found")
            .await
            .unwrap_err();
        assert!(matches!(error, MetastoreError::IndexDoesNotExist { .. }));

        metastore.create_index(index_config).await.unwrap();

        metastore
            .stage_splits(
                &index_id,
                vec![
                    split_metadata_1,
                    split_metadata_2,
                    split_metadata_3,
                    split_metadata_4,
                    split_metadata_5,
                    split_metadata_6,
                ],
            )
            .await
            .unwrap();

        metastore
            .publish_splits(&index_id, &[&split_id_1, &split_id_2], &[], None)
            .await
            .unwrap();

        metastore
            .mark_splits_for_deletion(&index_id, &[&split_id_3, &split_id_4])
            .await
            .unwrap();

        let splits = metastore.list_all_splits(&index_id).await.unwrap();
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

        cleanup_index(&metastore, &index_id).await;
    }

    pub async fn test_metastore_list_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

        let index_id = append_random_suffix("test-list-splits");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        let split_id_1 = format!("{index_id}--split-1");
        let split_metadata_1 = SplitMetadata {
            split_id: split_id_1.clone(),
            index_id: index_id.clone(),
            time_range: Some(0..=99),
            create_timestamp: current_timestamp,
            tags: to_btree_set(&["tag!", "tag:foo", "tag:bar"]),
            delete_opstamp: 3,
            ..Default::default()
        };

        let split_id_2 = format!("{index_id}--split-2");
        let split_metadata_2 = SplitMetadata {
            split_id: split_id_2.clone(),
            index_id: index_id.clone(),
            time_range: Some(100..=199),
            create_timestamp: current_timestamp,
            tags: to_btree_set(&["tag!", "tag:bar"]),
            delete_opstamp: 1,
            ..Default::default()
        };

        let split_id_3 = format!("{index_id}--split-3");
        let split_metadata_3 = SplitMetadata {
            split_id: split_id_3.clone(),
            index_id: index_id.clone(),
            time_range: Some(200..=299),
            create_timestamp: current_timestamp,
            tags: to_btree_set(&["tag!", "tag:foo", "tag:baz"]),
            delete_opstamp: 5,
            ..Default::default()
        };

        let split_id_4 = format!("{index_id}--split-4");
        let split_metadata_4 = SplitMetadata {
            split_id: split_id_4.clone(),
            index_id: index_id.clone(),
            time_range: Some(300..=399),
            tags: to_btree_set(&["tag!", "tag:foo"]),
            delete_opstamp: 7,
            ..Default::default()
        };

        let split_id_5 = format!("{index_id}--split-5");
        let split_metadata_5 = SplitMetadata {
            split_id: split_id_5.clone(),
            index_id: index_id.clone(),
            time_range: None,
            create_timestamp: current_timestamp,
            tags: to_btree_set(&["tag!", "tag:baz", "tag:biz"]),
            delete_opstamp: 9,
            ..Default::default()
        };

        {
            let query = ListSplitsQuery::for_index(&index_id).with_split_state(SplitState::Staged);
            let error = metastore.list_splits(query).await.unwrap_err();
            assert!(matches!(error, MetastoreError::IndexDoesNotExist { .. }));
        }
        {
            metastore.create_index(index_config.clone()).await.unwrap();

            metastore
                .stage_splits(
                    &index_id,
                    vec![
                        split_metadata_1.clone(),
                        split_metadata_2.clone(),
                        split_metadata_3.clone(),
                        split_metadata_4.clone(),
                        split_metadata_5.clone(),
                    ],
                )
                .await
                .unwrap();

            let query = ListSplitsQuery::for_index(&index_id).with_limit(3);
            let splits = metastore.list_splits(query).await.unwrap();
            assert_eq!(
                splits.len(),
                3,
                "Expected number of splits returned to match limit.",
            );

            let query = ListSplitsQuery::for_index(&index_id).with_offset(3);
            let splits = metastore.list_splits(query).await.unwrap();
            assert_eq!(
                splits.len(),
                2,
                "Expected 3 splits to be skipped out of the 5 provided splits.",
            );

            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_time_range_start_gte(0)
                .with_time_range_end_lt(99);

            let splits = metastore.list_splits(query).await.unwrap();
            let split_ids: Vec<&str> = splits
                .iter()
                .map(|split| split.split_id())
                .sorted()
                .collect();
            assert_eq!(split_ids, &[&split_id_1, &split_id_5]);

            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_time_range_start_gte(200);

            let splits = metastore.list_splits(query).await.unwrap();
            let split_ids = collect_split_ids(&splits);
            assert_eq!(split_ids, &[&split_id_3, &split_id_4, &split_id_5]);

            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_time_range_end_lt(200);
            let splits = metastore.list_splits(query).await.unwrap();
            let split_ids = collect_split_ids(&splits);
            assert_eq!(split_ids, &[&split_id_1, &split_id_2, &split_id_5]);

            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_time_range_start_gte(0)
                .with_time_range_end_lt(100);
            let splits = metastore.list_splits(query).await.unwrap();
            let split_ids = collect_split_ids(&splits);
            assert_eq!(split_ids, &[&split_id_1, &split_id_5]);

            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_time_range_start_gte(0)
                .with_time_range_end_lt(101);
            let splits = metastore.list_splits(query).await.unwrap();
            let split_ids = collect_split_ids(&splits);
            assert_eq!(split_ids, &[&split_id_1, &split_id_2, &split_id_5]);

            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_time_range_start_gte(0)
                .with_time_range_end_lt(199);
            let splits = metastore.list_splits(query).await.unwrap();
            let split_ids = collect_split_ids(&splits);
            assert_eq!(split_ids, &[&split_id_1, &split_id_2, &split_id_5]);

            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_time_range_start_gte(0)
                .with_time_range_end_lt(200);
            let splits = metastore.list_splits(query).await.unwrap();
            let split_ids = collect_split_ids(&splits);
            assert_eq!(split_ids, &[&split_id_1, &split_id_2, &split_id_5]);

            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_time_range_start_gte(0)
                .with_time_range_end_lt(201);
            let splits = metastore.list_splits(query).await.unwrap();
            let split_ids = collect_split_ids(&splits);
            assert_eq!(
                split_ids,
                &[&split_id_1, &split_id_2, &split_id_3, &split_id_5]
            );

            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_time_range_start_gte(0)
                .with_time_range_end_lt(299);
            let splits = metastore.list_splits(query).await.unwrap();
            let split_ids = collect_split_ids(&splits);
            assert_eq!(
                split_ids,
                &[&split_id_1, &split_id_2, &split_id_3, &split_id_5]
            );

            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_time_range_start_gte(0)
                .with_time_range_end_lt(300);
            let splits = metastore.list_splits(query).await.unwrap();
            let split_ids = collect_split_ids(&splits);
            assert_eq!(
                split_ids,
                &[&split_id_1, &split_id_2, &split_id_3, &split_id_5]
            );

            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_time_range_start_gte(0)
                .with_time_range_end_lt(301);
            let splits = metastore.list_splits(query).await.unwrap();
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

            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_time_range_start_gte(301)
                .with_time_range_end_lt(400);
            let splits = metastore.list_splits(query).await.unwrap();
            let split_ids = collect_split_ids(&splits);
            assert_eq!(split_ids, &[&split_id_4, &split_id_5]);

            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_time_range_start_gte(300)
                .with_time_range_end_lt(400);
            let splits = metastore.list_splits(query).await.unwrap();
            let split_ids = collect_split_ids(&splits);
            assert_eq!(split_ids, &[&split_id_4, &split_id_5]);

            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_time_range_start_gte(299)
                .with_time_range_end_lt(400);
            let splits = metastore.list_splits(query).await.unwrap();
            let split_ids = collect_split_ids(&splits);
            assert_eq!(split_ids, &[&split_id_3, &split_id_4, &split_id_5]);

            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_time_range_start_gte(201)
                .with_time_range_end_lt(400);
            let splits = metastore.list_splits(query).await.unwrap();
            let split_ids = collect_split_ids(&splits);
            assert_eq!(split_ids, &[&split_id_3, &split_id_4, &split_id_5]);

            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_time_range_start_gte(200)
                .with_time_range_end_lt(400);
            let splits = metastore.list_splits(query).await.unwrap();
            let split_ids = collect_split_ids(&splits);
            assert_eq!(split_ids, &[&split_id_3, &split_id_4, &split_id_5]);

            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_time_range_start_gte(199)
                .with_time_range_end_lt(400);
            let splits = metastore.list_splits(query).await.unwrap();
            let split_ids = collect_split_ids(&splits);
            assert_eq!(
                split_ids,
                &[&split_id_2, &split_id_3, &split_id_4, &split_id_5]
            );

            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_time_range_start_gte(101)
                .with_time_range_end_lt(400);
            let splits = metastore.list_splits(query).await.unwrap();
            let split_ids = collect_split_ids(&splits);
            assert_eq!(
                split_ids,
                &[&split_id_2, &split_id_3, &split_id_4, &split_id_5]
            );

            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_time_range_start_gte(101)
                .with_time_range_end_lt(400);
            let splits = metastore.list_splits(query).await.unwrap();
            let split_ids = collect_split_ids(&splits);
            assert_eq!(
                split_ids,
                &[&split_id_2, &split_id_3, &split_id_4, &split_id_5]
            );

            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_time_range_start_gte(100)
                .with_time_range_end_lt(400);
            let splits = metastore.list_splits(query).await.unwrap();
            let split_ids = collect_split_ids(&splits);

            assert_eq!(
                split_ids,
                &[&split_id_2, &split_id_3, &split_id_4, &split_id_5]
            );

            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_time_range_start_gte(99)
                .with_time_range_end_lt(400);
            let splits = metastore.list_splits(query).await.unwrap();
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

            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_time_range_start_gte(1000)
                .with_time_range_end_lt(1100);
            let splits = metastore.list_splits(query).await.unwrap();
            let split_ids = collect_split_ids(&splits);
            assert_eq!(split_ids, &[&split_id_5]);

            // Artificially increase the create_timestamp
            sleep(Duration::from_secs(1)).await;
            // add a split without tag
            let split_id_6 = format!("{index_id}--split-6");
            let split_metadata_6 = SplitMetadata {
                split_id: split_id_6.clone(),
                index_id: index_id.clone(),
                time_range: None,
                create_timestamp: OffsetDateTime::now_utc().unix_timestamp(),
                ..Default::default()
            };
            metastore
                .stage_splits(&index_id, vec![split_metadata_6.clone()])
                .await
                .unwrap();

            let query = ListSplitsQuery::for_index(&index_id).with_split_state(SplitState::Staged);
            let splits = metastore.list_splits(query).await.unwrap();
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
                TagFilterAst::Or(vec![no_tag("tag!"), tag("tag:bar")]),
                TagFilterAst::Or(vec![no_tag("tag!"), tag("tag:baz")]),
            ]);
            let query = ListSplitsQuery::for_index(&index_id)
                .with_split_state(SplitState::Staged)
                .with_tags_filter(tag_filter_ast);
            let splits = metastore.list_splits(query).await.unwrap();
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

            let query =
                ListSplitsQuery::for_index(&index_id).with_update_timestamp_gte(current_timestamp);
            let splits = metastore.list_splits(query).await.unwrap();
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

            let query = ListSplitsQuery::for_index(&index_id)
                .with_update_timestamp_gte(split_metadata_6.create_timestamp);
            let splits = metastore.list_splits(query).await.unwrap();
            let split_ids: Vec<&String> = splits
                .iter()
                .map(|split| &split.split_metadata.split_id)
                .sorted()
                .collect();
            assert_eq!(split_ids, vec![&split_id_6]);

            let query = ListSplitsQuery::for_index(&index_id)
                .with_create_timestamp_lt(split_metadata_6.create_timestamp);
            let splits = metastore.list_splits(query).await.unwrap();
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

            let query = ListSplitsQuery::for_index(&index_id).with_delete_opstamp_lt(6);
            let splits = metastore.list_splits(query).await.unwrap();
            let split_ids = collect_split_ids(&splits);
            assert_eq!(
                split_ids,
                &[&split_id_1, &split_id_2, &split_id_3, &split_id_6,]
            );

            cleanup_index(&metastore, &index_id).await;
        }
    }

    pub async fn test_metastore_split_update_timestamp<
        MetastoreToTest: Metastore + DefaultForTest,
    >() {
        let metastore = MetastoreToTest::default_for_test().await;

        let mut current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

        let index_id = append_random_suffix("split-update-timestamp");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        let source_id = format!("{index_id}--source");

        let split_id = format!("{index_id}--split");
        let split_metadata = SplitMetadata {
            split_id: split_id.clone(),
            index_id: index_id.clone(),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        // Create an index
        metastore.create_index(index_config.clone()).await.unwrap();

        // wait for 1s, stage split & check `update_timestamp`
        sleep(Duration::from_secs(1)).await;
        metastore
            .stage_splits(&index_id, vec![split_metadata.clone()])
            .await
            .unwrap();

        sleep(Duration::from_secs(1)).await;
        let split_meta = metastore.list_all_splits(&index_id).await.unwrap()[0].clone();
        assert!(split_meta.update_timestamp > current_timestamp);
        assert!(split_meta.publish_timestamp.is_none());

        current_timestamp = split_meta.update_timestamp;

        // wait for 1s, publish split & check `update_timestamp`
        sleep(Duration::from_secs(1)).await;
        metastore
            .publish_splits(
                &index_id,
                &[&split_id],
                &[],
                {
                    let offsets = 0..5;
                    IndexCheckpointDelta::for_test(&source_id, offsets)
                }
                .into(),
            )
            .await
            .unwrap();
        let split_meta = metastore.list_all_splits(&index_id).await.unwrap()[0].clone();
        assert!(split_meta.update_timestamp > current_timestamp);
        assert_eq!(
            split_meta.publish_timestamp,
            Some(split_meta.update_timestamp)
        );
        current_timestamp = split_meta.update_timestamp;

        // wait for 1s, mark split for deletion & check `update_timestamp`
        sleep(Duration::from_secs(1)).await;
        metastore
            .mark_splits_for_deletion(&index_id, &[&split_id])
            .await
            .unwrap();
        let split_meta = metastore.list_all_splits(&index_id).await.unwrap()[0].clone();
        assert!(split_meta.update_timestamp > current_timestamp);
        assert!(split_meta.publish_timestamp.is_some());

        cleanup_index(&metastore, &index_id).await;
    }

    pub async fn test_metastore_create_delete_task<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;
        let index_id = append_random_suffix("add-delete-task");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);
        let delete_query = DeleteQuery {
            index_id: index_id.clone(),
            query: "my_field:my_value".to_string(),
            start_timestamp: Some(1),
            end_timestamp: Some(2),
            search_fields: Vec::new(),
        };
        metastore.create_index(index_config.clone()).await.unwrap();

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

        cleanup_index(&metastore, &index_id).await;
    }

    pub async fn test_metastore_last_delete_opstamp<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;
        let index_id_1 = append_random_suffix("test-last-delete-opstamp-1");
        let index_uri_1 = format!("ram:///indexes/{index_id_1}");
        let index_config_1 = IndexConfig::for_test(&index_id_1, &index_uri_1);
        let delete_query_index_1 = DeleteQuery {
            index_id: index_id_1.to_string(),
            query: "my_field:my_value".to_string(),
            start_timestamp: Some(1),
            end_timestamp: Some(2),
            search_fields: Vec::new(),
        };
        let index_id_2 = append_random_suffix("test-last-delete-opstamp-2");
        let index_uri_2 = format!("ram:///indexes/{index_id_2}");
        let index_config_2 = IndexConfig::for_test(&index_id_2, &index_uri_2);
        let delete_query_index_2 = DeleteQuery {
            index_id: index_id_2.to_string(),
            query: "my_field:my_value".to_string(),
            start_timestamp: Some(1),
            end_timestamp: Some(2),
            search_fields: Vec::new(),
        };
        metastore
            .create_index(index_config_1.clone())
            .await
            .unwrap();
        metastore
            .create_index(index_config_2.clone())
            .await
            .unwrap();

        let last_opstamp_index_1_with_no_task =
            metastore.last_delete_opstamp(&index_id_1).await.unwrap();
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

        let last_opstamp_index_1 = metastore.last_delete_opstamp(&index_id_1).await.unwrap();
        let last_opstamp_index_2 = metastore.last_delete_opstamp(&index_id_2).await.unwrap();
        assert_eq!(last_opstamp_index_1, delete_task_2.opstamp);
        assert_eq!(last_opstamp_index_2, delete_task_3.opstamp);
        cleanup_index(&metastore, &index_id_1).await;
        cleanup_index(&metastore, &index_id_2).await;
    }

    pub async fn test_metastore_delete_index_with_tasks<
        MetastoreToTest: Metastore + DefaultForTest,
    >() {
        let metastore = MetastoreToTest::default_for_test().await;
        let index_id = append_random_suffix("delete-delete-tasks");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);
        let delete_query = DeleteQuery {
            index_id: index_id.clone(),
            query: "my_field:my_value".to_string(),
            start_timestamp: Some(1),
            end_timestamp: Some(2),
            search_fields: Vec::new(),
        };
        metastore.create_index(index_config.clone()).await.unwrap();
        let _ = metastore
            .create_delete_task(delete_query.clone())
            .await
            .unwrap();
        let _ = metastore
            .create_delete_task(delete_query.clone())
            .await
            .unwrap();

        metastore.delete_index(&index_id).await.unwrap();
    }

    pub async fn test_metastore_list_delete_tasks<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;
        let index_id_1 = append_random_suffix("test-list-delete-tasks-1");
        let index_uri_1 = format!("ram:///indexes/{index_id_1}");
        let index_config_1 = IndexConfig::for_test(&index_id_1, &index_uri_1);
        let delete_query_index_1 = DeleteQuery {
            index_id: index_id_1.to_string(),
            query: "my_field:my_value".to_string(),
            start_timestamp: Some(1),
            end_timestamp: Some(2),
            search_fields: Vec::new(),
        };
        let index_id_2 = append_random_suffix("test-list-delete-tasks-2");
        let index_uri_2 = format!("ram:///indexes/{index_id_2}");
        let index_config_2 = IndexConfig::for_test(&index_id_2, &index_uri_2);
        let delete_query_index_2 = DeleteQuery {
            index_id: index_id_2.to_string(),
            query: "my_field:my_value".to_string(),
            start_timestamp: Some(1),
            end_timestamp: Some(2),
            search_fields: Vec::new(),
        };
        metastore
            .create_index(index_config_1.clone())
            .await
            .unwrap();
        metastore
            .create_index(index_config_2.clone())
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

        let all_index_id_1_delete_tasks =
            metastore.list_delete_tasks(&index_id_1, 0).await.unwrap();
        assert_eq!(all_index_id_1_delete_tasks.len(), 2);

        let recent_index_id_1_delete_tasks = metastore
            .list_delete_tasks(&index_id_1, delete_task_1.opstamp)
            .await
            .unwrap();
        assert_eq!(recent_index_id_1_delete_tasks.len(), 1);
        assert_eq!(
            recent_index_id_1_delete_tasks[0].opstamp,
            delete_task_2.opstamp
        );
        cleanup_index(&metastore, &index_id_1).await;
        cleanup_index(&metastore, &index_id_2).await;
    }

    pub async fn test_metastore_list_stale_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;
        let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();
        let index_id = append_random_suffix("test-list-stale-splits");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        let split_id_1 = format!("{index_id}--split-1");
        let split_metadata_1 = SplitMetadata {
            split_id: split_id_1.clone(),
            index_id: index_id.clone(),
            create_timestamp: current_timestamp,
            delete_opstamp: 20,
            ..Default::default()
        };
        let split_id_2 = format!("{index_id}--split-2");
        let split_metadata_2 = SplitMetadata {
            split_id: split_id_2.clone(),
            index_id: index_id.clone(),
            create_timestamp: current_timestamp,
            delete_opstamp: 10,
            ..Default::default()
        };
        let split_id_3 = format!("{index_id}--split-3");
        let split_metadata_3 = SplitMetadata {
            split_id: split_id_3.clone(),
            index_id: index_id.clone(),
            create_timestamp: current_timestamp,
            delete_opstamp: 0,
            ..Default::default()
        };
        let split_id_4 = format!("{index_id}--split-4");
        let split_metadata_4 = SplitMetadata {
            split_id: split_id_4.clone(),
            index_id: index_id.clone(),
            create_timestamp: current_timestamp,
            delete_opstamp: 20,
            ..Default::default()
        };

        let error = metastore
            .list_stale_splits("index-not-found", 0, 10)
            .await
            .unwrap_err();
        assert!(matches!(error, MetastoreError::IndexDoesNotExist { .. }));

        {
            info!("List stale splits on an index");
            metastore.create_index(index_config.clone()).await.unwrap();

            metastore
                .stage_splits(
                    &index_id,
                    vec![
                        split_metadata_1.clone(),
                        split_metadata_2.clone(),
                        split_metadata_3.clone(),
                    ],
                )
                .await
                .unwrap();

            // Sleep for 1 second to have different publish timestamps.
            sleep(Duration::from_secs(1)).await;

            metastore
                .stage_splits(&index_id, vec![split_metadata_4.clone()])
                .await
                .unwrap();
            metastore
                .publish_splits(&index_id, &[&split_id_4], &[], None)
                .await
                .unwrap();
            // Sleep for 1 second to have different publish timestamps.
            tokio::time::sleep(Duration::from_secs(1)).await;
            metastore
                .publish_splits(&index_id, &[&split_id_1, &split_id_2], &[], None)
                .await
                .unwrap();
            let splits = metastore
                .list_stale_splits(&index_id, 100, 1)
                .await
                .unwrap();
            assert_eq!(splits.len(), 1);
            assert_eq!(
                splits[0].split_metadata.delete_opstamp,
                split_metadata_2.delete_opstamp
            );

            let splits = metastore
                .list_stale_splits(&index_id, 100, 4)
                .await
                .unwrap();
            assert_eq!(splits.len(), 3);
            assert_eq!(splits[0].split_id(), split_metadata_2.split_id());
            assert_eq!(splits[1].split_id(), split_metadata_4.split_id());
            assert_eq!(splits[2].split_id(), split_metadata_1.split_id());
            assert_eq!(
                splits[2].split_metadata.delete_opstamp,
                split_metadata_1.delete_opstamp
            );

            let splits = metastore.list_stale_splits(&index_id, 20, 2).await.unwrap();
            assert_eq!(splits.len(), 1);
            assert_eq!(
                splits[0].split_metadata.delete_opstamp,
                split_metadata_2.delete_opstamp
            );

            let splits = metastore.list_stale_splits(&index_id, 10, 2).await.unwrap();
            assert!(splits.is_empty());
            cleanup_index(&metastore, &index_id).await;
        }
    }

    pub async fn test_metastore_update_splits_delete_opstamp<
        MetastoreToTest: Metastore + DefaultForTest,
    >() {
        let metastore = MetastoreToTest::default_for_test().await;
        let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();
        let index_id = append_random_suffix("update-splits-delete-opstamp");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        let split_id_1 = format!("{index_id}--split-1");
        let split_metadata_1 = SplitMetadata {
            split_id: split_id_1.clone(),
            index_id: index_id.clone(),
            create_timestamp: current_timestamp,
            delete_opstamp: 20,
            ..Default::default()
        };
        let split_id_2 = format!("{index_id}--split-2");
        let split_metadata_2 = SplitMetadata {
            split_id: split_id_2.clone(),
            index_id: index_id.clone(),
            create_timestamp: current_timestamp,
            delete_opstamp: 10,
            ..Default::default()
        };
        let split_id_3 = format!("{index_id}--split-3");
        let split_metadata_3 = SplitMetadata {
            split_id: split_id_3.clone(),
            index_id: index_id.clone(),
            create_timestamp: current_timestamp,
            delete_opstamp: 0,
            ..Default::default()
        };

        {
            info!("Update splits delete opstamp on a non-existent index.");
            let metastore_err = metastore
                .update_splits_delete_opstamp("index-not-found", &[&split_id_1], 10)
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
            metastore.create_index(index_config.clone()).await.unwrap();

            metastore
                .stage_splits(
                    &index_id,
                    vec![
                        split_metadata_1.clone(),
                        split_metadata_2.clone(),
                        split_metadata_3.clone(),
                    ],
                )
                .await
                .unwrap();
            metastore
                .publish_splits(&index_id, &[&split_id_1, &split_id_2], &[], None)
                .await
                .unwrap();

            let splits = metastore
                .list_stale_splits(&index_id, 100, 2)
                .await
                .unwrap();
            assert_eq!(splits.len(), 2);

            metastore
                .update_splits_delete_opstamp(&index_id, &[&split_id_1, &split_id_2], 100)
                .await
                .unwrap();

            let splits = metastore
                .list_stale_splits(&index_id, 100, 2)
                .await
                .unwrap();
            assert_eq!(splits.len(), 0);

            let splits = metastore
                .list_stale_splits(&index_id, 200, 2)
                .await
                .unwrap();
            assert_eq!(splits.len(), 2);
            assert_eq!(splits[0].split_metadata.delete_opstamp, 100);
            assert_eq!(splits[1].split_metadata.delete_opstamp, 100);

            cleanup_index(&metastore, &index_id).await;
        }
    }

    pub async fn test_metastore_stage_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;
        let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();
        let index_id = append_random_suffix("test-stage-splits");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        let split_id_1 = format!("{index_id}--split-1");
        let split_metadata_1 = SplitMetadata {
            split_id: split_id_1.clone(),
            index_id: index_id.clone(),
            create_timestamp: current_timestamp,
            delete_opstamp: 20,
            ..Default::default()
        };
        let split_id_2 = format!("{index_id}--split-2");
        let split_metadata_2 = SplitMetadata {
            split_id: split_id_2.clone(),
            index_id: index_id.clone(),
            create_timestamp: current_timestamp,
            delete_opstamp: 10,
            ..Default::default()
        };

        // Stage a splits on a non-existent index
        let error = metastore
            .stage_splits("index-not-found", vec![split_metadata_1.clone()])
            .await
            .unwrap_err();
        assert!(matches!(error, MetastoreError::IndexDoesNotExist { .. }));

        metastore.create_index(index_config.clone()).await.unwrap();

        // Stage a split on an index
        metastore
            .stage_splits(
                &index_id,
                vec![split_metadata_1.clone(), split_metadata_2.clone()],
            )
            .await
            .unwrap();

        let query = ListSplitsQuery::for_index(&index_id).with_split_state(SplitState::Staged);
        let splits = metastore.list_splits(query).await.unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(split_ids, &[&split_id_1, &split_id_2]);

        // Stage a existent-staged-split on an index
        metastore
            .stage_splits(&index_id, vec![split_metadata_1.clone()])
            .await
            .expect("Pre-existing staged splits should be updated.");

        metastore
            .publish_splits(&index_id, &[&split_id_1, &split_id_2], &[], None)
            .await
            .unwrap();
        let err = metastore
            .stage_splits(&index_id, vec![split_metadata_1.clone()])
            .await
            .expect_err(
                "Metastore should not allow splits which are not `Staged` to be overwritten.",
            );
        assert!(
            matches!(err, MetastoreError::SplitsNotStaged { .. }),
            "Metastore should return a `SplitsNotStaged` error when attempting to perform an \
             operation that must not occur.",
        );

        metastore
            .mark_splits_for_deletion(&index_id, &[&split_id_2])
            .await
            .unwrap();
        let err = metastore
            .stage_splits(&index_id, vec![split_metadata_2.clone()])
            .await
            .expect_err(
                "Metastore should not allow splits which are not `Staged` to be overwritten.",
            );
        assert!(
            matches!(err, MetastoreError::SplitsNotStaged { .. }),
            "Metastore should return a `SplitsNotStaged` error when attempting to perform an \
             operation that must not occur.",
        );

        cleanup_index(&metastore, &index_id).await;
    }
}

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
                crate::tests::test_suite::test_metastore_create_index::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_index_exists() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_index_exists::<$metastore_type>().await;
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
            async fn test_metastore_delete_index() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_delete_index::<$metastore_type>().await;
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
                crate::tests::test_suite::test_metastore_publish_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_publish_splits_concurrency() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_publish_splits_concurrency::<$metastore_type>().await;
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

            #[tokio::test]
            async fn test_metastore_stage_splits() {
                let _ = tracing_subscriber::fmt::try_init();
                crate::tests::test_suite::test_metastore_stage_splits::<$metastore_type>().await;
            }
        }
    }
}
