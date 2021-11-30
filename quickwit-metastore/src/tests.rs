// Copyright (C) 2021 Quickwit, Inc.
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
    use std::collections::{HashMap, HashSet};
    use std::collections::{BTreeSet, HashSet};
    use std::ops::{Range, RangeInclusive};
    use std::sync::Arc;

    use async_trait::async_trait;
    use chrono::Utc;
    use tokio::time::{sleep, Duration};

    use crate::checkpoint::{Checkpoint, CheckpointDelta};
    use crate::{
        IndexMetadata, Metastore, MetastoreError, SplitMetadata,
        SplitMetadataAndFooterOffsets, SplitState,
    };

    #[async_trait]
    pub trait DefaultForTest {
        async fn default_for_test() -> Self;
    }

    fn to_set(tags: &[&str]) -> BTreeSet<String> {
        tags.iter().map(ToString::to_string).collect()
    }

    fn make_index_meta(index_id: &str) -> IndexMetadata {
        IndexMetadata {
            index_id: index_id.to_string(),
            index_uri: "ram://indexes/my-index".to_string(),
            index_config: Arc::new(quickwit_index_config::default_config_for_tests()),
            checkpoint: Checkpoint::default(),
        }
    }

    fn make_split_meta(
        split_id: String,
        split_state: SplitState,
        time_range: Option<RangeInclusive<i64>>,
        timestamp: i64,
        tags: HashSet<String>,
    ) -> SplitMetadataAndFooterOffsets {
        SplitMetadataAndFooterOffsets {
            footer_offsets: 1000..2000,
            split_metadata: SplitMetadata {
                split_id,
                split_state,
                num_records: 1,
                size_in_bytes: 2,
                time_range,
                update_timestamp: timestamp,
                tags,
                ..Default::default()
            },
        }
    }

    fn make_split_metas(
        splits_with_states: Vec<(SplitMetadataAndFooterOffsets, SplitState)>,
    ) -> Vec<SplitMetadataAndFooterOffsets> {
        splits_with_states
            .into_iter()
            .map(|(split, split_state)| SplitMetadataAndFooterOffsets {
                split_metadata: SplitMetadata {
                    split_state,
                    ..split.split_metadata
                },
                ..split
            })
            .collect()
    }

    async fn cleanup_index(metastore: &dyn Metastore, index_id: &str) {
        // List all splits.
        let all_splits = metastore.list_all_splits(index_id).await.unwrap();

        if !all_splits.is_empty() {
            let all_split_ids = all_splits
                .iter()
                .map(|meta| meta.split_metadata.split_id.as_ref())
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

    async fn index_exists(metastore: &dyn Metastore, index_id: &str) -> bool {
        match metastore.index_metadata(index_id).await {
            Ok(_) => true,
            Err(MetastoreError::IndexDoesNotExist { index_id }) => false,
            Err(error) => Err(error).unwrap(),
        }
    }

    async fn create_index(metastore: &dyn Metastore, index_metadata: IndexMetadata) {
        metastore.create_index(index_metadata).await.unwrap()
    }

    async fn insert_split(
        metastore: &dyn Metastore,
        index_id: &str,
        split_metadata: SplitMetadataAndFooterOffsets,
        split_state: SplitState,
    ) {
        let split_id = split_metadata.split_metadata.split_id.clone();

        metastore
            .stage_split(index_id, split_metadata)
            .await
            .unwrap();
        if split_state == SplitState::Staged {
            return;
        }
        metastore
            .publish_splits(index_id, &[&split_id], CheckpointDelta::default())
            .await
            .unwrap();
        if split_state == SplitState::Published {
            return;
        }
        metastore
            .mark_splits_for_deletion(index_id, &[&split_id])
            .await
            .unwrap();
    }

    async fn insert_staged_split(
        metastore: &dyn Metastore,
        index_id: &str,
        split_metadata: SplitMetadataAndFooterOffsets,
    ) {
        insert_split(metastore, index_id, split_metadata, SplitState::Staged).await;
    }

    async fn insert_published_split(
        metastore: &dyn Metastore,
        index_id: &str,
        split_metadata: SplitMetadataAndFooterOffsets,
    ) {
        insert_split(metastore, index_id, split_metadata, SplitState::Published).await;
    }

    async fn insert_deleted_split(
        metastore: &dyn Metastore,
        index_id: &str,
        split_metadata: SplitMetadataAndFooterOffsets,
    ) {
        insert_split(
            metastore,
            index_id,
            split_metadata,
            SplitState::ScheduledForDeletion,
        )
        .await;
    }

    pub async fn test_metastore_delete_index<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = "delete-index-index";
        let index_metadata = make_index_meta(index_id);

        // Delete non-existent index.
        let metastore_error = metastore
            .delete_index("non-existent-index")
            .await
            .unwrap_err();
        assert!(matches!(
            metastore_error,
            MetastoreError::IndexDoesNotExist { .. }
        ));

        create_index(&metastore, index_metadata).await;
        assert!(index_exists(&metastore, index_id).await);

        // Delete index.
        metastore.delete_index(index_id).await.unwrap();
        assert!(!index_exists(&metastore, index_id).await);

        // Ensure splits are effectively gone.
        let metastore_error = metastore.list_all_splits(index_id).await.unwrap_err();
        assert!(matches!(
            metastore_error,
            MetastoreError::IndexDoesNotExist { .. }
        ));
    }

    pub async fn test_metastore_index_metadata<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = "index-metadata-index";
        let index_metadata = make_index_meta(index_id);

        // Get non-existent index metadata.
        let metastore_error = metastore
            .index_metadata("non-existent-index")
            .await
            .unwrap_err();
        assert!(matches!(
            metastore_error,
            MetastoreError::IndexDoesNotExist { .. }
        ));

        create_index(&metastore, index_metadata.clone()).await;

        // Get index metadata.
        let index_metadata = metastore.index_metadata(index_id).await.unwrap();
        assert_eq!(index_metadata.index_id, index_metadata.index_id);
        assert_eq!(index_metadata.index_uri, index_metadata.index_uri);
        assert_eq!(index_metadata.checkpoint, index_metadata.checkpoint);
        assert!(matches!(index_metadata.index_config, Arc { .. }));

        cleanup_index(&metastore, index_id).await;
    }

    pub async fn test_metastore_publish_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let now_timestamp = Utc::now().timestamp();

        let index_id = "publish-splits-index";
        let index_metadata = make_index_meta(index_id);

        let split_id_1 = "publish-splits-index-one";
        let split_metadata_1 = make_split_meta(
            split_id_1.to_string(),
            SplitState::Staged,
            Some(RangeInclusive::new(0, 99)),
            now_timestamp,
            to_set(&[]),
        );

        let split_id_2 = "publish-splits-index-two";
        let split_metadata_2 = make_split_meta(
            split_id_2.to_string(),
            SplitState::Staged,
            Some(RangeInclusive::new(30, 99)),
            now_timestamp,
            to_set(&[]),
        );

        // Publish a split on a non-existent index.
        {
            let metastore_error = metastore
                .publish_splits(
                    "non-existent-index",
                    &["non-existent-split"],
                    CheckpointDelta::from(1..10),
                )
                .await
                .unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::IndexDoesNotExist { .. }
            ));
        }

        // Publish a non-existent split on an index.
        {
            create_index(&metastore, index_metadata.clone()).await;

            let metastore_error = metastore
                .publish_splits(
                    index_id,
                    &["non-existent-split"],
                    CheckpointDelta::default(),
                )
                .await
                .unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::SplitsDoNotExist { .. }
            ));

            cleanup_index(&metastore, index_id).await;
        }

        // Publish a staged split on an index.
        {
            create_index(&metastore, index_metadata.clone()).await;
            insert_staged_split(&metastore, index_id, split_metadata_1.clone()).await;

            metastore
                .publish_splits(index_id, &[split_id_1], CheckpointDelta::default())
                .await
                .unwrap();

            assert_eq!(
                metastore.list_all_splits(index_id).await.unwrap(),
                make_split_metas(vec![(split_metadata_1.clone(), SplitState::Published)])
            );

            cleanup_index(&metastore, index_id).await;
        }

        // Publish a non-staged split on an index.
        {
            create_index(&metastore, index_metadata.clone()).await;
            insert_deleted_split(&metastore, index_id, split_metadata_1.clone()).await;

            let metastore_error = metastore
                .publish_splits(index_id, &[split_id_1], CheckpointDelta::from(15..18))
                .await
                .unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::SplitsNotStaged { .. }
            ));
            assert_eq!(
                metastore.list_all_splits(index_id).await.unwrap(),
                make_split_metas(vec![(
                    split_metadata_1.clone(),
                    SplitState::ScheduledForDeletion
                )])
            );

            cleanup_index(&metastore, index_id).await;
        }

        // Publish a staged split and non-existent split on an index.
        {
            create_index(&metastore, index_metadata.clone()).await;
            insert_staged_split(&metastore, index_id, split_metadata_1.clone()).await;

            let metastore_error = metastore
                .publish_splits(
                    index_id,
                    &[split_id_1, "non-existent-split"],
                    CheckpointDelta::from(15..18),
                )
                .await
                .unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::SplitsDoNotExist { .. }
            ));
            assert_eq!(
                metastore.list_all_splits(index_id).await.unwrap(),
                make_split_metas(vec![(split_metadata_1.clone(), SplitState::Staged)])
            );

            cleanup_index(&metastore, index_id).await;
        }

        // Publish a non-staged split and non-existant split on an index.
        {
            create_index(&metastore, index_metadata.clone()).await;
            insert_deleted_split(&metastore, index_id, split_metadata_1.clone()).await;

            let metastore_error = metastore
                .publish_splits(
                    index_id,
                    &[split_id_1, "non-existent-split"],
                    CheckpointDelta::from(24..26),
                )
                .await
                .unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::SplitsDoNotExist { .. }
            ));
            assert_eq!(
                metastore.list_all_splits(index_id).await.unwrap(),
                make_split_metas(vec![(
                    split_metadata_1.clone(),
                    SplitState::ScheduledForDeletion
                )])
            );

            cleanup_index(&metastore, index_id).await;
        }

        // Publish a staged split and published split on an index
        {
            create_index(&metastore, index_metadata.clone()).await;
            insert_staged_split(&metastore, index_id, split_metadata_1.clone()).await;
            insert_published_split(&metastore, index_id, split_metadata_2.clone()).await;

            metastore
                .publish_splits(
                    index_id,
                    &[split_id_1, split_id_2],
                    CheckpointDelta::from(28..30),
                )
                .await
                .unwrap();
            let mut splits = metastore.list_all_splits(index_id).await.unwrap();
            splits.sort_by(|meta1, meta2| {
                meta1
                    .split_metadata
                    .split_id
                    .cmp(&meta2.split_metadata.split_id)
            });
            assert_eq!(
                splits,
                make_split_metas(vec![
                    (split_metadata_1.clone(), SplitState::Published),
                    (split_metadata_2.clone(), SplitState::Published)
                ])
            );

            cleanup_index(&metastore, index_id).await;
        }

        // Publish splits with incompatible checkpoint deltas.
        {
            create_index(&metastore, index_metadata.clone()).await;
            insert_staged_split(&metastore, index_id, split_metadata_1.clone()).await;
            insert_staged_split(&metastore, index_id, split_metadata_2.clone()).await;

            metastore
                .publish_splits(
                    index_id,
                    &[split_id_1, split_id_2],
                    CheckpointDelta::from(30..31),
                )
                .await
                .unwrap();

            let metastore_error = metastore
                .publish_splits(
                    index_id,
                    &[split_id_1, split_id_2],
                    CheckpointDelta::from(30..31),
                )
                .await
                .unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::IncompatibleCheckpointDelta(_)
            ));

            cleanup_index(&metastore, index_id).await;
        }
    }

    pub async fn test_metastore_replace_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let current_timestamp = Utc::now().timestamp();

        let index_id = "replace_splits-index";
        let index_metadata = make_index_meta(index_id);

        let split_id_1 = "replace_splits-index-one";
        let split_metadata_1 = make_split_meta(
            split_id_1.to_string(),
            SplitState::Staged,
            None,
            current_timestamp,
            to_set(&[]),
        );

        let split_id_2 = "replace_splits-index-two";
        let split_metadata_2 = make_split_meta(
            split_id_2.to_string(),
            SplitState::Staged,
            None,
            current_timestamp,
            to_set(&[]),
        );

        let split_id_3 = "replace_splits-index-three";
        let split_metadata_3 = make_split_meta(
            split_id_3.to_string(),
            SplitState::Staged,
            None,
            current_timestamp,
            to_set(&[]),
        );

        // Replace splits on a non-existent index
        {
            let result = metastore
                .replace_splits(
                    "non-existent-index",
                    &["non-existent-split-one"],
                    &["non-existent-split-two"],
                )
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));
        }

        // Replace a non-existent split on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            let result = metastore
                .replace_splits(
                    index_id,
                    &["non-existent-split"],
                    &["non-existent-split-two"],
                )
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, index_id).await;
        }

        // Replace a publish split with mixed splits
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
                .publish_splits(index_id, &[split_id_1], CheckpointDelta::default())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_2.clone())
                .await
                .unwrap();

            let result = metastore
                .replace_splits(index_id, &[split_id_2, split_id_3], &[split_id_1])
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsDoNotExist { .. }));
            let mut splits = metastore.list_all_splits(index_id).await.unwrap();
            splits.sort_by(|meta1, meta2| {
                meta1
                    .split_metadata
                    .split_id
                    .cmp(&meta2.split_metadata.split_id)
            });
            assert_eq!(
                splits,
                make_split_metas(vec![
                    (split_metadata_1.clone(), SplitState::Published),
                    (split_metadata_2.clone(), SplitState::Staged)
                ])
            );

            cleanup_index(&metastore, index_id).await;
        }

        // Replace a publish split with staged splits
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
                .publish_splits(index_id, &[split_id_1], CheckpointDelta::default())
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
            assert_eq!(metastore.list_all_splits(index_id).await.unwrap().len(), 3);

            let result = metastore
                .replace_splits(index_id, &[split_id_2, split_id_3], &[split_id_1])
                .await
                .unwrap();
            assert!(matches!(result, ()));
            let mut splits = metastore.list_all_splits(index_id).await.unwrap();
            splits.sort_by(|meta1, meta2| {
                meta1
                    .split_metadata
                    .split_id
                    .cmp(&meta2.split_metadata.split_id)
            });
            assert_eq!(
                splits,
                make_split_metas(vec![
                    (split_metadata_1.clone(), SplitState::ScheduledForDeletion),
                    (split_metadata_3.clone(), SplitState::Published),
                    (split_metadata_2.clone(), SplitState::Published)
                ])
            );

            cleanup_index(&metastore, index_id).await;
        }
    }

    pub async fn test_metastore_delete_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let current_timestamp = Utc::now().timestamp();

        let index_id = "delete-splits-index";
        let index_metadata = make_index_meta(index_id);

        let split_id_1 = "delete-splits-index-one";
        let split_metadata_1 = make_split_meta(
            split_id_1.to_string(),
            SplitState::Staged,
            Some(RangeInclusive::new(0, 99)),
            current_timestamp,
            to_set(&[]),
        );

        // Delete a split on a non-existent index
        {
            let result = metastore
                .delete_splits("non-existent-index", &["non-existent-split"])
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));
        }

        // Delete a non-existent split on an index
        {
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

        // Delete a staged split on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();
            assert_eq!(
                metastore.list_all_splits(index_id).await.unwrap(),
                vec![split_metadata_1.clone()]
            );

            let result = metastore
                .delete_splits(index_id, &[split_id_1])
                .await
                .unwrap();
            assert!(matches!(result, ()));
            assert_eq!(metastore.list_all_splits(index_id).await.unwrap(), vec![]);

            cleanup_index(&metastore, index_id).await;
        }

        // Delete a split that has been marked for deletion on an index
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

            let result = metastore
                .delete_splits(index_id, &[split_id_1])
                .await
                .unwrap();
            assert!(matches!(result, ()));
            assert_eq!(metastore.list_all_splits(index_id).await.unwrap(), vec![]);

            cleanup_index(&metastore, index_id).await;
        }

        // Delete a split that is not marked for deletion
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
                .publish_splits(index_id, &[split_id_1], CheckpointDelta::from(0..5))
                .await
                .unwrap();

            let result = metastore
                .delete_splits(index_id, &[split_id_1])
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsNotDeletable { .. }));
            assert_eq!(
                metastore.list_all_splits(index_id).await.unwrap(),
                make_split_metas(vec![(split_metadata_1.clone(), SplitState::Published)])
            );

            cleanup_index(&metastore, index_id).await;
        }
    }

    pub async fn test_metastore_list_all_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let current_timestamp = Utc::now().timestamp();

        let index_id = "list-all-splits-index";
        let index_metadata = make_index_meta(index_id);

        let split_metadata_1 = make_split_meta(
            "list-all-splits-index-one".to_string(),
            SplitState::Staged,
            Some(RangeInclusive::new(0, 99)),
            current_timestamp,
            to_set(&[]),
        );

        let split_metadata_2 = make_split_meta(
            "list-all-splits-index-two".to_string(),
            SplitState::Staged,
            Some(RangeInclusive::new(100, 199)),
            current_timestamp,
            to_set(&[]),
        );

        let split_metadata_3 = make_split_meta(
            "list-all-splits-index-three".to_string(),
            SplitState::Staged,
            Some(RangeInclusive::new(200, 299)),
            current_timestamp,
            to_set(&[]),
        );

        let split_metadata_4 = make_split_meta(
            "list-all-splits-index-four".to_string(),
            SplitState::Staged,
            Some(RangeInclusive::new(300, 399)),
            current_timestamp,
            to_set(&[]),
        );

        let split_metadata_5 = make_split_meta(
            "list-all-splits-index-five".to_string(),
            SplitState::Staged,
            None,
            current_timestamp,
            to_set(&[]),
        );

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

            let splits: HashMap<String, SplitMetadataAndFooterOffsets> = metastore
                .list_all_splits(index_id)
                .await
                .unwrap()
                .into_iter()
                .map(|metadata| (metadata.split_metadata.split_id.clone(), metadata))
                .collect();

            assert_eq!(
                splits.get("list-all-splits-index-one"),
                Some(&split_metadata_1)
            );
            assert_eq!(
                splits.get("list-all-splits-index-two"),
                Some(&split_metadata_2)
            );
            assert_eq!(
                splits.get("list-all-splits-index-three"),
                Some(&split_metadata_3)
            );
            assert_eq!(
                splits.get("list-all-splits-index-four"),
                Some(&split_metadata_4)
            );
            assert_eq!(
                splits.get("list-all-splits-index-five"),
                Some(&split_metadata_5)
            );

            cleanup_index(&metastore, index_id).await;
        }
    }

    pub async fn test_metastore_list_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let current_timestamp = Utc::now().timestamp();

        let index_id = "list-splits-index";
        let index_metadata = make_index_meta(index_id);

        let split_metadata_1 = make_split_meta(
            "list-splits-one".to_string(),
            SplitState::Staged,
            Some(RangeInclusive::new(0, 99)),
            current_timestamp,
            to_set(&["foo", "bar"]),
        );

        let split_metadata_2 = make_split_meta(
            "list-splits-two".to_string(),
            SplitState::Staged,
            Some(RangeInclusive::new(100, 199)),
            current_timestamp,
            to_set(&["bar"]),
        );

        let split_metadata_3 = make_split_meta(
            "list-splits-three".to_string(),
            SplitState::Staged,
            Some(RangeInclusive::new(200, 299)),
            current_timestamp,
            to_set(&["foo", "baz"]),
        );

        let split_metadata_4 = make_split_meta(
            "list-splits-four".to_string(),
            SplitState::Staged,
            Some(RangeInclusive::new(300, 399)),
            current_timestamp,
            to_set(&["foo"]),
        );

        let split_metadata_5 = make_split_meta(
            "list-splits-five".to_string(),
            SplitState::Staged,
            None,
            current_timestamp,
            to_set(&["baz", "biz"]),
        );

        // List all splits on a non-existent index
        {
            let result = metastore
                .list_splits("non-existent-index", SplitState::Staged, None, &[])
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

            let time_range_opt = Some(Range {
                start: 0i64,
                end: 99i64,
            });
            let splits: HashMap<String, SplitMetadataAndFooterOffsets> = metastore
                .list_splits(index_id, SplitState::Staged, time_range_opt, &[])
                .await
                .unwrap()
                .into_iter()
                .map(|metadata| (metadata.split_metadata.split_id.clone(), metadata))
                .collect();
            assert_eq!(splits.get("list-splits-one"), Some(&split_metadata_1));
            assert_eq!(splits.get("list-splits-two"), None);
            assert_eq!(splits.get("list-splits-three"), None);
            assert_eq!(splits.get("list-splits-four"), None);
            assert_eq!(splits.get("list-splits-five"), Some(&split_metadata_5));

            let time_range_opt = Some(Range {
                start: 200,
                end: i64::MAX,
            });
            let splits: HashMap<String, SplitMetadataAndFooterOffsets> = metastore
                .list_splits(index_id, SplitState::Staged, time_range_opt, &[])
                .await
                .unwrap()
                .into_iter()
                .map(|metadata| (metadata.split_metadata.split_id.clone(), metadata))
                .collect();
            assert_eq!(splits.get("list-splits-one"), None);
            assert_eq!(splits.get("list-splits-two"), None);
            assert_eq!(splits.get("list-splits-three"), Some(&split_metadata_3));
            assert_eq!(splits.get("list-splits-four"), Some(&split_metadata_4));
            assert_eq!(splits.get("list-splits-five"), Some(&split_metadata_5));

            let time_range_opt = Some(Range {
                start: i64::MIN,
                end: 200,
            });
            let splits: HashMap<String, SplitMetadataAndFooterOffsets> = metastore
                .list_splits(index_id, SplitState::Staged, time_range_opt, &[])
                .await
                .unwrap()
                .into_iter()
                .map(|metadata| (metadata.split_metadata.split_id.clone(), metadata))
                .collect();
            assert_eq!(splits.get("list-splits-one"), Some(&split_metadata_1));
            assert_eq!(splits.get("list-splits-two"), Some(&split_metadata_2));
            assert_eq!(splits.get("list-splits-three"), None);
            assert_eq!(splits.get("list-splits-four"), None);
            assert_eq!(splits.get("list-splits-five"), Some(&split_metadata_5));

            let range = Some(Range { start: 0, end: 199 });
            let splits: HashMap<String, SplitMetadataAndFooterOffsets> = metastore
                .list_splits(index_id, SplitState::Staged, range, &[])
                .await
                .unwrap()
                .into_iter()
                .map(|metadata| (metadata.split_metadata.split_id.clone(), metadata))
                .collect();
            assert_eq!(splits.get("list-splits-one"), Some(&split_metadata_1));
            assert_eq!(splits.get("list-splits-two"), Some(&split_metadata_2));
            assert_eq!(splits.get("list-splits-three"), None);
            assert_eq!(splits.get("list-splits-four"), None);
            assert_eq!(splits.get("list-splits-five"), Some(&split_metadata_5));

            let range = Some(Range {
                start: 99,
                end: 400,
            });
            let splits: HashMap<String, SplitMetadataAndFooterOffsets> = metastore
                .list_splits(index_id, SplitState::Staged, range, &[])
                .await
                .unwrap()
                .into_iter()
                .map(|metadata| (metadata.split_metadata.split_id.clone(), metadata))
                .collect();
            assert_eq!(splits.get("list-splits-one"), Some(&split_metadata_1));
            assert_eq!(splits.get("list-splits-two"), Some(&split_metadata_2));
            assert_eq!(splits.get("list-splits-three"), Some(&split_metadata_3));
            assert_eq!(splits.get("list-splits-four"), Some(&split_metadata_4));
            assert_eq!(splits.get("list-splits-five"), Some(&split_metadata_5));

            // add a split without tag
            let split_metadata_6 = make_split_meta(
                "list-splits-six".to_string(),
                SplitState::Staged,
                None,
                current_timestamp,
                to_set(&[]),
            );
            metastore
                .stage_split(index_id, split_metadata_6.clone())
                .await
                .unwrap();

            let range = None;
            let splits: HashMap<String, SplitMetadataAndFooterOffsets> = metastore
                .list_splits(index_id, SplitState::Staged, range, &[])
                .await
                .unwrap()
                .into_iter()
                .map(|metadata| (metadata.split_metadata.split_id.clone(), metadata))
                .collect();
            assert_eq!(splits.get("list-splits-one"), Some(&split_metadata_1));
            assert_eq!(splits.get("list-splits-two"), Some(&split_metadata_2));
            assert_eq!(splits.get("list-splits-three"), Some(&split_metadata_3));
            assert_eq!(splits.get("list-splits-four"), Some(&split_metadata_4));
            assert_eq!(splits.get("list-splits-five"), Some(&split_metadata_5));
            assert_eq!(splits.get("list-splits-six"), Some(&split_metadata_6));

            let range = None;
            let tags = vec!["bar".to_string(), "baz".to_string()];
            let splits: HashMap<String, SplitMetadataAndFooterOffsets> = metastore
                .list_splits(index_id, SplitState::Staged, range, &tags)
                .await
                .unwrap()
                .into_iter()
                .map(|metadata| (metadata.split_metadata.split_id.clone(), metadata))
                .collect();
            assert_eq!(splits.get("list-splits-one"), Some(&split_metadata_1));
            assert_eq!(splits.get("list-splits-two"), Some(&split_metadata_2));
            assert_eq!(splits.get("list-splits-three"), Some(&split_metadata_3));
            assert_eq!(splits.get("list-splits-four"), None);
            assert_eq!(splits.get("list-splits-five"), Some(&split_metadata_5));
            assert_eq!(splits.get("list-splits-six"), None);

            cleanup_index(&metastore, index_id).await;
        }
    }

    pub async fn test_metastore_split_update_timestamp<
        MetastoreToTest: Metastore + DefaultForTest,
    >() {
        let metastore = MetastoreToTest::default_for_test().await;

        let mut current_timestamp = Utc::now().timestamp();

        let index_id = "split-update-timestamp-index";
        let index_metadata = make_index_meta(index_id);

        let split_id = "split-update-timestamp-one";
        let split_metadata = make_split_meta(
            split_id.to_string(),
            SplitState::Staged,
            Some(RangeInclusive::new(0, 99)),
            current_timestamp,
            to_set(&[]),
        );

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
        let split_meta = metastore.list_all_splits(index_id).await.unwrap()[0]
            .clone()
            .split_metadata;
        assert!(split_meta.update_timestamp > current_timestamp);

        current_timestamp = split_meta.update_timestamp;

        // wait for 1s, publish split & check `update_timestamp`
        sleep(Duration::from_secs(1)).await;
        metastore
            .publish_splits(index_id, &[split_id], CheckpointDelta::from(0..5))
            .await
            .unwrap();
        let split_meta = metastore.list_all_splits(index_id).await.unwrap()[0]
            .clone()
            .split_metadata;
        assert!(split_meta.update_timestamp > current_timestamp);

        current_timestamp = split_meta.update_timestamp;

        // wait for 1s, mark split for deletion & check `update_timestamp`
        sleep(Duration::from_secs(1)).await;
        metastore
            .mark_splits_for_deletion(index_id, &[split_id])
            .await
            .unwrap();
        let split_meta = metastore.list_all_splits(index_id).await.unwrap()[0]
            .clone()
            .split_metadata;
        assert!(split_meta.update_timestamp > current_timestamp);

        cleanup_index(&metastore, index_id).await;
    }
}

macro_rules! metastore_test_suite {
    ($metastore_type:ty) => {
        #[cfg(test)]
        mod common_tests {
            #[tokio::test]
            async fn test_metastore_delete_index() {
                crate::tests::test_suite::test_metastore_delete_index::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_index_metadata() {
                crate::tests::test_suite::test_metastore_index_metadata::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_publish_splits() {
                crate::tests::test_suite::test_metastore_publish_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_replace_splits() {
                crate::tests::test_suite::test_metastore_replace_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_delete_splits() {
                crate::tests::test_suite::test_metastore_delete_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_list_all_splits() {
                crate::tests::test_suite::test_metastore_list_all_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_list_splits() {
                crate::tests::test_suite::test_metastore_list_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_split_update_timestamp() {
                crate::tests::test_suite::test_metastore_split_update_timestamp::<$metastore_type>(
                )
                .await;
            }
        }
    };
}

#[cfg(feature = "postgres")]
macro_rules! metastore_test_suite_for_postgresql {
    ($metastore_type:ty) => {
        #[cfg(test)]
        mod common_tests {
            #[tokio::test]
            async fn test_metastore_delete_index() {
                crate::tests::test_suite::test_metastore_delete_index::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_index_metadata() {
                crate::tests::test_suite::test_metastore_index_metadata::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_publish_splits() {
                crate::tests::test_suite::test_metastore_publish_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_replace_splits() {
                crate::tests::test_suite::test_metastore_replace_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_delete_splits() {
                crate::tests::test_suite::test_metastore_delete_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_list_all_splits() {
                crate::tests::test_suite::test_metastore_list_all_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_list_splits() {
                crate::tests::test_suite::test_metastore_list_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_split_update_timestamp() {
                crate::tests::test_suite::test_metastore_split_update_timestamp::<$metastore_type>(
                )
                .await;
            }
        }
    };
}
