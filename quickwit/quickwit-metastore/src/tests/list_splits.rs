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

use std::time::Duration;

use futures::TryStreamExt;
use itertools::Itertools;
use quickwit_common::rand::append_random_suffix;
use quickwit_config::IndexConfig;
use quickwit_doc_mapper::tag_pruning::{no_tag, tag, TagFilterAst};
use quickwit_proto::metastore::{
    CreateIndexRequest, ListSplitsRequest, ListStaleSplitsRequest, MarkSplitsForDeletionRequest,
    PublishSplitsRequest, StageSplitsRequest,
};
use quickwit_proto::types::{IndexUid, SplitId};
use time::OffsetDateTime;
use tokio::time::sleep;
use tracing::info;

use super::{to_btree_set, DefaultForTest};
use crate::metastore::MetastoreServiceStreamSplitsExt;
use crate::tests::{cleanup_index, collect_split_ids};
use crate::{
    CreateIndexRequestExt, ListSplitsQuery, ListSplitsRequestExt, ListSplitsResponseExt,
    MetastoreServiceExt, SplitMaturity, SplitMetadata, SplitState, StageSplitsRequestExt,
};

pub async fn test_metastore_list_all_splits<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
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

    let no_splits = metastore
        .list_splits(
            ListSplitsRequest::try_from_index_uid(IndexUid::new_with_random_ulid(
                "index-not-found",
            ))
            .unwrap(),
        )
        .await
        .unwrap()
        .collect_splits()
        .await
        .unwrap();
    assert!(no_splits.is_empty());

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
        .collect_splits()
        .await
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

pub async fn test_metastore_stream_splits<MetastoreToTest: MetastoreServiceExt + DefaultForTest>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-stream-splits");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let create_index_request = CreateIndexRequest::try_from_index_config(index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid
        .into();

    let mut split_metadatas_to_create = Vec::new();
    for idx in 1..10001 {
        let split_id = format!("{index_id}--split-{idx:0>5}");
        let split_metadata = SplitMetadata {
            split_id: split_id.clone(),
            index_uid: index_uid.clone(),
            ..Default::default()
        };
        split_metadatas_to_create.push(split_metadata);

        if idx > 0 && idx % 1000 == 0 {
            let staged_split_ids: Vec<SplitId> = split_metadatas_to_create
                .iter()
                .map(|split_metadata| split_metadata.split_id.clone())
                .collect();
            let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
                index_uid.clone(),
                split_metadatas_to_create.clone(),
            )
            .unwrap();
            metastore.stage_splits(stage_splits_request).await.unwrap();
            let publish_splits_request = PublishSplitsRequest {
                index_uid: index_uid.clone().to_string(),
                staged_split_ids,
                ..Default::default()
            };
            metastore
                .publish_splits(publish_splits_request)
                .await
                .unwrap();
            split_metadatas_to_create.clear();
        }
    }

    let stream_splits_request = ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap();
    let mut stream_response = metastore.list_splits(stream_splits_request).await.unwrap();
    let mut all_splits = Vec::new();
    for _ in 0..10 {
        let mut splits = stream_response
            .try_next()
            .await
            .unwrap()
            .unwrap()
            .deserialize_splits()
            .unwrap();
        assert_eq!(splits.len(), 1000);
        all_splits.append(&mut splits);
    }
    all_splits.sort_by_key(|split| split.split_id().to_string());
    assert_eq!(all_splits[0].split_id(), format!("{index_id}--split-00001"));
    assert_eq!(
        all_splits[all_splits.len() - 1].split_id(),
        format!("{index_id}--split-10000")
    );
}

pub async fn test_metastore_list_splits<MetastoreToTest: MetastoreServiceExt + DefaultForTest>() {
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
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(query).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
        assert!(splits.is_empty());
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
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
            .collect_splits()
            .await
            .unwrap();
        let split_ids = collect_split_ids(&splits);
        assert_eq!(split_ids, &[&split_id_2, &split_id_3]);

        cleanup_index(&mut metastore, index_uid).await;
    }
}

pub async fn test_metastore_list_stale_splits<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
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
    let no_splits = metastore
        .list_stale_splits(list_stale_splits_request)
        .await
        .unwrap()
        .deserialize_splits()
        .unwrap();
    assert!(no_splits.is_empty());

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
