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

use futures::TryStreamExt;
use itertools::Itertools;
use quickwit_common::rand::append_random_suffix;
use quickwit_config::IndexConfig;
use quickwit_doc_mapper::tag_pruning::{TagFilterAst, no_tag, tag};
use quickwit_proto::metastore::{
    CreateIndexRequest, ListSplitsRequest, ListStaleSplitsRequest, MarkSplitsForDeletionRequest,
    PublishSplitsRequest, StageSplitsRequest,
};
use quickwit_proto::types::{IndexUid, NodeId, SplitId};
use time::OffsetDateTime;
use tokio::time::sleep;
use tracing::info;

use super::{DefaultForTest, to_btree_set};
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

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

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
        index_uid: Some(index_uid.clone()),
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
    let metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-stream-splits");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    let mut split_metadatas_to_create = Vec::new();
    for split_idx in 1..1001 {
        let split_id = format!("{index_id}--split-{split_idx:0>4}");
        let split_metadata = SplitMetadata {
            split_id: split_id.clone(),
            index_uid: index_uid.clone(),
            ..Default::default()
        };
        split_metadatas_to_create.push(split_metadata);

        if split_idx > 0 && split_idx % 100 == 0 {
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
                index_uid: Some(index_uid.clone()),
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
            .await
            .unwrap();
        assert_eq!(splits.len(), 100);
        all_splits.append(&mut splits);
    }
    all_splits.sort_by_key(|split| split.split_id().to_string());
    assert_eq!(all_splits[0].split_id(), format!("{index_id}--split-0001"));
    assert_eq!(
        all_splits[all_splits.len() - 1].split_id(),
        format!("{index_id}--split-1000")
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
        assert!(splits.is_empty());
    }
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
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

pub async fn test_metastore_list_splits_by_node_id<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let metastore = MetastoreToTest::default_for_test().await;

    let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();
    let index_id = append_random_suffix("test-list-splits-by-node-id");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid
        .unwrap();

    let split_id_1 = format!("{index_id}--split-1");
    let split_metadata_1 = SplitMetadata {
        split_id: split_id_1.clone(),
        index_uid: index_uid.clone(),
        create_timestamp: current_timestamp,
        delete_opstamp: 20,
        node_id: "test-node-1".to_string(),
        ..Default::default()
    };
    let split_id_2 = format!("{index_id}--split-2");
    let split_metadata_2 = SplitMetadata {
        split_id: split_id_2.clone(),
        index_uid: index_uid.clone(),
        create_timestamp: current_timestamp,
        delete_opstamp: 10,
        node_id: "test-node-2".to_string(),
        ..Default::default()
    };
    let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
        index_uid.clone(),
        vec![split_metadata_1.clone(), split_metadata_2.clone()],
    )
    .unwrap();

    metastore.stage_splits(stage_splits_request).await.unwrap();

    let list_splits_query =
        ListSplitsQuery::for_index(index_uid.clone()).with_node_id(NodeId::from("test-node-1"));
    let list_splits_request =
        ListSplitsRequest::try_from_list_splits_query(&list_splits_query).unwrap();

    let splits = metastore
        .list_splits(list_splits_request)
        .await
        .unwrap()
        .collect_splits()
        .await
        .unwrap();

    assert_eq!(splits.len(), 1);
    assert_eq!(splits[0].split_metadata.split_id, split_id_1);
    assert_eq!(splits[0].split_metadata.node_id, "test-node-1");
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
        index_uid: Some(IndexUid::new_with_random_ulid("index-not-found")),
        delete_opstamp: 0,
        num_splits: 100,
    };
    let no_splits = metastore
        .list_stale_splits(list_stale_splits_request)
        .await
        .unwrap()
        .deserialize_splits()
        .await
        .unwrap();
    assert!(no_splits.is_empty());

    {
        info!("list stale splits on an index");
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
            .await
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
            .await
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
            .await
            .unwrap();
        assert!(splits.is_empty());
        cleanup_index(&mut metastore, index_uid).await;
    }
}

pub async fn test_metastore_list_sorted_splits<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let split_id = append_random_suffix("test-list-sorted-splits-");
    let index_id_1 = append_random_suffix("test-list-sorted-splits-1");
    let index_uid_1 = IndexUid::new_with_random_ulid(&index_id_1);
    let index_uri_1 = format!("ram:///indexes/{index_id_1}");
    let index_config_1 = IndexConfig::for_test(&index_id_1, &index_uri_1);

    let index_id_2 = append_random_suffix("test-list-sorted-splits-2");
    let index_uid_2 = IndexUid::new_with_random_ulid(&index_id_2);
    let index_uri_2 = format!("ram:///indexes/{index_id_2}");
    let index_config_2 = IndexConfig::for_test(&index_id_2, &index_uri_2);

    let split_id_1 = format!("{split_id}--split-1");
    let split_metadata_1 = SplitMetadata {
        split_id: split_id_1.clone(),
        index_uid: index_uid_1.clone(),
        delete_opstamp: 5,
        ..Default::default()
    };
    let split_id_2 = format!("{split_id}--split-2");
    let split_metadata_2 = SplitMetadata {
        split_id: split_id_2.clone(),
        index_uid: index_uid_2.clone(),
        delete_opstamp: 3,
        ..Default::default()
    };
    let split_id_3 = format!("{split_id}--split-3");
    let split_metadata_3 = SplitMetadata {
        split_id: split_id_3.clone(),
        index_uid: index_uid_1.clone(),
        delete_opstamp: 1,
        ..Default::default()
    };
    let split_id_4 = format!("{split_id}--split-4");
    let split_metadata_4 = SplitMetadata {
        split_id: split_id_4.clone(),
        index_uid: index_uid_2.clone(),
        delete_opstamp: 0,
        ..Default::default()
    };
    let split_id_5 = format!("{split_id}--split-5");
    let split_metadata_5 = SplitMetadata {
        split_id: split_id_5.clone(),
        index_uid: index_uid_1.clone(),
        delete_opstamp: 2,
        ..Default::default()
    };
    let split_id_6 = format!("{split_id}--split-6");
    let split_metadata_6 = SplitMetadata {
        split_id: split_id_6.clone(),
        index_uid: index_uid_2.clone(),
        delete_opstamp: 4,
        ..Default::default()
    };

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config_1).unwrap();
    let index_uid_1: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();
    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config_2).unwrap();
    let index_uid_2: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    {
        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid_1.clone(),
            vec![split_metadata_1, split_metadata_3, split_metadata_5],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid_1.clone()),
            staged_split_ids: vec![split_id_1.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        let mark_splits_for_deletion =
            MarkSplitsForDeletionRequest::new(index_uid_1.clone(), vec![split_id_3.clone()]);
        metastore
            .mark_splits_for_deletion(mark_splits_for_deletion)
            .await
            .unwrap();

        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid_2.clone(),
            vec![split_metadata_2, split_metadata_4, split_metadata_6],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid_2.clone()),
            staged_split_ids: vec![split_id_2.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        let mark_splits_for_deletion =
            MarkSplitsForDeletionRequest::new(index_uid_2.clone(), vec![split_id_4.clone()]);
        metastore
            .mark_splits_for_deletion(mark_splits_for_deletion)
            .await
            .unwrap();
    }

    let query =
        ListSplitsQuery::try_from_index_uids(vec![index_uid_1.clone(), index_uid_2.clone()])
            .unwrap()
            .sort_by_staleness();
    let splits = metastore
        .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
        .await
        .unwrap()
        .collect_splits()
        .await
        .unwrap();
    // we don't use collect_split_ids because it sorts splits internally
    let split_ids = splits
        .iter()
        .map(|split| split.split_id())
        .collect::<Vec<_>>();
    assert_eq!(
        split_ids,
        &[
            &split_id_4,
            &split_id_3,
            &split_id_5,
            &split_id_2,
            &split_id_6,
            &split_id_1,
        ]
    );

    let query =
        ListSplitsQuery::try_from_index_uids(vec![index_uid_1.clone(), index_uid_2.clone()])
            .unwrap()
            .sort_by_index_uid();
    let splits = metastore
        .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
        .await
        .unwrap()
        .collect_splits()
        .await
        .unwrap();
    // we don't use collect_split_ids because it sorts splits internally
    let split_ids = splits
        .iter()
        .map(|split| split.split_id())
        .collect::<Vec<_>>();
    assert_eq!(
        split_ids,
        &[
            &split_id_1,
            &split_id_3,
            &split_id_5,
            &split_id_2,
            &split_id_4,
            &split_id_6,
        ]
    );

    cleanup_index(&mut metastore, index_uid_1.clone()).await;
    cleanup_index(&mut metastore, index_uid_2.clone()).await;
}

pub async fn test_metastore_list_after_split<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let split_id = append_random_suffix("test-list-sorted-splits-");
    let index_id_1 = append_random_suffix("test-list-sorted-splits-1");
    let index_uri_1 = format!("ram:///indexes/{index_id_1}");
    let index_config_1 = IndexConfig::for_test(&index_id_1, &index_uri_1);

    let index_id_2 = append_random_suffix("test-list-sorted-splits-2");
    let index_uri_2 = format!("ram:///indexes/{index_id_2}");
    let index_config_2 = IndexConfig::for_test(&index_id_2, &index_uri_2);

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config_1).unwrap();
    let index_uid_1: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();
    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config_2).unwrap();
    let index_uid_2: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    let split_id_1 = format!("{split_id}--split-1");
    let split_metadata_1 = SplitMetadata {
        split_id: split_id_1.clone(),
        index_uid: index_uid_1.clone(),
        ..Default::default()
    };
    let split_id_2 = format!("{split_id}--split-2");
    let split_metadata_2 = SplitMetadata {
        split_id: split_id_2.clone(),
        index_uid: index_uid_2.clone(),
        ..Default::default()
    };
    let split_id_3 = format!("{split_id}--split-3");
    let split_metadata_3 = SplitMetadata {
        split_id: split_id_3.clone(),
        index_uid: index_uid_1.clone(),
        ..Default::default()
    };
    let split_id_4 = format!("{split_id}--split-4");
    let split_metadata_4 = SplitMetadata {
        split_id: split_id_4.clone(),
        index_uid: index_uid_2.clone(),
        ..Default::default()
    };
    let split_id_5 = format!("{split_id}--split-5");
    let split_metadata_5 = SplitMetadata {
        split_id: split_id_5.clone(),
        index_uid: index_uid_1.clone(),
        ..Default::default()
    };
    let split_id_6 = format!("{split_id}--split-6");
    let split_metadata_6 = SplitMetadata {
        split_id: split_id_6.clone(),
        index_uid: index_uid_2.clone(),
        ..Default::default()
    };

    {
        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid_1.clone(),
            vec![
                split_metadata_1.clone(),
                split_metadata_3.clone(),
                split_metadata_5.clone(),
            ],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid_1.clone()),
            staged_split_ids: vec![split_id_1.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        let mark_splits_for_deletion =
            MarkSplitsForDeletionRequest::new(index_uid_1.clone(), vec![split_id_3.clone()]);
        metastore
            .mark_splits_for_deletion(mark_splits_for_deletion)
            .await
            .unwrap();

        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid_2.clone(),
            vec![
                split_metadata_2.clone(),
                split_metadata_4.clone(),
                split_metadata_6.clone(),
            ],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid_2.clone()),
            staged_split_ids: vec![split_id_2.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        let mark_splits_for_deletion =
            MarkSplitsForDeletionRequest::new(index_uid_2.clone(), vec![split_id_4.clone()]);
        metastore
            .mark_splits_for_deletion(mark_splits_for_deletion)
            .await
            .unwrap();
    }

    let expected_all = [
        &split_metadata_1,
        &split_metadata_3,
        &split_metadata_5,
        &split_metadata_2,
        &split_metadata_4,
        &split_metadata_6,
    ];

    for i in 0..expected_all.len() {
        let after = expected_all[i];
        let expected_res = expected_all[(i + 1)..]
            .iter()
            .map(|split| (&split.index_uid, &split.split_id))
            .collect::<Vec<_>>();

        let query =
            ListSplitsQuery::try_from_index_uids(vec![index_uid_1.clone(), index_uid_2.clone()])
                .unwrap()
                .sort_by_index_uid()
                .after_split(after);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
        // we don't use collect_split_ids because it sorts splits internally
        let split_ids = splits
            .iter()
            .map(|split| {
                (
                    &split.split_metadata.index_uid,
                    &split.split_metadata.split_id,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(split_ids, expected_res,);
    }

    cleanup_index(&mut metastore, index_uid_1.clone()).await;
    cleanup_index(&mut metastore, index_uid_2.clone()).await;
}

pub async fn test_metastore_list_splits_from_all_indexes<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let split_id = append_random_suffix("test-list-sorted-splits-");
    let index_id_1 = append_random_suffix("test-list-sorted-splits-1");
    let index_uri_1 = format!("ram:///indexes/{index_id_1}");
    let index_config_1 = IndexConfig::for_test(&index_id_1, &index_uri_1);

    let index_id_2 = append_random_suffix("test-list-sorted-splits-2");
    let index_uri_2 = format!("ram:///indexes/{index_id_2}");
    let index_config_2 = IndexConfig::for_test(&index_id_2, &index_uri_2);

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config_1).unwrap();
    let index_uid_1: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();
    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config_2).unwrap();
    let index_uid_2: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    let split_id_1 = format!("{split_id}--split-1");
    let split_metadata_1 = SplitMetadata {
        split_id: split_id_1.clone(),
        index_uid: index_uid_1.clone(),
        ..Default::default()
    };
    let split_id_2 = format!("{split_id}--split-2");
    let split_metadata_2 = SplitMetadata {
        split_id: split_id_2.clone(),
        index_uid: index_uid_2.clone(),
        ..Default::default()
    };
    let split_id_3 = format!("{split_id}--split-3");
    let split_metadata_3 = SplitMetadata {
        split_id: split_id_3.clone(),
        index_uid: index_uid_1.clone(),
        ..Default::default()
    };
    let split_id_4 = format!("{split_id}--split-4");
    let split_metadata_4 = SplitMetadata {
        split_id: split_id_4.clone(),
        index_uid: index_uid_2.clone(),
        ..Default::default()
    };
    let split_id_5 = format!("{split_id}--split-5");
    let split_metadata_5 = SplitMetadata {
        split_id: split_id_5.clone(),
        index_uid: index_uid_1.clone(),
        ..Default::default()
    };
    let split_id_6 = format!("{split_id}--split-6");
    let split_metadata_6 = SplitMetadata {
        split_id: split_id_6.clone(),
        index_uid: index_uid_2.clone(),
        ..Default::default()
    };

    {
        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid_1.clone(),
            vec![
                split_metadata_1.clone(),
                split_metadata_3.clone(),
                split_metadata_5.clone(),
            ],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid_1.clone()),
            staged_split_ids: vec![split_id_1.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        let mark_splits_for_deletion =
            MarkSplitsForDeletionRequest::new(index_uid_1.clone(), vec![split_id_3.clone()]);
        metastore
            .mark_splits_for_deletion(mark_splits_for_deletion)
            .await
            .unwrap();

        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid_2.clone(),
            vec![
                split_metadata_2.clone(),
                split_metadata_4.clone(),
                split_metadata_6.clone(),
            ],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid_2.clone()),
            staged_split_ids: vec![split_id_2.clone()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();

        let mark_splits_for_deletion =
            MarkSplitsForDeletionRequest::new(index_uid_2.clone(), vec![split_id_4.clone()]);
        metastore
            .mark_splits_for_deletion(mark_splits_for_deletion)
            .await
            .unwrap();
    }

    let expected_all = [
        &split_metadata_1,
        &split_metadata_3,
        &split_metadata_5,
        &split_metadata_2,
        &split_metadata_4,
        &split_metadata_6,
    ];

    let expected_res = expected_all[1..]
        .iter()
        .map(|split| (&split.index_uid, &split.split_id))
        .collect::<Vec<_>>();

    let query = ListSplitsQuery::for_all_indexes()
        .sort_by_index_uid()
        .after_split(expected_all[0]);
    let splits = metastore
        .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
        .await
        .unwrap()
        .collect_splits()
        .await
        .unwrap();
    // we don't use collect_split_ids because it sorts splits internally
    let split_ids = splits
        .iter()
        .map(|split| {
            (
                &split.split_metadata.index_uid,
                &split.split_metadata.split_id,
            )
        })
        // when running this test against a clean database, this line isn't needed. In practice,
        // any test that leaves any split behind breaks this test if we remove this filter
        .filter(|(index_uid, _split_id)| {
            [index_uid_1.clone(), index_uid_2.clone()].contains(index_uid)
        })
        .collect::<Vec<_>>();
    assert_eq!(split_ids, expected_res);

    cleanup_index(&mut metastore, index_uid_1.clone()).await;
    cleanup_index(&mut metastore, index_uid_2.clone()).await;
}
