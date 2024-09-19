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

use async_trait::async_trait;
use itertools::Itertools;
use quickwit_common::rand::append_random_suffix;
use quickwit_config::{IndexConfig, SourceConfig};
use quickwit_proto::compatibility_shard_update_timestamp;
use quickwit_proto::ingest::{Shard, ShardState};
use quickwit_proto::metastore::{
    AcquireShardsRequest, AddSourceRequest, CreateIndexRequest, DeleteShardsRequest, EntityKind,
    ListShardsRequest, ListShardsSubrequest, MetastoreError, MetastoreService, OpenShardSubrequest,
    OpenShardsRequest, PruneShardsRequest, PublishSplitsRequest,
};
use quickwit_proto::types::{DocMappingUid, IndexUid, Position, ShardId, SourceId};
use time::OffsetDateTime;

use super::DefaultForTest;
use crate::checkpoint::{IndexCheckpointDelta, PartitionId, SourceCheckpointDelta};
use crate::tests::cleanup_index;
use crate::{AddSourceRequestExt, CreateIndexRequestExt, MetastoreServiceExt};

#[async_trait]
pub trait ReadWriteShardsForTest {
    async fn insert_shards(&self, index_uid: &IndexUid, source_id: &SourceId, shards: Vec<Shard>);

    async fn list_all_shards(&self, index_uid: &IndexUid, source_id: &SourceId) -> Vec<Shard>;
}

struct TestIndex {
    index_uid: IndexUid,
    source_id: SourceId,
}

impl TestIndex {
    async fn create_index_with_source(
        metastore: &mut dyn MetastoreService,
        index_id: &str,
        source_config: SourceConfig,
    ) -> Self {
        let index_id = append_random_suffix(index_id);
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let create_index_response = metastore.create_index(create_index_request).await.unwrap();
        let index_uid: IndexUid = create_index_response.index_uid().clone();

        let add_source_request =
            AddSourceRequest::try_from_source_config(index_uid.clone(), &source_config).unwrap();
        metastore.add_source(add_source_request).await.unwrap();

        Self {
            index_uid,
            source_id: source_config.source_id,
        }
    }
}

pub async fn test_metastore_open_shards<
    MetastoreUnderTest: MetastoreService + MetastoreServiceExt + DefaultForTest + ReadWriteShardsForTest,
>() {
    let mut metastore = MetastoreUnderTest::default_for_test().await;

    let test_index = TestIndex::create_index_with_source(
        &mut metastore,
        "test-open-shards",
        SourceConfig::ingest_v2(),
    )
    .await;

    // Test empty request.
    let open_shards_request = OpenShardsRequest {
        subrequests: Vec::new(),
    };
    let open_shards_response = metastore.open_shards(open_shards_request).await.unwrap();
    assert!(open_shards_response.subresponses.is_empty());

    // Test index not found.
    // let open_shards_request = OpenShardsRequest {
    //     subrequests: vec![OpenShardSubrequest {
    //         index_uid: "index-does-not-exist:0".to_string(),
    //         source_id: test_index.source_id.clone(),
    //         leader_id: "test-ingester-foo".to_string(),
    //         ..Default::default()
    //     }],
    // };
    // let error = metastore
    //     .open_shards(open_shards_request)
    //     .await
    //     .unwrap_err();
    // assert!(
    //     matches!(error, MetastoreError::NotFound(EntityKind::Index { index_id }) if index_id ==
    // "index-does-not-exist") );

    // // Test source not found.
    // let open_shards_request = OpenShardsRequest {
    //     subrequests: vec![OpenShardSubrequest {
    //         index_uid: Some(test_index.index_uid.clone()),
    //         source_id: "source-does-not-exist".to_string(),
    //         leader_id: "test-ingester-foo".to_string(),
    //         ..Default::default()
    //     }],
    // };
    // let error = metastore
    //     .open_shards(open_shards_request)
    //     .await
    //     .unwrap_err();
    // assert!(
    //     matches!(error, MetastoreError::NotFound(EntityKind::Source { source_id, ..}) if
    // source_id == "source-does-not-exist") );

    // Test open shard #1.
    let open_shards_request = OpenShardsRequest {
        subrequests: vec![OpenShardSubrequest {
            subrequest_id: 0,
            index_uid: Some(test_index.index_uid.clone()),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            leader_id: "test-ingester-foo".to_string(),
            follower_id: Some("test-ingester-bar".to_string()),
            doc_mapping_uid: Some(DocMappingUid::default()),
            publish_token: None,
        }],
    };
    let open_shards_response = metastore.open_shards(open_shards_request).await.unwrap();
    assert_eq!(open_shards_response.subresponses.len(), 1);

    let subresponse = &open_shards_response.subresponses[0];
    assert_eq!(subresponse.subrequest_id, 0);

    let shard = subresponse.open_shard();
    assert_eq!(shard.index_uid(), &test_index.index_uid);
    assert_eq!(shard.source_id, test_index.source_id);
    assert_eq!(shard.shard_id(), ShardId::from(1));
    assert_eq!(shard.shard_state(), ShardState::Open);
    assert_eq!(shard.leader_id, "test-ingester-foo");
    assert_eq!(shard.follower_id(), "test-ingester-bar");
    assert_eq!(shard.doc_mapping_uid(), DocMappingUid::default(),);
    assert_eq!(shard.publish_position_inclusive(), Position::Beginning);
    let shard_ts = shard.update_timestamp;
    assert_ne!(shard_ts, compatibility_shard_update_timestamp());
    assert_ne!(shard_ts, 0);
    assert!(shard.publish_token.is_none());

    // Test open shard #1 is idempotent.
    let open_shards_request = OpenShardsRequest {
        subrequests: vec![OpenShardSubrequest {
            subrequest_id: 0,
            index_uid: Some(test_index.index_uid.clone()),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            leader_id: "test-ingester-foo".to_string(),
            follower_id: Some("test-ingester-bar".to_string()),
            doc_mapping_uid: Some(DocMappingUid::default()),
            publish_token: Some("publish-token-baz".to_string()),
        }],
    };
    let open_shards_response = metastore.open_shards(open_shards_request).await.unwrap();
    assert_eq!(open_shards_response.subresponses.len(), 1);

    let subresponse = &open_shards_response.subresponses[0];
    assert_eq!(subresponse.subrequest_id, 0);

    let shard = subresponse.open_shard();
    assert_eq!(shard.index_uid(), &test_index.index_uid);
    assert_eq!(shard.source_id, test_index.source_id);
    assert_eq!(shard.shard_id(), ShardId::from(1));
    assert_eq!(shard.shard_state(), ShardState::Open);
    assert_eq!(shard.leader_id, "test-ingester-foo");
    assert_eq!(shard.follower_id(), "test-ingester-bar");
    assert_eq!(shard.publish_position_inclusive(), Position::Beginning);
    assert_eq!(shard.update_timestamp, shard_ts);
    assert!(shard.publish_token.is_none());

    // Test open shard #2.
    let open_shards_request = OpenShardsRequest {
        subrequests: vec![OpenShardSubrequest {
            subrequest_id: 0,
            index_uid: Some(test_index.index_uid.clone()),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(2)),
            leader_id: "test-ingester-foo".to_string(),
            follower_id: None,
            doc_mapping_uid: Some(DocMappingUid::default()),
            publish_token: Some("publish-token-open".to_string()),
        }],
    };
    let open_shards_response = metastore.open_shards(open_shards_request).await.unwrap();
    assert_eq!(open_shards_response.subresponses.len(), 1);

    let subresponse = &open_shards_response.subresponses[0];
    assert_eq!(subresponse.subrequest_id, 0);

    let shard = subresponse.open_shard();
    assert_eq!(shard.index_uid(), &test_index.index_uid);
    assert_eq!(shard.source_id, test_index.source_id);
    assert_eq!(shard.shard_id(), ShardId::from(2));
    assert_eq!(shard.shard_state(), ShardState::Open);
    assert_eq!(shard.leader_id, "test-ingester-foo");
    assert!(shard.follower_id.is_none());
    assert_eq!(shard.publish_position_inclusive(), Position::Beginning);
    assert_eq!(shard.publish_token(), "publish-token-open");

    cleanup_index(&mut metastore, test_index.index_uid).await;
}

pub async fn test_metastore_acquire_shards<
    MetastoreUnderTest: MetastoreService + MetastoreServiceExt + DefaultForTest + ReadWriteShardsForTest,
>() {
    let mut metastore = MetastoreUnderTest::default_for_test().await;

    let test_index = TestIndex::create_index_with_source(
        &mut metastore,
        "test-acquire-shards",
        SourceConfig::ingest_v2(),
    )
    .await;

    let shards = vec![
        Shard {
            index_uid: Some(test_index.index_uid.clone()),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Closed as i32,
            leader_id: "test-ingester-foo".to_string(),
            follower_id: Some("test-ingester-bar".to_string()),
            doc_mapping_uid: Some(DocMappingUid::default()),
            publish_position_inclusive: Some(Position::Beginning),
            publish_token: Some("test-publish-token-foo".to_string()),
            update_timestamp: 1724158996,
        },
        Shard {
            index_uid: Some(test_index.index_uid.clone()),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(2)),
            shard_state: ShardState::Open as i32,
            leader_id: "test-ingester-bar".to_string(),
            follower_id: Some("test-ingester-qux".to_string()),
            doc_mapping_uid: Some(DocMappingUid::default()),
            publish_position_inclusive: Some(Position::Beginning),
            publish_token: Some("test-publish-token-bar".to_string()),
            update_timestamp: 1724158996,
        },
        Shard {
            index_uid: Some(test_index.index_uid.clone()),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(3)),
            shard_state: ShardState::Open as i32,
            leader_id: "test-ingester-qux".to_string(),
            follower_id: Some("test-ingester-baz".to_string()),
            doc_mapping_uid: Some(DocMappingUid::default()),
            publish_position_inclusive: Some(Position::Beginning),
            publish_token: None,
            update_timestamp: 1724158996,
        },
        Shard {
            index_uid: Some(test_index.index_uid.clone()),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(4)),
            shard_state: ShardState::Open as i32,
            leader_id: "test-ingester-baz".to_string(),
            follower_id: Some("test-ingester-tux".to_string()),
            doc_mapping_uid: Some(DocMappingUid::default()),
            publish_position_inclusive: Some(Position::Beginning),
            publish_token: None,
            update_timestamp: 1724158996,
        },
    ];
    metastore
        .insert_shards(&test_index.index_uid, &test_index.source_id, shards)
        .await;

    // Test acquire shards.
    let acquire_shards_request = AcquireShardsRequest {
        index_uid: Some(test_index.index_uid.clone()),
        source_id: test_index.source_id.clone(),
        shard_ids: vec![
            ShardId::from(1),
            ShardId::from(2),
            ShardId::from(3),
            ShardId::from(666),
        ], // shard 666 does not exist
        publish_token: "test-publish-token-foo".to_string(),
    };
    let mut acquire_shards_response = metastore
        .acquire_shards(acquire_shards_request)
        .await
        .unwrap();

    acquire_shards_response
        .acquired_shards
        .sort_unstable_by(|left, right| left.shard_id().cmp(right.shard_id()));

    let shard = &acquire_shards_response.acquired_shards[0];
    assert_eq!(shard.index_uid(), &test_index.index_uid);
    assert_eq!(shard.source_id, test_index.source_id);
    assert_eq!(shard.shard_id(), ShardId::from(1));
    assert_eq!(shard.shard_state(), ShardState::Closed);
    assert_eq!(shard.leader_id, "test-ingester-foo");
    assert_eq!(shard.follower_id(), "test-ingester-bar");
    assert_eq!(shard.publish_position_inclusive(), Position::Beginning);
    assert_eq!(shard.publish_token(), "test-publish-token-foo");

    let shard = &acquire_shards_response.acquired_shards[1];
    assert_eq!(shard.index_uid(), &test_index.index_uid);
    assert_eq!(shard.source_id, test_index.source_id);
    assert_eq!(shard.shard_id(), ShardId::from(2));
    assert_eq!(shard.shard_state(), ShardState::Open);
    assert_eq!(shard.leader_id, "test-ingester-bar");
    assert_eq!(shard.follower_id(), "test-ingester-qux");
    assert_eq!(shard.publish_position_inclusive(), Position::Beginning);
    assert_eq!(shard.publish_token(), "test-publish-token-foo");

    let shard = &acquire_shards_response.acquired_shards[2];
    assert_eq!(shard.index_uid(), &test_index.index_uid);
    assert_eq!(shard.source_id, test_index.source_id);
    assert_eq!(shard.shard_id(), ShardId::from(3));
    assert_eq!(shard.shard_state(), ShardState::Open);
    assert_eq!(shard.leader_id, "test-ingester-qux");
    assert_eq!(shard.follower_id(), "test-ingester-baz");
    assert_eq!(shard.publish_position_inclusive(), Position::Beginning);
    assert_eq!(shard.publish_token(), "test-publish-token-foo");

    cleanup_index(&mut metastore, test_index.index_uid).await;
}

pub async fn test_metastore_list_shards<
    MetastoreUnderTest: MetastoreService + MetastoreServiceExt + DefaultForTest + ReadWriteShardsForTest,
>() {
    let mut metastore = MetastoreUnderTest::default_for_test().await;

    let test_index_0 = TestIndex::create_index_with_source(
        &mut metastore,
        "test-list-shards-0",
        SourceConfig::ingest_v2(),
    )
    .await;

    let test_index_1 = TestIndex::create_index_with_source(
        &mut metastore,
        "test-list-shards-1",
        SourceConfig::ingest_v2(),
    )
    .await;

    for test_index in [&test_index_0, &test_index_1] {
        let shards = vec![
            Shard {
                index_uid: Some(test_index.index_uid.clone()),
                source_id: test_index.source_id.clone(),
                shard_id: Some(ShardId::from(1)),
                shard_state: ShardState::Open as i32,
                leader_id: "test-ingester-foo".to_string(),
                follower_id: Some("test-ingester-bar".to_string()),
                doc_mapping_uid: Some(DocMappingUid::default()),
                publish_position_inclusive: Some(Position::Beginning),
                publish_token: Some("test-publish-token-foo".to_string()),
                update_timestamp: 1724158996,
            },
            Shard {
                index_uid: Some(test_index.index_uid.clone()),
                source_id: test_index.source_id.clone(),
                shard_id: Some(ShardId::from(2)),
                shard_state: ShardState::Closed as i32,
                leader_id: "test-ingester-bar".to_string(),
                follower_id: Some("test-ingester-qux".to_string()),
                doc_mapping_uid: Some(DocMappingUid::default()),
                publish_position_inclusive: Some(Position::Beginning),
                publish_token: Some("test-publish-token-bar".to_string()),
                update_timestamp: 1724158997,
            },
        ];
        metastore
            .insert_shards(&test_index.index_uid, &test_index.source_id, shards)
            .await;
    }

    // Test list shards.
    let list_shards_request = ListShardsRequest {
        subrequests: vec![
            ListShardsSubrequest {
                index_uid: Some(test_index_0.index_uid.clone()),
                source_id: test_index_0.source_id.clone(),
                shard_state: None,
            },
            ListShardsSubrequest {
                index_uid: Some(test_index_1.index_uid.clone()),
                source_id: test_index_1.source_id.clone(),
                shard_state: None,
            },
        ],
    };
    let mut list_shards_response = metastore.list_shards(list_shards_request).await.unwrap();
    assert_eq!(list_shards_response.subresponses.len(), 2);

    list_shards_response
        .subresponses
        .sort_unstable_by(|left, right| left.index_uid().cmp(right.index_uid()));

    for (idx, test_index) in [&test_index_0, &test_index_1].into_iter().enumerate() {
        let subresponse = &mut list_shards_response.subresponses[idx];
        assert_eq!(subresponse.index_uid(), &test_index.index_uid);
        assert_eq!(subresponse.source_id, test_index.source_id);
        assert_eq!(subresponse.shards.len(), 2);

        subresponse
            .shards
            .sort_unstable_by(|left, right| left.shard_id.cmp(&right.shard_id));

        let shard = &subresponse.shards[0];
        assert_eq!(shard.index_uid(), &test_index.index_uid);
        assert_eq!(shard.source_id, test_index.source_id);
        assert_eq!(shard.shard_id(), ShardId::from(1));
        assert_eq!(shard.shard_state(), ShardState::Open);
        assert_eq!(shard.leader_id, "test-ingester-foo");
        assert_eq!(shard.follower_id(), "test-ingester-bar");
        assert_eq!(shard.publish_position_inclusive(), Position::Beginning);
        assert_eq!(shard.publish_token(), "test-publish-token-foo");
        assert_eq!(shard.update_timestamp, 1724158996);

        let shard = &subresponse.shards[1];
        assert_eq!(shard.index_uid(), &test_index.index_uid);
        assert_eq!(shard.source_id, test_index.source_id);
        assert_eq!(shard.shard_id(), ShardId::from(2));
        assert_eq!(shard.shard_state(), ShardState::Closed);
        assert_eq!(shard.leader_id, "test-ingester-bar");
        assert_eq!(shard.follower_id(), "test-ingester-qux");
        assert_eq!(shard.publish_position_inclusive(), Position::Beginning);
        assert_eq!(shard.publish_token(), "test-publish-token-bar");
        assert_eq!(shard.update_timestamp, 1724158997);
    }

    // Test list shards with shard state filter.
    let list_shards_request = ListShardsRequest {
        subrequests: vec![
            ListShardsSubrequest {
                index_uid: Some(test_index_0.index_uid.clone()),
                source_id: test_index_0.source_id.clone(),
                shard_state: Some(ShardState::Open as i32),
            },
            ListShardsSubrequest {
                index_uid: Some(test_index_1.index_uid.clone()),
                source_id: test_index_1.source_id.clone(),
                shard_state: Some(ShardState::Closed as i32),
            },
        ],
    };
    let mut list_shards_response = metastore.list_shards(list_shards_request).await.unwrap();
    assert_eq!(list_shards_response.subresponses.len(), 2);

    list_shards_response
        .subresponses
        .sort_unstable_by(|left, right| left.index_uid().cmp(right.index_uid()));

    assert_eq!(list_shards_response.subresponses[0].shards.len(), 1);

    let shard = &list_shards_response.subresponses[0].shards[0];
    assert_eq!(shard.shard_id(), ShardId::from(1));
    assert_eq!(shard.shard_state(), ShardState::Open);

    assert_eq!(list_shards_response.subresponses[1].shards.len(), 1);

    let shard = &list_shards_response.subresponses[1].shards[0];
    assert_eq!(shard.shard_id(), ShardId::from(2));
    assert_eq!(shard.shard_state(), ShardState::Closed);

    let list_shards_request = ListShardsRequest {
        subrequests: vec![ListShardsSubrequest {
            index_uid: Some(test_index_0.index_uid.clone()),
            source_id: test_index_0.source_id.clone(),
            shard_state: Some(ShardState::Unavailable as i32),
        }],
    };
    let list_shards_response = metastore.list_shards(list_shards_request).await.unwrap();
    assert_eq!(list_shards_response.subresponses.len(), 1);
    assert!(list_shards_response.subresponses[0].shards.is_empty());

    cleanup_index(&mut metastore, test_index_0.index_uid).await;
}

pub async fn test_metastore_delete_shards<
    MetastoreUnderTest: MetastoreService + MetastoreServiceExt + DefaultForTest + ReadWriteShardsForTest,
>() {
    let mut metastore = MetastoreUnderTest::default_for_test().await;

    let test_index = TestIndex::create_index_with_source(
        &mut metastore,
        "test-delete-shards",
        SourceConfig::ingest_v2(),
    )
    .await;

    let shards = vec![
        Shard {
            index_uid: Some(test_index.index_uid.clone()),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(DocMappingUid::default()),
            publish_position_inclusive: Some(Position::Beginning),
            ..Default::default()
        },
        Shard {
            index_uid: Some(test_index.index_uid.clone()),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(2)),
            shard_state: ShardState::Closed as i32,
            doc_mapping_uid: Some(DocMappingUid::default()),
            publish_position_inclusive: Some(Position::Beginning),
            ..Default::default()
        },
        Shard {
            index_uid: Some(test_index.index_uid.clone()),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(3)),
            shard_state: ShardState::Closed as i32,
            doc_mapping_uid: Some(DocMappingUid::default()),
            publish_position_inclusive: Some(Position::Eof(None)),
            ..Default::default()
        },
    ];
    metastore
        .insert_shards(&test_index.index_uid, &test_index.source_id, shards)
        .await;

    // Attempt to delete shards #1, #2, #3, and #4.
    let delete_index_request = DeleteShardsRequest {
        index_uid: Some(test_index.index_uid.clone()),
        source_id: test_index.source_id.clone(),
        shard_ids: vec![
            ShardId::from(1),
            ShardId::from(2),
            ShardId::from(3),
            ShardId::from(4),
        ],
        force: false,
    };
    let mut response = metastore.delete_shards(delete_index_request).await.unwrap();

    assert_eq!(response.index_uid(), &test_index.index_uid);
    assert_eq!(response.source_id, test_index.source_id);
    assert_eq!(response.successes.len(), 2);
    assert_eq!(response.failures.len(), 2);

    response.successes.sort_unstable();
    assert_eq!(response.successes[0], ShardId::from(3));
    assert_eq!(response.successes[1], ShardId::from(4));

    response.failures.sort_unstable();
    assert_eq!(response.failures[0], ShardId::from(1));
    assert_eq!(response.failures[1], ShardId::from(2));

    let mut all_shards = metastore
        .list_all_shards(&test_index.index_uid, &test_index.source_id)
        .await;
    assert_eq!(all_shards.len(), 2);

    all_shards.sort_unstable_by(|left, right| left.shard_id.cmp(&right.shard_id));

    assert_eq!(all_shards[0].shard_id(), ShardId::from(1));
    assert_eq!(all_shards[1].shard_id(), ShardId::from(2));

    // Attempt to delete shards #1, #2, #3, and #4.
    let delete_index_request = DeleteShardsRequest {
        index_uid: Some(test_index.index_uid.clone()),
        source_id: test_index.source_id.clone(),
        shard_ids: vec![
            ShardId::from(1),
            ShardId::from(2),
            ShardId::from(3),
            ShardId::from(4),
        ],
        force: true,
    };
    let mut response = metastore.delete_shards(delete_index_request).await.unwrap();

    assert_eq!(response.index_uid(), &test_index.index_uid);
    assert_eq!(response.source_id, test_index.source_id);

    assert_eq!(response.successes.len(), 4);
    assert_eq!(response.failures.len(), 0);

    response.successes.sort_unstable();
    assert_eq!(response.successes[0], ShardId::from(1));
    assert_eq!(response.successes[1], ShardId::from(2));
    assert_eq!(response.successes[2], ShardId::from(3));
    assert_eq!(response.successes[3], ShardId::from(4));

    let all_shards = metastore
        .list_all_shards(&test_index.index_uid, &test_index.source_id)
        .await;

    assert_eq!(all_shards.len(), 0);

    cleanup_index(&mut metastore, test_index.index_uid).await;
}

pub async fn test_metastore_prune_shards<
    MetastoreUnderTest: MetastoreService + MetastoreServiceExt + DefaultForTest + ReadWriteShardsForTest,
>() {
    let mut metastore = MetastoreUnderTest::default_for_test().await;

    let test_index = TestIndex::create_index_with_source(
        &mut metastore,
        "test-prune-shards",
        SourceConfig::ingest_v2(),
    )
    .await;

    let now_timestamp = OffsetDateTime::now_utc().unix_timestamp();
    let oldest_shard_age = 10000u32;

    // Create shards with timestamp intervals of 100s starting from
    // now_timestamp - oldest_shard_age
    let shards = (0..100)
        .map(|shard_id| Shard {
            index_uid: Some(test_index.index_uid.clone()),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(shard_id)),
            shard_state: ShardState::Closed as i32,
            doc_mapping_uid: Some(DocMappingUid::default()),
            publish_position_inclusive: Some(Position::Beginning),
            update_timestamp: now_timestamp - oldest_shard_age as i64 + shard_id as i64 * 100,
            ..Default::default()
        })
        .collect_vec();

    metastore
        .insert_shards(&test_index.index_uid, &test_index.source_id, shards)
        .await;

    // noop prune request
    {
        let prune_index_request = PruneShardsRequest {
            index_uid: Some(test_index.index_uid.clone()),
            source_id: test_index.source_id.clone(),
            max_age_secs: None,
            max_count: None,
            interval: None,
        };
        metastore.prune_shards(prune_index_request).await.unwrap();
        let all_shards = metastore
            .list_all_shards(&test_index.index_uid, &test_index.source_id)
            .await;
        assert_eq!(all_shards.len(), 100);
    }

    // delete shards 4 last shards with age limit
    {
        let prune_index_request = PruneShardsRequest {
            index_uid: Some(test_index.index_uid.clone()),
            source_id: test_index.source_id.clone(),
            max_age_secs: Some(oldest_shard_age - 350),
            max_count: None,
            interval: None,
        };
        metastore.prune_shards(prune_index_request).await.unwrap();

        let mut all_shards = metastore
            .list_all_shards(&test_index.index_uid, &test_index.source_id)
            .await;
        assert_eq!(all_shards.len(), 96);
        all_shards.sort_unstable_by_key(|shard| shard.update_timestamp);
        assert_eq!(all_shards[0].shard_id(), ShardId::from(4));
        assert_eq!(all_shards[95].shard_id(), ShardId::from(99));
    }

    // delete 6 more shards with count limit
    {
        let prune_index_request = PruneShardsRequest {
            index_uid: Some(test_index.index_uid.clone()),
            source_id: test_index.source_id.clone(),
            max_age_secs: None,
            max_count: Some(90),
            interval: None,
        };
        metastore.prune_shards(prune_index_request).await.unwrap();
        let mut all_shards = metastore
            .list_all_shards(&test_index.index_uid, &test_index.source_id)
            .await;
        assert_eq!(all_shards.len(), 90);
        all_shards.sort_unstable_by_key(|shard| shard.update_timestamp);
        assert_eq!(all_shards[0].shard_id(), ShardId::from(10));
        assert_eq!(all_shards[89].shard_id(), ShardId::from(99));
    }

    // age limit is the limiting factor, delete 10 more shards
    let prune_index_request = PruneShardsRequest {
        index_uid: Some(test_index.index_uid.clone()),
        source_id: test_index.source_id.clone(),
        max_age_secs: Some(oldest_shard_age - 2950),
        max_count: Some(80),
        interval: None,
    };
    metastore.prune_shards(prune_index_request).await.unwrap();
    let all_shards = metastore
        .list_all_shards(&test_index.index_uid, &test_index.source_id)
        .await;
    assert_eq!(all_shards.len(), 70);

    // count limit is the limiting factor, delete 20 more shards
    let prune_index_request = PruneShardsRequest {
        index_uid: Some(test_index.index_uid.clone()),
        source_id: test_index.source_id.clone(),
        max_age_secs: Some(oldest_shard_age - 4000),
        max_count: Some(50),
        interval: None,
    };
    metastore.prune_shards(prune_index_request).await.unwrap();
    let all_shards = metastore
        .list_all_shards(&test_index.index_uid, &test_index.source_id)
        .await;
    assert_eq!(all_shards.len(), 50);

    cleanup_index(&mut metastore, test_index.index_uid).await;
}

pub async fn test_metastore_apply_checkpoint_delta_v2_single_shard<
    MetastoreUnderTest: MetastoreService + MetastoreServiceExt + DefaultForTest + ReadWriteShardsForTest,
>() {
    let mut metastore = MetastoreUnderTest::default_for_test().await;

    let test_index = TestIndex::create_index_with_source(
        &mut metastore,
        "test-delete-shards",
        SourceConfig::ingest_v2(),
    )
    .await;

    let mut source_delta = SourceCheckpointDelta::default();
    source_delta
        .record_partition_delta(
            PartitionId::from(0u64),
            Position::Beginning,
            Position::offset(0u64),
        )
        .unwrap();
    let index_checkpoint_delta = IndexCheckpointDelta {
        source_id: test_index.source_id.clone(),
        source_delta,
    };
    let index_checkpoint_delta_json = serde_json::to_string(&index_checkpoint_delta).unwrap();
    let publish_splits_request = PublishSplitsRequest {
        index_uid: Some(test_index.index_uid.clone()),
        staged_split_ids: Vec::new(),
        replaced_split_ids: Vec::new(),
        index_checkpoint_delta_json_opt: Some(index_checkpoint_delta_json),
        publish_token_opt: Some("test-publish-token-foo".to_string()),
    };
    let error = metastore
        .publish_splits(publish_splits_request)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MetastoreError::NotFound(EntityKind::Shard { .. })
    ));

    let dummy_create_timestamp = 1;
    let shards = vec![Shard {
        index_uid: Some(test_index.index_uid.clone()),
        source_id: test_index.source_id.clone(),
        shard_id: Some(ShardId::from(0)),
        shard_state: ShardState::Open as i32,
        doc_mapping_uid: Some(DocMappingUid::default()),
        publish_position_inclusive: Some(Position::Beginning),
        publish_token: Some("test-publish-token-bar".to_string()),
        update_timestamp: dummy_create_timestamp,
        ..Default::default()
    }];
    metastore
        .insert_shards(&test_index.index_uid, &test_index.source_id, shards)
        .await;

    let index_checkpoint_delta_json = serde_json::to_string(&index_checkpoint_delta).unwrap();
    let publish_splits_request = PublishSplitsRequest {
        index_uid: Some(test_index.index_uid.clone()),
        staged_split_ids: Vec::new(),
        replaced_split_ids: Vec::new(),
        index_checkpoint_delta_json_opt: Some(index_checkpoint_delta_json),
        publish_token_opt: Some("test-publish-token-foo".to_string()),
    };
    let error = metastore
        .publish_splits(publish_splits_request.clone())
        .await
        .unwrap_err();
    assert!(
        matches!(error, MetastoreError::InvalidArgument { message } if message.contains("token"))
    );

    let index_checkpoint_delta_json = serde_json::to_string(&index_checkpoint_delta).unwrap();
    let publish_splits_request = PublishSplitsRequest {
        index_uid: Some(test_index.index_uid.clone()),
        staged_split_ids: Vec::new(),
        replaced_split_ids: Vec::new(),
        index_checkpoint_delta_json_opt: Some(index_checkpoint_delta_json),
        publish_token_opt: Some("test-publish-token-bar".to_string()),
    };
    metastore
        .publish_splits(publish_splits_request.clone())
        .await
        .unwrap();

    let shards = metastore
        .list_all_shards(&test_index.index_uid, &test_index.source_id)
        .await;
    assert_eq!(shards.len(), 1);
    assert_eq!(shards[0].shard_state(), ShardState::Open);
    assert_eq!(
        shards[0].publish_position_inclusive(),
        Position::offset(0u64)
    );
    assert!(
        shards[0].update_timestamp > dummy_create_timestamp,
        "shard timestamp was not updated"
    );

    let index_checkpoint_delta_json = serde_json::to_string(&index_checkpoint_delta).unwrap();
    let publish_splits_request = PublishSplitsRequest {
        index_uid: Some(test_index.index_uid.clone()),
        staged_split_ids: Vec::new(),
        replaced_split_ids: Vec::new(),
        index_checkpoint_delta_json_opt: Some(index_checkpoint_delta_json),
        publish_token_opt: Some("test-publish-token-bar".to_string()),
    };
    let error = metastore
        .publish_splits(publish_splits_request.clone())
        .await
        .unwrap_err();
    assert!(
        matches!(error, MetastoreError::InvalidArgument { message } if message.contains("checkpoint"))
    );

    let mut source_delta = SourceCheckpointDelta::default();
    source_delta
        .record_partition_delta(
            PartitionId::from(0u64),
            Position::offset(0u64),
            Position::eof(1u64),
        )
        .unwrap();
    let index_checkpoint_delta = IndexCheckpointDelta {
        source_id: test_index.source_id.clone(),
        source_delta,
    };
    let index_checkpoint_delta_json = serde_json::to_string(&index_checkpoint_delta).unwrap();
    let publish_splits_request = PublishSplitsRequest {
        index_uid: Some(test_index.index_uid.clone()),
        staged_split_ids: Vec::new(),
        replaced_split_ids: Vec::new(),
        index_checkpoint_delta_json_opt: Some(index_checkpoint_delta_json),
        publish_token_opt: Some("test-publish-token-bar".to_string()),
    };
    metastore
        .publish_splits(publish_splits_request)
        .await
        .unwrap();

    let shards = metastore
        .list_all_shards(&test_index.index_uid, &test_index.source_id)
        .await;
    assert_eq!(shards.len(), 1);
    assert_eq!(shards[0].shard_state(), ShardState::Closed);
    assert_eq!(shards[0].publish_position_inclusive(), Position::eof(1u64));
    cleanup_index(&mut metastore, test_index.index_uid).await;
}

pub async fn test_metastore_apply_checkpoint_delta_v2_multi_shards<
    MetastoreUnderTest: MetastoreService + MetastoreServiceExt + DefaultForTest + ReadWriteShardsForTest,
>() {
    let mut metastore = MetastoreUnderTest::default_for_test().await;

    let test_index = TestIndex::create_index_with_source(
        &mut metastore,
        "test-delete-shards",
        SourceConfig::ingest_v2(),
    )
    .await;

    let dummy_create_timestamp = 1;
    let shards = vec![
        Shard {
            index_uid: Some(test_index.index_uid.clone()),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(0)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(DocMappingUid::default()),
            publish_position_inclusive: Some(Position::offset(0u64)),
            publish_token: Some("test-publish-token-foo".to_string()),
            update_timestamp: dummy_create_timestamp,
            ..Default::default()
        },
        Shard {
            index_uid: Some(test_index.index_uid.clone()),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(DocMappingUid::default()),
            publish_position_inclusive: Some(Position::offset(1u64)),
            publish_token: Some("test-publish-token-foo".to_string()),
            update_timestamp: dummy_create_timestamp,
            ..Default::default()
        },
        Shard {
            index_uid: Some(test_index.index_uid.clone()),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(2)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(DocMappingUid::default()),
            publish_position_inclusive: Some(Position::offset(2u64)),
            publish_token: Some("test-publish-token-foo".to_string()),
            update_timestamp: dummy_create_timestamp,
            ..Default::default()
        },
        Shard {
            index_uid: Some(test_index.index_uid.clone()),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(3)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(DocMappingUid::default()),
            publish_position_inclusive: Some(Position::offset(3u64)),
            publish_token: Some("test-publish-token-bar".to_string()),
            update_timestamp: dummy_create_timestamp,
            ..Default::default()
        },
    ];
    metastore
        .insert_shards(&test_index.index_uid, &test_index.source_id, shards)
        .await;

    let mut source_delta = SourceCheckpointDelta::default();
    source_delta
        .record_partition_delta(
            PartitionId::from(0u64),
            Position::offset(0u64),
            Position::offset(10u64),
        )
        .unwrap();
    source_delta
        .record_partition_delta(
            PartitionId::from(1u64),
            Position::offset(1u64),
            Position::offset(11u64),
        )
        .unwrap();
    source_delta
        .record_partition_delta(
            PartitionId::from(2u64),
            Position::offset(2u64),
            Position::eof(12u64),
        )
        .unwrap();
    let index_checkpoint_delta = IndexCheckpointDelta {
        source_id: test_index.source_id.clone(),
        source_delta,
    };
    let index_checkpoint_delta_json = serde_json::to_string(&index_checkpoint_delta).unwrap();
    let publish_splits_request = PublishSplitsRequest {
        index_uid: Some(test_index.index_uid.clone()),
        staged_split_ids: Vec::new(),
        replaced_split_ids: Vec::new(),
        index_checkpoint_delta_json_opt: Some(index_checkpoint_delta_json),
        publish_token_opt: Some("test-publish-token-foo".to_string()),
    };
    metastore
        .publish_splits(publish_splits_request)
        .await
        .unwrap();

    let mut shards = metastore
        .list_all_shards(&test_index.index_uid, &test_index.source_id)
        .await;
    assert_eq!(shards.len(), 4);

    shards.sort_unstable_by(|left, right| left.shard_id.cmp(&right.shard_id));

    let shard = &shards[0];
    assert_eq!(shard.shard_id(), ShardId::from(0));
    assert_eq!(shard.shard_state(), ShardState::Open);
    assert_eq!(shard.publish_position_inclusive(), Position::offset(10u64));
    assert!(shard.update_timestamp > dummy_create_timestamp);

    let shard = &shards[1];
    assert_eq!(shard.shard_id(), ShardId::from(1));
    assert_eq!(shard.shard_state(), ShardState::Open);
    assert_eq!(shard.publish_position_inclusive(), Position::offset(11u64));
    assert!(shard.update_timestamp > dummy_create_timestamp);

    let shard = &shards[2];
    assert_eq!(shard.shard_id(), ShardId::from(2));
    assert_eq!(shard.shard_state(), ShardState::Closed);
    assert_eq!(shard.publish_position_inclusive(), Position::eof(12u64));
    assert!(shard.update_timestamp > dummy_create_timestamp);

    let shard = &shards[3];
    assert_eq!(shard.shard_id(), ShardId::from(3));
    assert_eq!(shard.shard_state(), ShardState::Open);
    assert_eq!(shard.publish_position_inclusive(), Position::offset(3u64));
    assert_eq!(shard.update_timestamp, dummy_create_timestamp);

    cleanup_index(&mut metastore, test_index.index_uid).await;
}
