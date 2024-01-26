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
use quickwit_common::rand::append_random_suffix;
use quickwit_config::{IndexConfig, SourceConfig};
use quickwit_proto::ingest::{Shard, ShardState};
use quickwit_proto::metastore::{
    AcquireShardsRequest, AcquireShardsSubrequest, AddSourceRequest, CreateIndexRequest,
    DeleteShardsRequest, DeleteShardsSubrequest, EntityKind, ListShardsRequest,
    ListShardsSubrequest, MetastoreError, MetastoreService, OpenShardsRequest,
    OpenShardsSubrequest, PublishSplitsRequest,
};
use quickwit_proto::types::{IndexUid, Position, ShardId, SourceId};

use super::DefaultForTest;
use crate::checkpoint::{IndexCheckpointDelta, PartitionId, SourceCheckpointDelta};
use crate::tests::cleanup_index;
use crate::{AddSourceRequestExt, CreateIndexRequestExt, MetastoreServiceExt};

#[async_trait]
pub trait ReadWriteShardsForTest {
    async fn insert_shards(
        &mut self,
        index_uid: &IndexUid,
        source_id: &SourceId,
        shards: Vec<Shard>,
    );

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
            CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
        let create_index_response = metastore.create_index(create_index_request).await.unwrap();
        let index_uid: IndexUid = create_index_response.index_uid.into();

        let add_source_request =
            AddSourceRequest::try_from_source_config(index_uid.clone(), source_config.clone())
                .unwrap();
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
        SourceConfig::ingest_v2_default(),
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
    //     subrequests: vec![OpenShardsSubrequest {
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
    //     subrequests: vec![OpenShardsSubrequest {
    //         index_uid: test_index.index_uid.clone().into(),
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
        subrequests: vec![OpenShardsSubrequest {
            subrequest_id: 0,
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            leader_id: "test-ingester-foo".to_string(),
            follower_id: Some("test-ingester-bar".to_string()),
        }],
    };
    let open_shards_response = metastore.open_shards(open_shards_request).await.unwrap();
    assert_eq!(open_shards_response.subresponses.len(), 1);

    let subresponse = &open_shards_response.subresponses[0];
    assert_eq!(subresponse.index_uid, test_index.index_uid);
    assert_eq!(subresponse.source_id, test_index.source_id);
    assert_eq!(subresponse.opened_shards.len(), 1);

    let shard = &subresponse.opened_shards[0];
    assert_eq!(shard.index_uid, test_index.index_uid);
    assert_eq!(shard.source_id, test_index.source_id);
    assert_eq!(shard.shard_id(), ShardId::from(1));
    assert_eq!(shard.shard_state(), ShardState::Open);
    assert_eq!(shard.leader_id, "test-ingester-foo");
    assert_eq!(shard.follower_id(), "test-ingester-bar");
    assert_eq!(shard.publish_position_inclusive(), Position::Beginning);
    assert!(shard.publish_token.is_none());

    // Test open shard #1 is idempotent.
    let open_shards_request = OpenShardsRequest {
        subrequests: vec![OpenShardsSubrequest {
            subrequest_id: 0,
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            leader_id: "test-ingester-foo".to_string(),
            follower_id: Some("test-ingester-bar".to_string()),
        }],
    };
    let open_shards_response = metastore.open_shards(open_shards_request).await.unwrap();
    assert_eq!(open_shards_response.subresponses.len(), 1);

    let subresponse = &open_shards_response.subresponses[0];
    assert_eq!(subresponse.index_uid, test_index.index_uid);
    assert_eq!(subresponse.source_id, test_index.source_id);
    assert_eq!(subresponse.opened_shards.len(), 1);

    let shard = &subresponse.opened_shards[0];
    assert_eq!(shard.index_uid, test_index.index_uid);
    assert_eq!(shard.source_id, test_index.source_id);
    assert_eq!(shard.shard_id(), ShardId::from(1));
    assert_eq!(shard.shard_state(), ShardState::Open);
    assert_eq!(shard.leader_id, "test-ingester-foo");
    assert_eq!(shard.follower_id(), "test-ingester-bar");
    assert_eq!(shard.publish_position_inclusive(), Position::Beginning);
    assert!(shard.publish_token.is_none());

    cleanup_index(&mut metastore, test_index.index_uid).await;
}

pub async fn test_metastore_acquire_shards<
    MetastoreUnderTest: MetastoreService + MetastoreServiceExt + DefaultForTest + ReadWriteShardsForTest,
>() {
    let mut metastore = MetastoreUnderTest::default_for_test().await;

    let test_index = TestIndex::create_index_with_source(
        &mut metastore,
        "test-acquire-shards",
        SourceConfig::ingest_v2_default(),
    )
    .await;

    let shards = vec![
        Shard {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Closed as i32,
            leader_id: "test-ingester-foo".to_string(),
            follower_id: Some("test-ingester-bar".to_string()),
            publish_position_inclusive: Some(Position::Beginning),
            publish_token: Some("test-publish-token-foo".to_string()),
        },
        Shard {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(2)),
            shard_state: ShardState::Open as i32,
            leader_id: "test-ingester-bar".to_string(),
            follower_id: Some("test-ingester-qux".to_string()),
            publish_position_inclusive: Some(Position::Beginning),
            publish_token: Some("test-publish-token-bar".to_string()),
        },
        Shard {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(3)),
            shard_state: ShardState::Open as i32,
            leader_id: "test-ingester-qux".to_string(),
            follower_id: Some("test-ingester-baz".to_string()),
            publish_position_inclusive: Some(Position::Beginning),
            publish_token: None,
        },
        Shard {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(4)),
            shard_state: ShardState::Open as i32,
            leader_id: "test-ingester-baz".to_string(),
            follower_id: Some("test-ingester-tux".to_string()),
            publish_position_inclusive: Some(Position::Beginning),
            publish_token: None,
        },
    ];
    metastore
        .insert_shards(&test_index.index_uid, &test_index.source_id, shards)
        .await;

    // Test acquire shards.
    let acquires_shards_request = AcquireShardsRequest {
        subrequests: vec![AcquireShardsSubrequest {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_id.clone(),
            shard_ids: vec![ShardId::from(1), ShardId::from(2), ShardId::from(3)],
            publish_token: "test-publish-token-foo".to_string(),
        }],
    };
    let acquire_shards_response = metastore
        .acquire_shards(acquires_shards_request)
        .await
        .unwrap();
    assert_eq!(acquire_shards_response.subresponses.len(), 1);

    let mut subresponses = acquire_shards_response.subresponses;
    assert_eq!(subresponses[0].index_uid, test_index.index_uid);
    assert_eq!(subresponses[0].source_id, test_index.source_id);
    assert_eq!(subresponses[0].acquired_shards.len(), 3);

    subresponses[0]
        .acquired_shards
        .sort_unstable_by(|left, right| left.shard_id.cmp(&right.shard_id));

    let shard = &subresponses[0].acquired_shards[0];
    assert_eq!(shard.index_uid, test_index.index_uid);
    assert_eq!(shard.source_id, test_index.source_id);
    assert_eq!(shard.shard_id(), ShardId::from(1));
    assert_eq!(shard.shard_state(), ShardState::Closed);
    assert_eq!(shard.leader_id, "test-ingester-foo");
    assert_eq!(shard.follower_id(), "test-ingester-bar");
    assert_eq!(shard.publish_position_inclusive(), Position::Beginning);
    assert_eq!(shard.publish_token(), "test-publish-token-foo");

    let shard = &subresponses[0].acquired_shards[1];
    assert_eq!(shard.index_uid, test_index.index_uid);
    assert_eq!(shard.source_id, test_index.source_id);
    assert_eq!(shard.shard_id(), ShardId::from(2));
    assert_eq!(shard.shard_state(), ShardState::Open);
    assert_eq!(shard.leader_id, "test-ingester-bar");
    assert_eq!(shard.follower_id(), "test-ingester-qux");
    assert_eq!(shard.publish_position_inclusive(), Position::Beginning);
    assert_eq!(shard.publish_token(), "test-publish-token-foo");

    let shard = &subresponses[0].acquired_shards[2];
    assert_eq!(shard.index_uid, test_index.index_uid);
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

    let test_index = TestIndex::create_index_with_source(
        &mut metastore,
        "test-list-shards",
        SourceConfig::ingest_v2_default(),
    )
    .await;

    let shards = vec![
        Shard {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            leader_id: "test-ingester-foo".to_string(),
            follower_id: Some("test-ingester-bar".to_string()),
            publish_position_inclusive: Some(Position::Beginning),
            publish_token: Some("test-publish-token-foo".to_string()),
        },
        Shard {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(2)),
            shard_state: ShardState::Closed as i32,
            leader_id: "test-ingester-bar".to_string(),
            follower_id: Some("test-ingester-qux".to_string()),
            publish_position_inclusive: Some(Position::Beginning),
            publish_token: Some("test-publish-token-bar".to_string()),
        },
    ];
    metastore
        .insert_shards(&test_index.index_uid, &test_index.source_id, shards)
        .await;

    // Test list shards.
    let list_shards_request = ListShardsRequest {
        subrequests: vec![ListShardsSubrequest {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_id.clone(),
            shard_state: None,
        }],
    };
    let list_shards_response = metastore.list_shards(list_shards_request).await.unwrap();
    assert_eq!(list_shards_response.subresponses.len(), 1);

    let mut subresponses = list_shards_response.subresponses;
    assert_eq!(subresponses[0].index_uid, test_index.index_uid);
    assert_eq!(subresponses[0].source_id, test_index.source_id);
    assert_eq!(subresponses[0].shards.len(), 2);

    subresponses[0]
        .shards
        .sort_unstable_by(|left, right| left.shard_id.cmp(&right.shard_id));

    let shard = &subresponses[0].shards[0];
    assert_eq!(shard.index_uid, test_index.index_uid);
    assert_eq!(shard.source_id, test_index.source_id);
    assert_eq!(shard.shard_id(), ShardId::from(1));
    assert_eq!(shard.shard_state(), ShardState::Open);
    assert_eq!(shard.leader_id, "test-ingester-foo");
    assert_eq!(shard.follower_id(), "test-ingester-bar");
    assert_eq!(shard.publish_position_inclusive(), Position::Beginning);
    assert_eq!(shard.publish_token(), "test-publish-token-foo");

    let shard = &subresponses[0].shards[1];
    assert_eq!(shard.index_uid, test_index.index_uid);
    assert_eq!(shard.source_id, test_index.source_id);
    assert_eq!(shard.shard_id(), ShardId::from(2));
    assert_eq!(shard.shard_state(), ShardState::Closed);
    assert_eq!(shard.leader_id, "test-ingester-bar");
    assert_eq!(shard.follower_id(), "test-ingester-qux");
    assert_eq!(shard.publish_position_inclusive(), Position::Beginning);
    assert_eq!(shard.publish_token(), "test-publish-token-bar");

    // Test list shards with shard state filter.
    let list_shards_request = ListShardsRequest {
        subrequests: vec![ListShardsSubrequest {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_id.clone(),
            shard_state: Some(ShardState::Open as i32),
        }],
    };
    let list_shards_response = metastore.list_shards(list_shards_request).await.unwrap();
    assert_eq!(list_shards_response.subresponses.len(), 1);
    assert_eq!(list_shards_response.subresponses[0].shards.len(), 1);

    let shard = &list_shards_response.subresponses[0].shards[0];
    assert_eq!(shard.shard_id(), ShardId::from(1));
    assert_eq!(shard.shard_state(), ShardState::Open);

    let list_shards_request = ListShardsRequest {
        subrequests: vec![ListShardsSubrequest {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_id.clone(),
            shard_state: Some(ShardState::Unavailable as i32),
        }],
    };
    let list_shards_response = metastore.list_shards(list_shards_request).await.unwrap();
    assert_eq!(list_shards_response.subresponses.len(), 1);

    cleanup_index(&mut metastore, test_index.index_uid).await;
}

pub async fn test_metastore_delete_shards<
    MetastoreUnderTest: MetastoreService + MetastoreServiceExt + DefaultForTest + ReadWriteShardsForTest,
>() {
    let mut metastore = MetastoreUnderTest::default_for_test().await;

    let test_index = TestIndex::create_index_with_source(
        &mut metastore,
        "test-delete-shards",
        SourceConfig::ingest_v2_default(),
    )
    .await;

    let shards = vec![
        Shard {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            publish_position_inclusive: Some(Position::Beginning),
            ..Default::default()
        },
        Shard {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(2)),
            shard_state: ShardState::Closed as i32,
            publish_position_inclusive: Some(Position::Beginning),
            ..Default::default()
        },
        Shard {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(3)),
            shard_state: ShardState::Closed as i32,
            publish_position_inclusive: Some(Position::Eof(None)),
            ..Default::default()
        },
    ];
    metastore
        .insert_shards(&test_index.index_uid, &test_index.source_id, shards)
        .await;

    // Attempt to delete shards #1, #2, #3, and #4.
    let delete_index_request = DeleteShardsRequest {
        subrequests: vec![DeleteShardsSubrequest {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_id.clone(),
            shard_ids: vec![
                ShardId::from(1),
                ShardId::from(2),
                ShardId::from(3),
                ShardId::from(4),
            ],
        }],
        force: false,
    };
    metastore.delete_shards(delete_index_request).await.unwrap();

    let mut all_shards = metastore
        .list_all_shards(&test_index.index_uid, &test_index.source_id)
        .await;
    assert_eq!(all_shards.len(), 2);

    all_shards.sort_unstable_by(|left, right| left.shard_id.cmp(&right.shard_id));

    assert_eq!(all_shards[0].shard_id(), ShardId::from(1));
    assert_eq!(all_shards[1].shard_id(), ShardId::from(2));

    // Attempt to delete shards #1, #2, #3, and #4.
    let delete_index_request = DeleteShardsRequest {
        subrequests: vec![DeleteShardsSubrequest {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_id.clone(),
            shard_ids: vec![
                ShardId::from(1),
                ShardId::from(2),
                ShardId::from(3),
                ShardId::from(4),
            ],
        }],
        force: true,
    };
    metastore.delete_shards(delete_index_request).await.unwrap();

    let all_shards = metastore
        .list_all_shards(&test_index.index_uid, &test_index.source_id)
        .await;
    assert_eq!(all_shards.len(), 0);

    cleanup_index(&mut metastore, test_index.index_uid).await;
}

pub async fn test_metastore_apply_checkpoint_delta_v2_single_shard<
    MetastoreUnderTest: MetastoreService + MetastoreServiceExt + DefaultForTest + ReadWriteShardsForTest,
>() {
    let mut metastore = MetastoreUnderTest::default_for_test().await;

    let test_index = TestIndex::create_index_with_source(
        &mut metastore,
        "test-delete-shards",
        SourceConfig::ingest_v2_default(),
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
        index_uid: test_index.index_uid.clone().into(),
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

    let shards = vec![Shard {
        index_uid: test_index.index_uid.clone().into(),
        source_id: test_index.source_id.clone(),
        shard_id: Some(ShardId::from(0)),
        shard_state: ShardState::Open as i32,
        publish_position_inclusive: Some(Position::Beginning),
        publish_token: Some("test-publish-token-bar".to_string()),
        ..Default::default()
    }];
    metastore
        .insert_shards(&test_index.index_uid, &test_index.source_id, shards)
        .await;

    let index_checkpoint_delta_json = serde_json::to_string(&index_checkpoint_delta).unwrap();
    let publish_splits_request = PublishSplitsRequest {
        index_uid: test_index.index_uid.clone().into(),
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
        index_uid: test_index.index_uid.clone().into(),
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

    let index_checkpoint_delta_json = serde_json::to_string(&index_checkpoint_delta).unwrap();
    let publish_splits_request = PublishSplitsRequest {
        index_uid: test_index.index_uid.clone().into(),
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
        index_uid: test_index.index_uid.clone().into(),
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
        SourceConfig::ingest_v2_default(),
    )
    .await;

    let shards = vec![
        Shard {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(0)),
            shard_state: ShardState::Open as i32,
            publish_position_inclusive: Some(Position::offset(0u64)),
            publish_token: Some("test-publish-token-foo".to_string()),
            ..Default::default()
        },
        Shard {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            publish_position_inclusive: Some(Position::offset(1u64)),
            publish_token: Some("test-publish-token-foo".to_string()),
            ..Default::default()
        },
        Shard {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(2)),
            shard_state: ShardState::Open as i32,
            publish_position_inclusive: Some(Position::offset(2u64)),
            publish_token: Some("test-publish-token-foo".to_string()),
            ..Default::default()
        },
        Shard {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_id.clone(),
            shard_id: Some(ShardId::from(3)),
            shard_state: ShardState::Open as i32,
            publish_position_inclusive: Some(Position::offset(3u64)),
            publish_token: Some("test-publish-token-bar".to_string()),
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
        index_uid: test_index.index_uid.clone().into(),
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

    let shard = &shards[1];
    assert_eq!(shard.shard_id(), ShardId::from(1));
    assert_eq!(shard.shard_state(), ShardState::Open);
    assert_eq!(shard.publish_position_inclusive(), Position::offset(11u64));

    let shard = &shards[2];
    assert_eq!(shard.shard_id(), ShardId::from(2));
    assert_eq!(shard.shard_state(), ShardState::Closed);
    assert_eq!(shard.publish_position_inclusive(), Position::eof(12u64));

    let shard = &shards[3];
    assert_eq!(shard.shard_id(), ShardId::from(3));
    assert_eq!(shard.shard_state(), ShardState::Open);
    assert_eq!(shard.publish_position_inclusive(), Position::offset(3u64));

    cleanup_index(&mut metastore, test_index.index_uid).await;
}
