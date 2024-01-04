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

use quickwit_common::rand::append_random_suffix;
use quickwit_config::{IndexConfig, SourceConfig};
use quickwit_proto::metastore::{
    AddSourceRequest, CreateIndexRequest, EntityKind, MetastoreError, MetastoreService,
    OpenShardsRequest, OpenShardsSubrequest,
};
use quickwit_proto::types::{IndexUid, SourceId};

use super::DefaultForTest;
use crate::tests::cleanup_index;
use crate::{AddSourceRequestExt, CreateIndexRequestExt, FileBackedMetastore, MetastoreServiceExt};

// TODO: Remove when `PostgresqlMetastore` implements Shard API.
pub trait RunTests {
    fn run_open_shards_test() -> bool {
        true
    }

    fn run_other_tests() -> bool {
        true
    }
}

impl RunTests for FileBackedMetastore {}

#[cfg(feature = "postgres")]
impl RunTests for crate::PostgresqlMetastore {
    fn run_other_tests() -> bool {
        false
    }

    fn run_open_shards_test() -> bool {
        false
    }
}

struct TestIndex {
    index_uid: IndexUid,
    _index_config: IndexConfig,
    _source_id: SourceId,
    source_config: SourceConfig,
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
            _index_config: index_config,
            _source_id: source_config.source_id.clone(),
            source_config,
        }
    }
}

pub async fn test_metastore_open_shards<
    MetastoreUnderTest: MetastoreService + MetastoreServiceExt + DefaultForTest + RunTests,
>() {
    if !MetastoreUnderTest::run_open_shards_test() {
        return;
    }
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
    let open_shards_request = OpenShardsRequest {
        subrequests: vec![OpenShardsSubrequest {
            index_uid: "index-does-not-exist:0".to_string(),
            source_id: test_index.source_config.source_id.clone(),
            leader_id: "test-ingester-foo".to_string(),
            next_shard_id: 1,
            ..Default::default()
        }],
    };
    let error = metastore
        .open_shards(open_shards_request)
        .await
        .unwrap_err();
    assert!(
        matches!(error, MetastoreError::NotFound(EntityKind::Index { index_id }) if index_id == "index-does-not-exist")
    );

    // Test source not found.
    let open_shards_request = OpenShardsRequest {
        subrequests: vec![OpenShardsSubrequest {
            index_uid: test_index.index_uid.clone().into(),
            source_id: "source-does-not-exist".to_string(),
            leader_id: "test-ingester-foo".to_string(),
            next_shard_id: 1,
            ..Default::default()
        }],
    };
    let error = metastore
        .open_shards(open_shards_request)
        .await
        .unwrap_err();
    assert!(
        matches!(error, MetastoreError::NotFound(EntityKind::Source { source_id, ..}) if source_id == "source-does-not-exist")
    );

    // Test open shard #1.
    let open_shards_request = OpenShardsRequest {
        subrequests: vec![OpenShardsSubrequest {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_config.source_id.clone(),
            leader_id: "test-ingester-foo".to_string(),
            next_shard_id: 1,
            ..Default::default()
        }],
    };
    let open_shards_response = metastore.open_shards(open_shards_request).await.unwrap();
    assert_eq!(open_shards_response.subresponses.len(), 1);

    let subresponse = &open_shards_response.subresponses[0];
    assert_eq!(subresponse.index_uid, test_index.index_uid.as_str());
    assert_eq!(subresponse.source_id, test_index.source_config.source_id);
    assert_eq!(subresponse.next_shard_id, 2);
    assert_eq!(subresponse.opened_shards.len(), 1);

    let shard = &subresponse.opened_shards[0];
    assert_eq!(shard.index_uid, test_index.index_uid.as_str());
    assert_eq!(shard.source_id, test_index.source_config.source_id);
    assert_eq!(shard.shard_id, 1);
    assert_eq!(shard.leader_id, "test-ingester-foo");

    // Test open shard #1 is idempotent.
    let open_shards_request = OpenShardsRequest {
        subrequests: vec![OpenShardsSubrequest {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_config.source_id.clone(),
            leader_id: "test-ingester-bar".to_string(),
            next_shard_id: 1,
            ..Default::default()
        }],
    };
    let open_shards_response = metastore.open_shards(open_shards_request).await.unwrap();
    assert_eq!(open_shards_response.subresponses.len(), 1);

    let subresponse = &open_shards_response.subresponses[0];
    assert_eq!(subresponse.next_shard_id, 2);
    assert_eq!(subresponse.opened_shards.len(), 1);

    let shard = &subresponse.opened_shards[0];
    assert_eq!(shard.shard_id, 1);
    assert_eq!(shard.leader_id, "test-ingester-foo");

    // Test open shard #2.
    let open_shards_request = OpenShardsRequest {
        subrequests: vec![OpenShardsSubrequest {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_config.source_id.clone(),
            leader_id: "test-ingester-qux".to_string(),
            next_shard_id: 2,
            ..Default::default()
        }],
    };
    let open_shards_response = metastore.open_shards(open_shards_request).await.unwrap();
    assert_eq!(open_shards_response.subresponses.len(), 1);

    let subresponse = &open_shards_response.subresponses[0];
    assert_eq!(subresponse.index_uid, test_index.index_uid.as_str());
    assert_eq!(subresponse.source_id, test_index.source_config.source_id);
    assert_eq!(subresponse.next_shard_id, 3);
    assert_eq!(subresponse.opened_shards.len(), 1);

    let shard = &subresponse.opened_shards[0];
    assert_eq!(shard.index_uid, test_index.index_uid.as_str());
    assert_eq!(shard.source_id, test_index.source_config.source_id);
    assert_eq!(shard.shard_id, 2);
    assert_eq!(shard.leader_id, "test-ingester-qux");

    // Test open shard should not be called with a lagging `next_shard_id`.
    let open_shards_request = OpenShardsRequest {
        subrequests: vec![OpenShardsSubrequest {
            index_uid: test_index.index_uid.clone().into(),
            source_id: test_index.source_config.source_id.clone(),
            leader_id: "test-ingester-foo".to_string(),
            next_shard_id: 1,
            ..Default::default()
        }],
    };
    let error = metastore
        .open_shards(open_shards_request)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MetastoreError::InconsistentControlPlaneState
    ));

    cleanup_index(&mut metastore, test_index.index_uid).await;
}

pub async fn test_metastore_acquire_shards<
    MetastoreUnderTest: MetastoreService + MetastoreServiceExt + DefaultForTest + RunTests,
>() {
    if !MetastoreUnderTest::run_other_tests() {
        return;
    }
    let mut metastore = MetastoreUnderTest::default_for_test().await;

    let test_index = TestIndex::create_index_with_source(
        &mut metastore,
        "test-acquire-shards",
        SourceConfig::ingest_v2_default(),
    )
    .await;

    // TODO

    cleanup_index(&mut metastore, test_index.index_uid).await;
}

pub async fn test_metastore_list_shards<
    MetastoreUnderTest: MetastoreService + MetastoreServiceExt + DefaultForTest + RunTests,
>() {
    if !MetastoreUnderTest::run_other_tests() {
        return;
    }
    let mut metastore = MetastoreUnderTest::default_for_test().await;

    let test_index = TestIndex::create_index_with_source(
        &mut metastore,
        "test-open-shards",
        SourceConfig::ingest_v2_default(),
    )
    .await;

    // TODO

    cleanup_index(&mut metastore, test_index.index_uid).await;
}

pub async fn test_metastore_delete_shards<
    MetastoreUnderTest: MetastoreService + MetastoreServiceExt + DefaultForTest + RunTests,
>() {
    if !MetastoreUnderTest::run_other_tests() {
        return;
    }
    let mut metastore = MetastoreUnderTest::default_for_test().await;

    let test_index = TestIndex::create_index_with_source(
        &mut metastore,
        "test-open-shards",
        SourceConfig::ingest_v2_default(),
    )
    .await;

    // TODO

    cleanup_index(&mut metastore, test_index.index_uid).await;
}
