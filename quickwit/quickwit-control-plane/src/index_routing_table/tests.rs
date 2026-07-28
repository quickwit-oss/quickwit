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

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use quickwit_actors::{Mailbox, Universe};
use quickwit_cluster::ClusterKvPublisher;
use quickwit_config::{ClusterConfig, IndexConfig};
use quickwit_ingest::IngesterPool;
use quickwit_metastore::{CreateIndexRequestExt, FileBackedMetastore, IndexMetadataResponseExt};
use quickwit_proto::control_plane::ControlPlaneError;
use quickwit_proto::metastore::{
    CreateIndexRequest, DeleteIndexRequest, EmptyResponse, GetIndexRoutingTableRequest,
    IndexRoutingRule, MetastoreError, MetastoreService, MetastoreServiceClient,
    SetIndexRoutingTableRequest,
};
use quickwit_proto::types::{IndexUid, NodeId};
use quickwit_storage::RamStorage;

use crate::IndexerPool;
use crate::control_plane::ControlPlane;

/// A mock [`ClusterKvPublisher`] that counts calls to `set_self_key_value`.
#[derive(Clone, Default)]
struct MockClusterKvPublisher {
    broadcast_count: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl ClusterKvPublisher for MockClusterKvPublisher {
    async fn set_self_key_value(&self, _key: String, _value: String) {
        self.broadcast_count.fetch_add(1, Ordering::SeqCst);
    }
}

/// Sets up a test environment with indexes and an optional initial routing table.
/// Returns (universe, metastore, control_plane_mailbox, broadcast_count).
async fn setup_test(
    index_ids: &[&str],
    initial_routing_rules: Option<Vec<IndexRoutingRule>>,
) -> (
    Universe,
    FileBackedMetastore,
    Mailbox<ControlPlane>,
    Arc<AtomicUsize>,
) {
    let mock_cluster = MockClusterKvPublisher::default();
    let broadcast_count = mock_cluster.broadcast_count.clone();

    // Create metastore with indexes
    let storage = Arc::new(RamStorage::default());
    let metastore = FileBackedMetastore::try_new(storage, None).await.unwrap();
    for index_id in index_ids {
        let index_config = IndexConfig::for_test(index_id, &format!("ram:///{index_id}"));
        let create_request =
            CreateIndexRequest::try_from_index_and_source_configs(&index_config, &[]).unwrap();
        metastore.create_index(create_request).await.unwrap();
    }

    // Set initial routing table if provided
    if let Some(rules) = initial_routing_rules {
        metastore
            .set_index_routing_table(SetIndexRoutingTableRequest { rules })
            .await
            .unwrap();
    }

    // Spawn control plane
    let cluster_config = ClusterConfig::for_test();

    let universe = Universe::with_accelerated_time();
    let (control_plane_mailbox, _handle, mut readiness_rx) = ControlPlane::spawn(
        &universe,
        cluster_config,
        NodeId::from_str("test-node"),
        Arc::new(mock_cluster),
        IndexerPool::default(),
        IngesterPool::default(),
        MetastoreServiceClient::new(metastore.clone()),
    );

    tokio::time::timeout(
        Duration::from_secs(5),
        readiness_rx.wait_for(|readiness| *readiness),
    )
    .await
    .unwrap()
    .unwrap();

    (universe, metastore, control_plane_mailbox, broadcast_count)
}

#[tokio::test]
async fn test_set_routing_table_rejects_invalid_filter_syntax() {
    let (universe, _, control_plane, _) = setup_test(&["test-index-a", "test-index-b"], None).await;

    let request = SetIndexRoutingTableRequest {
        rules: vec![
            IndexRoutingRule {
                filter: "service:a AND (INVALID".to_string(), // Missing closing parenthesis
                index_id: "test-index-a".to_string(),
            },
            IndexRoutingRule {
                filter: "*".to_string(),
                index_id: "test-index-b".to_string(),
            },
        ],
    };

    let error: ControlPlaneError = control_plane.ask(request).await.unwrap().unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::Metastore(MetastoreError::InvalidArgument { message })
        if message.contains("Parse error")
    ));

    universe.assert_quit().await;
}

#[tokio::test]
async fn test_set_routing_table_rejects_non_existent_index() {
    let (universe, _, control_plane, _) =
        setup_test(&["test-index-a", "test-index-b", "test-index-c"], None).await;

    let request = SetIndexRoutingTableRequest {
        rules: vec![
            IndexRoutingRule {
                filter: "service:a".to_string(),
                index_id: "test-index-a".to_string(),
            },
            IndexRoutingRule {
                filter: "*".to_string(),
                index_id: "non-existent-index".to_string(),
            },
        ],
    };

    let error: ControlPlaneError = control_plane.ask(request).await.unwrap().unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::Metastore(MetastoreError::InvalidArgument { message })
        if message.contains("non-existent index") && message.contains("non-existent-index")
    ));

    universe.assert_quit().await;
}

#[tokio::test]
async fn test_set_routing_table_accepts_valid_table() {
    let (universe, metastore, control_plane, _) =
        setup_test(&["test-index-a", "test-index-b", "test-index-c"], None).await;

    let request = SetIndexRoutingTableRequest {
        rules: vec![
            IndexRoutingRule {
                filter: "service:a".to_string(),
                index_id: "test-index-a".to_string(),
            },
            IndexRoutingRule {
                filter: "*".to_string(),
                index_id: "test-index-b".to_string(),
            },
        ],
    };

    let result: EmptyResponse = control_plane.ask(request.clone()).await.unwrap().unwrap();
    assert_eq!(result, EmptyResponse {});

    // Verify the routing table was persisted
    let response = metastore
        .get_index_routing_table(GetIndexRoutingTableRequest {})
        .await
        .unwrap();
    assert_eq!(
        response.rules, request.rules,
        "routing table should match what was set"
    );

    universe.assert_quit().await;
}

#[tokio::test]
async fn test_delete_index_removes_rules() {
    let (universe, metastore, control_plane, _) = setup_test(
        &["index-a", "index-b"],
        Some(vec![
            IndexRoutingRule {
                filter: "service:a".to_string(),
                index_id: "index-a".to_string(),
            },
            IndexRoutingRule {
                filter: "*".to_string(),
                index_id: "index-b".to_string(),
            },
        ]),
    )
    .await;

    let index_a_uid: IndexUid = metastore
        .index_metadata(quickwit_proto::metastore::IndexMetadataRequest {
            index_id: Some("index-a".to_string()),
            index_uid: None,
        })
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap()
        .index_uid;

    let response: EmptyResponse = control_plane
        .ask(DeleteIndexRequest {
            index_uid: Some(index_a_uid),
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response, EmptyResponse {});

    // Verify only index-b remains
    let routing_response = metastore
        .get_index_routing_table(GetIndexRoutingTableRequest {})
        .await
        .unwrap();
    let expected_rules = vec![IndexRoutingRule {
        filter: "*".to_string(),
        index_id: "index-b".to_string(),
    }];
    assert_eq!(
        routing_response.rules, expected_rules,
        "only index-b should remain in routing table"
    );

    universe.assert_quit().await;
}

/// Tests that control plane doesn't overwrite existing routing table at startup.
#[tokio::test]
async fn test_control_plane_preserves_existing_routing_table() {
    // Create indexes with an existing routing table
    let initial_rules = vec![
        IndexRoutingRule {
            filter: "tag:prod".to_string(),
            index_id: "index-b".to_string(),
        },
        IndexRoutingRule {
            filter: "*".to_string(),
            index_id: "index-c".to_string(),
        },
    ];
    let (universe, metastore, _control_plane, _broadcast_count) = setup_test(
        &["index-a", "index-b", "index-c"],
        Some(initial_rules.clone()),
    )
    .await;

    // Give control plane time to initialize
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify routing table was NOT modified
    let response = metastore
        .get_index_routing_table(GetIndexRoutingTableRequest {})
        .await
        .unwrap();

    assert_eq!(
        response.rules, initial_rules,
        "routing table should not be modified at startup"
    );

    universe.assert_quit().await;
}

/// Test that when SetIndexRoutingTableRequest is handled, the control plane broadcasts
/// the change via ClusterKvPublisher.
#[tokio::test]
async fn test_set_routing_table_broadcasts_change() {
    let (universe, _, control_plane, broadcast_count) =
        setup_test(&["test-index-a", "test-index-b"], None).await;

    // Verify no broadcast during startup
    assert_eq!(
        broadcast_count.load(Ordering::SeqCst),
        0,
        "expected no broadcast during startup"
    );

    // Set a routing table
    let request = SetIndexRoutingTableRequest {
        rules: vec![
            IndexRoutingRule {
                filter: "service:a".to_string(),
                index_id: "test-index-a".to_string(),
            },
            IndexRoutingRule {
                filter: "*".to_string(),
                index_id: "test-index-b".to_string(),
            },
        ],
    };

    let result: EmptyResponse = control_plane.ask(request).await.unwrap().unwrap();
    assert_eq!(result, EmptyResponse {});

    // Verify the broadcast was called exactly once
    assert_eq!(
        broadcast_count.load(Ordering::SeqCst),
        1,
        "expected exactly one broadcast call after set_routing_table"
    );

    universe.assert_quit().await;
}

/// Test that when an index is deleted, the control plane broadcasts the routing table change.
#[tokio::test]
async fn test_delete_index_broadcasts_change() {
    let (universe, metastore, control_plane, broadcast_count) = setup_test(
        &["index-a", "index-b"],
        Some(vec![
            IndexRoutingRule {
                filter: "service:a".to_string(),
                index_id: "index-a".to_string(),
            },
            IndexRoutingRule {
                filter: "*".to_string(),
                index_id: "index-b".to_string(),
            },
        ]),
    )
    .await;

    // Verify no broadcast during startup
    assert_eq!(
        broadcast_count.load(Ordering::SeqCst),
        0,
        "expected no broadcast during startup"
    );

    // Get index-a UID
    let index_a_uid: IndexUid = metastore
        .index_metadata(quickwit_proto::metastore::IndexMetadataRequest {
            index_id: Some("index-a".to_string()),
            index_uid: None,
        })
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap()
        .index_uid;

    // Delete index-a
    let response: EmptyResponse = control_plane
        .ask(DeleteIndexRequest {
            index_uid: Some(index_a_uid),
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response, EmptyResponse {});

    // Verify the broadcast was called exactly once for the index deletion
    assert_eq!(
        broadcast_count.load(Ordering::SeqCst),
        1,
        "expected exactly one broadcast call after delete_index"
    );

    universe.assert_quit().await;
}
