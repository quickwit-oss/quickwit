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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chitchat::transport::ChannelTransport;
use hyper::{Body, Method, Request, StatusCode};
use quickwit_cluster::create_cluster_for_test;
use quickwit_common::uri::Uri;
use quickwit_config::service::QuickwitService;
use quickwit_metastore::{IndexMetadata, MockMetastore};
use quickwit_proto::SearchRequest;

use crate::test_utils::ClusterSandbox;
use crate::{check_cluster_configuration, node_readiness_reporting_task};

#[tokio::test]
async fn test_check_cluster_configuration() {
    let services = HashSet::from_iter([QuickwitService::Metastore]);
    let peer_seeds = ["192.168.0.12:7280".to_string()];
    let mut metastore = MockMetastore::new();

    metastore
        .expect_uri()
        .return_const(Uri::for_test("file:///qwdata/indexes"));

    metastore.expect_list_indexes_metadatas().return_once(|| {
        Ok(vec![IndexMetadata::for_test(
            "test-index",
            "file:///qwdata/indexes/test-index",
        )])
    });

    check_cluster_configuration(&services, &peer_seeds, Arc::new(metastore))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_standalone_server() {
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandbox::start_standalone_node().await.unwrap();
    sandbox
        .indexer_rest_client
        .cluster_snapshot()
        .await
        .unwrap();
    assert!(sandbox.indexer_rest_client.is_ready().await.unwrap());

    {
        let client = sandbox.indexer_rest_client.client();
        let root_uri = format!("{}/", sandbox.indexer_rest_client.root_url())
            .parse::<hyper::Uri>()
            .unwrap();
        let response = client.get(root_uri.clone()).await.unwrap();
        assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY);
        let post_request = Request::builder()
            .uri(root_uri)
            .method(Method::POST)
            .body(Body::from("{}"))
            .unwrap();
        let response = client.request(post_request).await.unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }
    {
        // The indexing service should bd running.
        let counters = sandbox
            .indexer_rest_client
            .indexing_service_counters()
            .await
            .unwrap();
        assert_eq!(counters.num_running_pipelines, 0);
    }
    {
        // Create an dynamic index.
        sandbox
            .indexer_rest_client
            .create_index(
                r#"
                version: 0.5
                index_id: my-new-index
                doc_mapping:
                  field_mappings:
                  - name: body
                    type: text
                "#,
            )
            .await
            .unwrap();

        // Index should be searchable
        let mut search_client = sandbox.get_random_search_client();
        search_client
            .root_search(SearchRequest {
                index_id: "my-new-index".to_string(),
                query: "body:test".to_string(),
                search_fields: Vec::new(),
                snippet_fields: Vec::new(),
                start_timestamp: None,
                end_timestamp: None,
                aggregation_request: None,
                max_hits: 10,
                sort_by_field: None,
                sort_order: None,
                start_offset: 0,
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let counters = sandbox
            .indexer_rest_client
            .indexing_service_counters()
            .await
            .unwrap();
        assert_eq!(counters.num_running_pipelines, 1);
    }
}

#[tokio::test]
async fn test_multi_nodes_cluster() {
    quickwit_common::setup_logging_for_tests();
    let nodes_services = vec![
        HashSet::from_iter([QuickwitService::Searcher]),
        HashSet::from_iter([QuickwitService::Metastore]),
        HashSet::from_iter([QuickwitService::Indexer]),
        HashSet::from_iter([QuickwitService::ControlPlane]),
        HashSet::from_iter([QuickwitService::Janitor]),
    ];
    let sandbox = ClusterSandbox::start_cluster_nodes(&nodes_services)
        .await
        .unwrap();
    sandbox.wait_for_cluster_num_ready_nodes(4).await.unwrap();

    {
        // Wait for indexer to fully start.
        // The starting time is a bit long for a cluster.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let indexing_service_counters = sandbox
            .indexer_rest_client
            .indexing_service_counters()
            .await
            .unwrap();
        assert_eq!(indexing_service_counters.num_running_pipelines, 0);
    }

    // Create index
    sandbox
        .indexer_rest_client
        .create_index(
            r#"
            version: 0.5
            index_id: my-new-multi-node-index
            doc_mapping:
              field_mappings:
              - name: body
                type: text
            indexing_settings:
              commit_timeout_secs: 1
            "#,
        )
        .await
        .unwrap();
    assert!(sandbox.indexer_rest_client.is_live().await.unwrap());

    // Wait until indexing pipelines are started.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let indexing_service_counters = sandbox
        .indexer_rest_client
        .indexing_service_counters()
        .await
        .unwrap();
    assert_eq!(indexing_service_counters.num_running_pipelines, 1);

    // Check search is working.
    let mut search_client = sandbox.get_random_search_client();
    let search_request = SearchRequest {
        index_id: "my-new-multi-node-index".to_string(),
        query: "body:test".to_string(),
        search_fields: Vec::new(),
        start_timestamp: None,
        end_timestamp: None,
        aggregation_request: None,
        max_hits: 10,
        sort_by_field: None,
        sort_order: None,
        start_offset: 0,
        snippet_fields: Vec::new(),
    };
    let search_response_empty = search_client
        .root_search(search_request.clone())
        .await
        .unwrap();
    assert_eq!(search_response_empty.num_hits, 0);

    // Check that ingest request send to searcher is forwarded to indexer and thus indexed.
    sandbox
        .searcher_rest_client
        .ingest_data("my-new-multi-node-index", "{\"body\": \"test\"}")
        .await
        .unwrap();
    // Wait until split is commited and search.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let mut search_client = sandbox.get_random_search_client();
    let search_response_one_hit = search_client
        .root_search(search_request.clone())
        .await
        .unwrap();
    assert_eq!(search_response_one_hit.num_hits, 1);
}

#[tokio::test]
async fn test_readiness_updates() -> anyhow::Result<()> {
    let transport = ChannelTransport::default();
    let cluster = Arc::new(
        create_cluster_for_test(Vec::new(), &[], &transport, false)
            .await
            .unwrap(),
    );
    cluster.set_self_node_ready(true).await;
    let mut counter = 2;
    let mut metastore = MockMetastore::new();
    metastore
        .expect_check_connectivity()
        .times(4)
        .returning(move || {
            counter -= 1;
            if counter == 1 {
                anyhow::bail!("error")
            }
            Ok(())
        });
    tokio::spawn(node_readiness_reporting_task(
        cluster.clone(),
        Arc::new(metastore),
    ));
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(!cluster.is_self_node_ready().await);
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(cluster.is_self_node_ready().await);
    Ok(())
}
