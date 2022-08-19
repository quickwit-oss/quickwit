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

use quickwit_common::uri::Uri;
use quickwit_proto::SearchRequest;

use crate::check_is_configured_for_cluster;
use crate::test_utils::ClusterSandbox;

#[test]
fn test_check_is_configured_for_cluster_on_single_node() {
    check_is_configured_for_cluster(
        &[],
        &Uri::new("file://qwdata/indexes".to_string()),
        [(
            "foo-index".to_string(),
            Uri::new("file:///qwdata/indexes/foo-index".to_string()),
        )]
        .into_iter(),
    )
    .unwrap();
}

#[tokio::test]
async fn test_standalone_server() -> anyhow::Result<()> {
    let sandbox = ClusterSandbox::start_standalone_node().await.unwrap();
    let mut search_client = sandbox.get_random_search_client();
    let search_result = search_client
        .root_search(SearchRequest {
            index_id: sandbox.index_id_for_test.clone(),
            query: "*".to_string(),
            search_fields: Vec::new(),
            start_timestamp: None,
            end_timestamp: None,
            aggregation_request: None,
            max_hits: 10,
            sort_by_field: None,
            sort_order: None,
            start_offset: 0,
        })
        .await;
    assert!(search_result.is_ok());
    let cluster_result = sandbox.rest_client.cluster_state().await;
    assert!(cluster_result.is_ok());
    let indexing_service_state = sandbox.rest_client.indexing_service_state().await.unwrap();
    assert_eq!(indexing_service_state.num_running_pipelines, 1);
    Ok(())
}

#[tokio::test]
async fn test_multi_nodes_cluster() -> anyhow::Result<()> {
    let sandbox = ClusterSandbox::start_cluster_nodes().await.unwrap();
    let cluster_result = sandbox.rest_client.cluster_state().await;
    assert!(cluster_result.is_ok());
    assert_eq!(cluster_result.unwrap().live_nodes.len(), 2);
    let indexing_service_state = sandbox.rest_client.indexing_service_state().await.unwrap();
    assert_eq!(indexing_service_state.num_running_pipelines, 1);
    Ok(())
}
