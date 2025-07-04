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

use axum::{Extension, Json};
use quickwit_cluster::{Cluster, ClusterSnapshot};

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(get_cluster_axum),
    components(schemas(ClusterSnapshot,))
)]
pub struct ClusterApiAxum;

/// Create axum router for cluster API
pub fn cluster_axum_routes() -> axum::Router {
    axum::Router::new()
        .route("/cluster", axum::routing::get(get_cluster_axum))
}

#[utoipa::path(
    get,
    tag = "Cluster Info",
    path = "/cluster",
    responses(
        (status = 200, description = "Successfully fetched cluster information.", body = ClusterSnapshot)
    )
)]
/// Get cluster information (axum version).
async fn get_cluster_axum(Extension(cluster): Extension<Cluster>) -> Json<ClusterSnapshot> {
    let snapshot = cluster.snapshot().await;
    Json(snapshot)
}

#[cfg(test)]
mod tests {
    use assert_json_diff::assert_json_include;
    use axum_test::TestServer;
    use quickwit_cluster::{ChannelTransport, create_cluster_for_test};
    use serde_json::Value as JsonValue;

    use super::*;

    async fn mock_cluster() -> Cluster {
        let transport = ChannelTransport::default();
        create_cluster_for_test(Vec::new(), &[], &transport, false)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_cluster_axum_handler() {
        // Create a test cluster
        let cluster = mock_cluster().await;
        
        // Create the axum app with the cluster extension
        let app = cluster_axum_routes()
            .layer(Extension(cluster.clone()));
        
        // Create test server
        let server = TestServer::new(app).unwrap();
        
        // Make a GET request to /cluster
        let response = server.get("/cluster").await;
        
        // Assert the response is successful
        response.assert_status_ok();
        
        // Check the response content type
        response.assert_header("content-type", "application/json");
        
        // Parse the response body
        let cluster_info: JsonValue = response.json();
        
        // Verify the response structure matches ClusterSnapshot
        // It should contain cluster information like nodes, etc.
        assert!(cluster_info.is_object());
        
        // The actual cluster snapshot should contain expected fields
        // Since we're using a test cluster, let's just verify basic structure
        let expected_fields = serde_json::json!({
            "cluster_id": cluster_info.get("cluster_id"),
            "self_node_id": cluster_info.get("self_node_id"),
        });
        
        assert_json_include!(actual: cluster_info, expected: expected_fields);
    }
} 