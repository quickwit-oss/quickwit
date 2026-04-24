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

use std::convert::Infallible;

use quickwit_actors::{AskError, Mailbox, Observe};
use quickwit_indexing::actors::{IndexingService, IndexingServiceCounters};
use quickwit_proto::control_plane::{
    ControlPlaneError, ControlPlaneService, ControlPlaneServiceClient, SwapIndexingPipelinesEntry,
    SwapIndexingPipelinesRequest, SwapIndexingPipelinesResponse, SwapIndexingPipelinesResult,
};
use warp::{Filter, Rejection};

use crate::format::extract_format_from_qs;
use crate::require;
use crate::rest::recover_fn;
use crate::rest_api_response::into_rest_api_response;

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(indexing_endpoint, swap_pipelines_endpoint),
    components(schemas(
        SwapIndexingPipelinesRequest,
        SwapIndexingPipelinesResponse,
        SwapIndexingPipelinesEntry,
        SwapIndexingPipelinesResult,
    ))
)]
pub struct IndexingApi;

#[utoipa::path(
    get,
    tag = "Indexing",
    path = "/indexing",
    responses(
        (status = 200, description = "Successfully observed indexing pipelines.", body = IndexingStatistics)
    ),
)]
/// Observe Indexing Pipeline
async fn indexing_endpoint(
    indexing_service_mailbox: Mailbox<IndexingService>,
) -> Result<IndexingServiceCounters, AskError<Infallible>> {
    let counters = indexing_service_mailbox.ask(Observe).await?;
    indexing_service_mailbox.ask(Observe).await?;
    Ok(counters)
}

fn indexing_get_filter() -> impl Filter<Extract = (), Error = Rejection> + Clone {
    warp::path!("indexing").and(warp::get())
}

pub fn indexing_get_handler(
    indexing_service_mailbox_opt: Option<Mailbox<IndexingService>>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    indexing_get_filter()
        .and(require(indexing_service_mailbox_opt))
        .then(indexing_endpoint)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .recover(recover_fn)
        .boxed()
}

#[utoipa::path(
    post,
    tag = "Swap pipelines",
    path = "/indexing/swap-pipelines",
    request_body = SwapIndexingPipelinesRequest,
    responses(
        (status = 200, description = "Successfully swapped indexing pipelines.", body = SwapIndexingPipelinesResponse)
    )
)]
async fn swap_pipelines_endpoint(
    body: SwapIndexingPipelinesRequest,
    control_plane_client: ControlPlaneServiceClient,
) -> Result<SwapIndexingPipelinesResponse, ControlPlaneError> {
    control_plane_client.swap_indexing_pipelines(body).await
}

fn swap_pipelines_post_filter() -> impl Filter<Extract = (), Error = Rejection> + Clone {
    warp::path!("indexing" / "swap-pipelines").and(warp::post())
}

pub fn swap_pipelines_handler(
    control_plane_client: ControlPlaneServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    swap_pipelines_post_filter()
        .and(warp::body::json())
        .and(warp::any().map(move || control_plane_client.clone()))
        .then(swap_pipelines_endpoint)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .recover(recover_fn)
        .boxed()
}

#[cfg(test)]
mod tests {
    use quickwit_proto::control_plane::{
        ControlPlaneServiceClient, MockControlPlaneService, SwapIndexingPipelinesEntry,
        SwapIndexingPipelinesRequest, SwapIndexingPipelinesResponse, SwapIndexingPipelinesResult,
    };
    use warp::Filter;

    use super::swap_pipelines_handler;
    use crate::rest::recover_fn;

    #[tokio::test]
    async fn test_swap_pipelines_handler_success() {
        let mut mock = MockControlPlaneService::new();
        mock.expect_swap_indexing_pipelines().returning(|request| {
            let results = request
                .swaps
                .iter()
                .map(|swap| SwapIndexingPipelinesResult {
                    swap: Some(swap.clone()),
                    success: true,
                    reason: String::new(),
                })
                .collect();
            Ok(SwapIndexingPipelinesResponse { results })
        });
        let control_plane_client = ControlPlaneServiceClient::from_mock(mock);

        let handler = swap_pipelines_handler(control_plane_client).recover(recover_fn);

        let body = serde_json::to_vec(&SwapIndexingPipelinesRequest {
            swaps: vec![SwapIndexingPipelinesEntry {
                left_node_id: "indexer-1".to_string(),
                left_index_id: "index-a".to_string(),
                right_node_id: "indexer-2".to_string(),
                right_index_id: Some("index-b".to_string()),
            }],
        })
        .unwrap();

        let resp = warp::test::request()
            .method("POST")
            .path("/indexing/swap-pipelines")
            .header("content-type", "application/json")
            .body(body)
            .reply(&handler)
            .await;

        assert_eq!(resp.status(), 200);

        let response: SwapIndexingPipelinesResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(response.results.len(), 1);
        assert!(response.results[0].success);
        let swap = response.results[0].swap.as_ref().unwrap();
        assert_eq!(swap.left_node_id, "indexer-1");
        assert_eq!(swap.left_index_id, "index-a");
        assert_eq!(swap.right_node_id, "indexer-2");
        assert_eq!(swap.right_index_id.as_deref(), Some("index-b"));
    }

    #[tokio::test]
    async fn test_swap_pipelines_handler_partial_failure() {
        let mut mock = MockControlPlaneService::new();
        mock.expect_swap_indexing_pipelines().returning(|request| {
            let results = request
                .swaps
                .iter()
                .enumerate()
                .map(|(i, swap)| {
                    if i == 0 {
                        SwapIndexingPipelinesResult {
                            swap: Some(swap.clone()),
                            success: true,
                            reason: String::new(),
                        }
                    } else {
                        SwapIndexingPipelinesResult {
                            swap: Some(swap.clone()),
                            success: false,
                            reason: "pipeline count mismatch".to_string(),
                        }
                    }
                })
                .collect();
            Ok(SwapIndexingPipelinesResponse { results })
        });
        let control_plane_client = ControlPlaneServiceClient::from_mock(mock);

        let handler = swap_pipelines_handler(control_plane_client).recover(recover_fn);

        let body = serde_json::to_vec(&SwapIndexingPipelinesRequest {
            swaps: vec![
                SwapIndexingPipelinesEntry {
                    left_node_id: "indexer-1".to_string(),
                    left_index_id: "index-a".to_string(),
                    right_node_id: "indexer-2".to_string(),
                    right_index_id: Some("index-b".to_string()),
                },
                SwapIndexingPipelinesEntry {
                    left_node_id: "indexer-3".to_string(),
                    left_index_id: "index-c".to_string(),
                    right_node_id: "indexer-4".to_string(),
                    right_index_id: Some("index-d".to_string()),
                },
            ],
        })
        .unwrap();

        let resp = warp::test::request()
            .method("POST")
            .path("/indexing/swap-pipelines")
            .header("content-type", "application/json")
            .body(body)
            .reply(&handler)
            .await;

        assert_eq!(resp.status(), 200);

        let response: SwapIndexingPipelinesResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(response.results.len(), 2);
        assert!(response.results[0].success);
        assert!(!response.results[1].success);
        assert!(
            response.results[1]
                .reason
                .contains("pipeline count mismatch")
        );
    }

    #[tokio::test]
    async fn test_swap_pipelines_handler_move_without_right_index() {
        let mut mock = MockControlPlaneService::new();
        mock.expect_swap_indexing_pipelines().returning(|request| {
            let results = request
                .swaps
                .iter()
                .map(|swap| SwapIndexingPipelinesResult {
                    swap: Some(swap.clone()),
                    success: true,
                    reason: String::new(),
                })
                .collect();
            Ok(SwapIndexingPipelinesResponse { results })
        });
        let control_plane_client = ControlPlaneServiceClient::from_mock(mock);

        let handler = swap_pipelines_handler(control_plane_client).recover(recover_fn);

        // Send JSON without right_index_id field — should deserialize to None.
        let body = r#"{"swaps": [{"left_node_id": "indexer-1", "left_index_id": "index-a", "right_node_id": "indexer-2"}]}"#;

        let resp = warp::test::request()
            .method("POST")
            .path("/indexing/swap-pipelines")
            .header("content-type", "application/json")
            .body(body)
            .reply(&handler)
            .await;

        assert_eq!(resp.status(), 200);

        let response: SwapIndexingPipelinesResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(response.results.len(), 1);
        assert!(response.results[0].success);
        let swap = response.results[0].swap.as_ref().unwrap();
        assert_eq!(swap.left_node_id, "indexer-1");
        assert_eq!(swap.left_index_id, "index-a");
        assert_eq!(swap.right_node_id, "indexer-2");
        assert!(swap.right_index_id.is_none());
    }

    #[tokio::test]
    async fn test_swap_pipelines_handler_invalid_json_body() {
        let mock = MockControlPlaneService::new();
        let control_plane_client = ControlPlaneServiceClient::from_mock(mock);

        let handler = swap_pipelines_handler(control_plane_client).recover(recover_fn);

        let resp = warp::test::request()
            .method("POST")
            .path("/indexing/swap-pipelines")
            .header("content-type", "application/json")
            .body(b"not json at all")
            .reply(&handler)
            .await;

        // Warp returns 400 for invalid JSON bodies.
        assert_eq!(resp.status(), 400);
    }

    #[tokio::test]
    async fn test_swap_pipelines_handler_wrong_method() {
        let mock = MockControlPlaneService::new();
        let control_plane_client = ControlPlaneServiceClient::from_mock(mock);

        let handler = swap_pipelines_handler(control_plane_client).recover(recover_fn);

        let resp = warp::test::request()
            .method("GET")
            .path("/indexing/swap-pipelines")
            .reply(&handler)
            .await;

        // GET on a POST-only route returns 405.
        assert_eq!(resp.status(), 405);
    }
}
