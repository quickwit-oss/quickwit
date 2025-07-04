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

use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use axum::extract::Query;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Extension, Router, middleware};
use quickwit_config::SearcherConfig;
use quickwit_config::service::QuickwitService;
use quickwit_proto::metastore::{MetastoreService, MetastoreServiceClient};
use quickwit_search::{
    ClusterClient, SearchJobPlacer, SearchService, SearchServiceClient, SearchServiceImpl,
    SearcherContext, SearcherPool,
};
use quickwit_serve::lambda_search_api::*;
use quickwit_storage::StorageResolver;
use quickwit_telemetry::payload::{QuickwitFeature, QuickwitTelemetryInfo, TelemetryEvent};
use tracing::info;

use crate::searcher::environment::CONFIGURATION_TEMPLATE;
use crate::utils::load_node_config;

async fn create_local_search_service(
    searcher_config: SearcherConfig,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
) -> Arc<dyn SearchService> {
    let searcher_pool = SearcherPool::default();
    let search_job_placer = SearchJobPlacer::new(searcher_pool.clone());
    let cluster_client = ClusterClient::new(search_job_placer);
    // TODO configure split cache
    let searcher_context = Arc::new(SearcherContext::new(searcher_config, None));
    let search_service = Arc::new(SearchServiceImpl::new(
        metastore,
        storage_resolver,
        cluster_client.clone(),
        searcher_context.clone(),
    ));
    // Add search service to pool to avoid "no available searcher nodes in the pool" error
    let socket_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 7280u16);
    let search_service_client =
        SearchServiceClient::from_service(search_service.clone(), socket_addr);
    searcher_pool.insert(socket_addr, search_service_client);
    search_service
}

fn native_api_routes() -> Router {
    Router::new().route(
        "/search",
        get(search_get_handler_axum).post(search_post_handler_axum),
    )
}

// Axum wrappers for search handlers
async fn search_get_handler_axum(
    Query(search_query_params): Query<quickwit_serve::SearchRequestQueryString>,
    Extension(search_service): Extension<Arc<dyn SearchService>>,
) -> impl axum::response::IntoResponse {
    let _body_format = search_query_params.format;
    let index_id_patterns = vec!["*".to_string()]; // Default to all indexes for lambda

    // Use the same search_endpoint function that the main search API uses
    let result = search_endpoint(
        index_id_patterns,
        search_query_params,
        search_service.as_ref(),
    )
    .await;
    match result {
        Ok(search_response) => axum::Json(search_response).into_response(),
        Err(error) => {
            let status_code = match error {
                quickwit_search::SearchError::IndexesNotFound { .. } => {
                    axum::http::StatusCode::NOT_FOUND
                }
                quickwit_search::SearchError::InvalidQuery(_) => {
                    axum::http::StatusCode::BAD_REQUEST
                }
                _ => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status_code, axum::Json(error)).into_response()
        }
    }
}

async fn search_post_handler_axum(
    Extension(search_service): Extension<Arc<dyn SearchService>>,
    axum::extract::Json(search_query_params): axum::extract::Json<
        quickwit_serve::SearchRequestQueryString,
    >,
) -> impl axum::response::IntoResponse {
    let _body_format = search_query_params.format;
    let index_id_patterns = vec!["*".to_string()]; // Default to all indexes for lambda

    // Use the same search_endpoint function that the main search API uses
    let result = search_endpoint(
        index_id_patterns,
        search_query_params,
        search_service.as_ref(),
    )
    .await;
    match result {
        Ok(search_response) => axum::Json(search_response).into_response(),
        Err(error) => {
            let status_code = match error {
                quickwit_search::SearchError::IndexesNotFound { .. } => {
                    axum::http::StatusCode::NOT_FOUND
                }
                quickwit_search::SearchError::InvalidQuery(_) => {
                    axum::http::StatusCode::BAD_REQUEST
                }
                _ => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status_code, axum::Json(error)).into_response()
        }
    }
}

// Helper function to reuse the search endpoint logic
async fn search_endpoint(
    index_id_patterns: Vec<String>,
    search_request: quickwit_serve::SearchRequestQueryString,
    search_service: &dyn SearchService,
) -> Result<quickwit_search::SearchResponseRest, quickwit_search::SearchError> {
    let allow_failed_splits = search_request.allow_failed_splits;
    let search_request =
        quickwit_serve::search_request_from_api_request(index_id_patterns, search_request)?;
    let search_response =
        search_service
            .root_search(search_request)
            .await
            .and_then(|search_response| {
                if !allow_failed_splits || search_response.num_successful_splits == 0 {
                    if let Some(search_error) = quickwit_search::SearchError::from_split_errors(
                        &search_response.failed_splits[..],
                    ) {
                        return Err(search_error);
                    }
                }
                Ok(search_response)
            })?;
    let search_response_rest = quickwit_search::SearchResponseRest::try_from(search_response)?;
    Ok(search_response_rest)
}

fn es_compat_api_routes() -> Router {
    Router::new()
        // Global elasticsearch endpoints
        .route("/_elastic/_search", get(es_compat_search_handler))
    // .route("/_elastic/_search", post(es_compat_search_handler))
    // .route("/_elastic/_stats", get(es_compat_stats_handler_axum))
    // .route("/_elastic/_cat/indices", get(es_compat_cat_indices_handler))
    // .route("/_elastic/_field_caps", get(es_compat_field_capabilities_handler_axum))
    // .route("/_elastic/_field_caps", post(es_compat_field_capabilities_handler_axum))
    // .route("/_elastic/_msearch", post(es_compat_multi_search_handler_axum))
    // .route("/_elastic/_search/scroll", get(es_compat_scroll_handler_axum))
    // .route("/_elastic/_search/scroll", post(es_compat_scroll_handler_axum))
    // .route("/_elastic/_bulk", post(es_compat_bulk_handler))
    // .route("/_elastic/_cluster/health", get(es_compat_cluster_health_handler_axum))

    // // Index-specific elasticsearch endpoints
    // .route("/_elastic/{index}/_search", get(es_compat_index_search_handler))
    // .route("/_elastic/{index}/_search", post(es_compat_index_search_handler))
    // .route("/_elastic/{index}/_count", get(es_compat_index_count_handler_axum))
    // .route("/_elastic/{index}/_count", post(es_compat_index_count_handler_axum))
    // .route("/_elastic/{index}/_stats", get(es_compat_index_stats_handler_axum))
    // .route("/_elastic/{index}/_field_caps", get(es_compat_index_field_capabilities_handler_axum))
    // .route("/_elastic/{index}/_field_caps",
    // post(es_compat_index_field_capabilities_handler_axum)) .route("/_elastic/{index}/_bulk",
    // post(es_compat_index_bulk_handler_axum)) .route("/_elastic/_cat/indices/{index}",
    // get(es_compat_index_cat_indices_handler_axum)) .route("/_elastic/_resolve/index/{index}",
    // get(es_compat_resolve_index_handler_axum)) .route("/_elastic/{index}",
    // axum::routing::delete(es_compat_delete_index_handler_axum))
}

async fn get_index_metadata_handler_axum(
    axum::extract::Path(index_id): axum::extract::Path<String>,
    Extension(metastore): Extension<MetastoreServiceClient>,
) -> impl axum::response::IntoResponse {
    // Simple wrapper for the metadata handler
    match metastore
        .index_metadata(
            quickwit_proto::metastore::IndexMetadataRequest::for_index_id(index_id.clone()),
        )
        .await
    {
        Ok(response) => axum::Json(response.index_metadata_serialized_json).into_response(),
        Err(error) => {
            let status_code = axum::http::StatusCode::INTERNAL_SERVER_ERROR;
            (
                status_code,
                axum::Json(format!(
                    "Error fetching metadata for index {}: {}",
                    index_id, error
                )),
            )
                .into_response()
        }
    }
}

fn index_api_routes() -> Router {
    Router::new().route(
        "/indexes/{index_id}/metadata",
        get(get_index_metadata_handler_axum),
    )
}

fn v1_searcher_api_routes() -> Router {
    Router::new().nest(
        "/api/v1",
        native_api_routes()
            .merge(es_compat_api_routes())
            .merge(index_api_routes()),
    )
}

pub async fn setup_searcher_api() -> anyhow::Result<Router> {
    let (node_config, storage_resolver, metastore) =
        load_node_config(CONFIGURATION_TEMPLATE).await?;

    let telemetry_info = QuickwitTelemetryInfo::new(
        HashSet::from_iter([QuickwitService::Searcher.as_str().to_string()]),
        HashSet::from_iter([QuickwitFeature::AwsLambda]),
    );
    let _telemetry_handle_opt = quickwit_telemetry::start_telemetry_loop(telemetry_info);

    let search_service = create_local_search_service(
        node_config.searcher_config,
        metastore.clone(),
        storage_resolver,
    )
    .await;

    // Setup middleware for telemetry and logging
    let telemetry_layer = middleware::from_fn(
        |req: axum::extract::Request, next: axum::middleware::Next| async move {
            let method = req.method().to_string();
            let uri = req.uri().to_string();
            info!(method = %method, route = %uri, "new request");
            quickwit_telemetry::send_telemetry_event(TelemetryEvent::RunCommand).await;

            let response = next.run(req).await;
            let status = response.status().as_u16();
            info!(status = %status, "request completed");
            response
        },
    );

    let api = v1_searcher_api_routes()
        .layer(Extension(search_service))
        .layer(Extension(metastore))
        .layer(telemetry_layer);

    Ok(api)
}
