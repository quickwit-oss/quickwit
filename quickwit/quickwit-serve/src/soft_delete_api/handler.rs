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

use itertools::Itertools;
use quickwit_metastore::IndexMetadataResponseExt;
use quickwit_proto::metastore::{
    IndexMetadataRequest, MetastoreService, MetastoreServiceClient, SoftDeleteDocumentsRequest,
    SoftDeleteDocumentsResponse, SplitDocIds,
};
use quickwit_proto::search::SearchRequest;
use quickwit_proto::types::IndexId;
use quickwit_query::query_ast::query_ast_from_user_text;
use quickwit_search::{SearchError, SearchService};
use serde::{Deserialize, Serialize};
use warp::{Filter, Rejection};

use crate::format::extract_format_from_qs;
use crate::rest::recover_fn;
use crate::rest_api_response::into_rest_api_response;
use crate::with_arg;

const MAX_SOFT_DELETED_HITS: u64 = 100;

#[allow(dead_code)]
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(post_soft_delete),
    components(schemas(SoftDeleteRequest, SoftDeleteResponse))
)]
pub struct SoftDeleteApi;

/// Request body for the soft-delete endpoint.
#[derive(Deserialize, Debug, PartialEq, Eq, Default, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SoftDeleteRequest {
    /// Query text in Tantivy query language to match events to soft-delete.
    pub query: String,
    /// Maximum number of events to soft-delete in a single call (default: 10000).
    #[serde(default = "default_max_soft_deletes")]
    pub max_hits: u64,
    /// If set, restrict soft-delete to documents with a `timestamp >= start_timestamp`.
    pub start_timestamp: Option<i64>,
    /// If set, restrict soft-delete to documents with a `timestamp < end_timestamp`.
    pub end_timestamp: Option<i64>,
}

fn default_max_soft_deletes() -> u64 {
    MAX_SOFT_DELETED_HITS
}

/// Response from the soft-delete endpoint.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, utoipa::ToSchema)]
pub struct SoftDeleteResponse {
    /// Total number of doc_ids that were newly soft-deleted across all splits.
    pub num_soft_deleted_doc_ids: u64,
}

/// Top-level filter combining all soft-delete API handlers.
pub fn soft_delete_api_handlers(
    search_service: Arc<dyn SearchService>,
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    post_soft_delete_handler(search_service, metastore.clone())
        .recover(recover_fn)
        .boxed()
}

fn post_soft_delete_handler(
    search_service: Arc<dyn SearchService>,
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!(String / "soft-delete")
        .and(warp::body::json())
        .and(warp::post())
        .and(with_arg(search_service))
        .and(with_arg(metastore))
        .then(post_soft_delete)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
}

#[utoipa::path(
    post,
    tag = "Soft Delete",
    path = "/{index_id}/soft-delete",
    request_body = SoftDeleteRequest,
    responses(
        (status = 200, description = "Successfully soft-deleted documents.", body = SoftDeleteResponse)
    ),
    params(
        ("index_id" = String, Path, description = "The index ID to soft-delete documents from."),
    )
)]
/// Soft Delete Documents
///
/// Runs a search query to identify matching documents, then records their internal
/// doc IDs in the metastore so they are excluded from future search results.
pub async fn post_soft_delete(
    index_id: IndexId,
    request: SoftDeleteRequest,
    search_service: Arc<dyn SearchService>,
    metastore: MetastoreServiceClient,
) -> Result<SoftDeleteResponse, SearchError> {
    // 1. Build a SearchRequest from the soft-delete query.
    // Validate the query and make sure it doesn't require default search fields
    query_ast_from_user_text(&request.query, None).parse_user_query(&[])?;
    let query_ast = query_ast_from_user_text(&request.query, None);
    let query_ast_json = serde_json::to_string(&query_ast)
        .map_err(|err| SearchError::Internal(format!("failed to serialize query AST: {err}")))?;

    // Enforce a hits limit that guarantee we won't delete
    // more than MAX_SOFT_DELETED_HITS per split
    let max_hits = if request.max_hits > MAX_SOFT_DELETED_HITS {
        MAX_SOFT_DELETED_HITS
    } else {
        request.max_hits
    };

    let search_request = SearchRequest {
        index_id_patterns: vec![index_id.to_string()],
        query_ast: query_ast_json,
        max_hits,
        start_timestamp: request.start_timestamp,
        end_timestamp: request.end_timestamp,
        ..Default::default()
    };

    // 2. Execute root_search to get PartialHits (split_id, doc_id).
    let search_response = search_service.root_search(search_request).await?;

    // 3. Group hits by split_id.
    let split_doc_ids: Vec<SplitDocIds> = search_response
        .hits
        .iter()
        .filter_map(|hit| hit.partial_hit.as_ref())
        .into_group_map_by(|ph| ph.split_id.clone())
        .into_iter()
        .map(|(split_id, hits)| SplitDocIds {
            split_id,
            doc_ids: hits.into_iter().map(|h| h.doc_id).collect(),
        })
        .collect();

    if split_doc_ids.is_empty() {
        return Ok(SoftDeleteResponse {
            num_soft_deleted_doc_ids: 0,
        });
    }

    // 4. Resolve index_uid.
    let index_metadata_request = IndexMetadataRequest::for_index_id(index_id.to_string());
    let index_uid = metastore
        .index_metadata(index_metadata_request)
        .await
        .map_err(|err| SearchError::Internal(format!("failed to fetch index metadata: {err}")))?
        .deserialize_index_metadata()
        .map_err(|err| {
            SearchError::Internal(format!("failed to deserialize index metadata: {err}"))
        })?
        .index_uid;

    // 5. Store in metastore.
    let SoftDeleteDocumentsResponse {
        num_soft_deleted_doc_ids,
    } = metastore
        .soft_delete_documents(SoftDeleteDocumentsRequest {
            index_uid: Some(index_uid),
            split_doc_ids,
        })
        .await
        .map_err(|err| SearchError::Internal(format!("failed to soft-delete documents: {err}")))?;

    Ok(SoftDeleteResponse {
        num_soft_deleted_doc_ids,
    })
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use quickwit_config::SearcherConfig;
    use quickwit_indexing::TestSandbox;
    use quickwit_search::{ClusterClient, SearchJobPlacer, SearchServiceImpl, SearcherPool};
    use warp::Filter;

    use super::*;
    use crate::rest::recover_fn;

    /// Build a real `Arc<dyn SearchService>` wired to the given `TestSandbox`.
    async fn build_search_service(sandbox: &TestSandbox) -> Arc<dyn SearchService> {
        let socket_addr = SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 7280u16);
        let searcher_pool = SearcherPool::default();
        let search_job_placer = SearchJobPlacer::new(searcher_pool.clone());
        let cluster_client = ClusterClient::new(search_job_placer);
        let searcher_config = SearcherConfig::default();
        let searcher_context =
            Arc::new(quickwit_search::SearcherContext::new(searcher_config, None));
        let search_service: Arc<dyn SearchService> = Arc::new(SearchServiceImpl::new(
            sandbox.metastore(),
            sandbox.storage_resolver(),
            cluster_client,
            searcher_context,
        ));
        let search_service_client =
            quickwit_search::SearchServiceClient::from_service(search_service.clone(), socket_addr);
        searcher_pool.insert(socket_addr, search_service_client);
        search_service
    }

    #[tokio::test]
    async fn test_soft_delete_api_post_no_matching_docs() {
        let index_id = "test-soft-delete-rest";
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: title
                type: text
              - name: body
                type: text
            mode: lenient
        "#;
        let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "", &["title"])
            .await
            .unwrap();
        let metastore = test_sandbox.metastore();
        let search_service = build_search_service(&test_sandbox).await;
        let handler = soft_delete_api_handlers(search_service, metastore).recover(recover_fn);

        // POST a soft-delete query matching no docs → should get 0
        let resp = warp::test::request()
            .path("/test-soft-delete-rest/soft-delete")
            .method("POST")
            .json(&true)
            .body(r#"{"query": "title:nonexistent_term_xyz"}"#)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), 200);
        let response: SoftDeleteResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(response.num_soft_deleted_doc_ids, 0);

        test_sandbox.assert_quit().await;
    }

    #[tokio::test]
    async fn test_soft_delete_api_post_with_matching_docs() {
        let index_id = "test-soft-delete-match";
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: title
                type: text
            mode: lenient
        "#;
        let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "", &["title"])
            .await
            .unwrap();

        // Ingest some documents.
        let docs = vec![
            serde_json::json!({"title": "apple"}),
            serde_json::json!({"title": "banana"}),
            serde_json::json!({"title": "cherry"}),
        ];
        test_sandbox.add_documents(docs).await.unwrap();

        let metastore = test_sandbox.metastore();
        let search_service = build_search_service(&test_sandbox).await;
        let handler = soft_delete_api_handlers(search_service, metastore).recover(recover_fn);

        // Soft-delete documents matching "apple".
        let resp = warp::test::request()
            .path("/test-soft-delete-match/soft-delete")
            .method("POST")
            .json(&true)
            .body(r#"{"query": "title:apple"}"#)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), 200);
        let response: SoftDeleteResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(response.num_soft_deleted_doc_ids, 1);

        test_sandbox.assert_quit().await;
    }

    #[tokio::test]
    async fn test_soft_delete_api_post_idempotent() {
        let index_id = "test-soft-delete-idempotent";
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: title
                type: text
            mode: lenient
        "#;
        let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "", &["title"])
            .await
            .unwrap();

        let docs = vec![serde_json::json!({"title": "apple"})];
        test_sandbox.add_documents(docs).await.unwrap();

        let metastore = test_sandbox.metastore();
        let search_service = build_search_service(&test_sandbox).await;
        let handler = soft_delete_api_handlers(search_service, metastore).recover(recover_fn);

        // First soft-delete.
        let resp = warp::test::request()
            .path("/test-soft-delete-idempotent/soft-delete")
            .method("POST")
            .json(&true)
            .body(r#"{"query": "title:apple"}"#)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), 200);
        let response: SoftDeleteResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(response.num_soft_deleted_doc_ids, 1);

        // Second soft-delete of same doc — the doc is already excluded from search
        // results, so the search won't find it again, yielding 0 new deletions.
        let resp = warp::test::request()
            .path("/test-soft-delete-idempotent/soft-delete")
            .method("POST")
            .json(&true)
            .body(r#"{"query": "title:apple"}"#)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), 200);
        let response: SoftDeleteResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(response.num_soft_deleted_doc_ids, 0);

        test_sandbox.assert_quit().await;
    }

    #[tokio::test]
    async fn test_soft_delete_api_post_deny_unknown_fields() {
        let index_id = "test-soft-delete-unknown";
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: title
                type: text
            mode: lenient
        "#;
        let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "", &["title"])
            .await
            .unwrap();
        let metastore = test_sandbox.metastore();
        let search_service = build_search_service(&test_sandbox).await;
        let handler = soft_delete_api_handlers(search_service, metastore).recover(recover_fn);

        // POST with unknown field should fail.
        let resp = warp::test::request()
            .path("/test-soft-delete-unknown/soft-delete")
            .method("POST")
            .json(&true)
            .body(r#"{"query": "title:apple", "unknown_field": true}"#)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), 400);

        test_sandbox.assert_quit().await;
    }
}
