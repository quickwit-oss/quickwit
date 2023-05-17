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

use std::sync::Arc;

use quickwit_config::build_doc_mapper;
use quickwit_janitor::error::JanitorError;
use quickwit_metastore::{Metastore, MetastoreError};
use quickwit_proto::metastore_api::{DeleteQuery, DeleteTask};
use quickwit_proto::{query_ast_from_user_text, IndexUid, SearchRequest};
use quickwit_query::query_ast::QueryAst;
use serde::Deserialize;
use warp::{Filter, Rejection};

use crate::format::extract_format_from_qs;
use crate::json_api_response::make_json_api_response;
use crate::with_arg;

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(get_delete_tasks, post_delete_request),
    components(schemas(DeleteQueryRequest, DeleteTask, DeleteQuery,))
)]
pub struct DeleteTaskApi;

/// This struct represents the delete query passed to
/// the rest API.
#[derive(Deserialize, Debug, Eq, PartialEq, Default, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeleteQueryRequest {
    /// Query text. The query language is that of tantivy.
    pub query: String,
    // Fields to search on
    #[serde(default)]
    pub search_fields: Vec<String>,
    /// If set, restrict delete to documents with a `timestamp >= start_timestamp`.
    pub start_timestamp: Option<i64>,
    /// If set, restrict delete to documents with a `timestamp < end_timestamp``.
    pub end_timestamp: Option<i64>,
}

/// Delete query API handlers.
pub fn delete_task_api_handlers(
    metastore: Arc<dyn Metastore>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    get_delete_tasks_handler(metastore.clone()).or(post_delete_tasks_handler(metastore.clone()))
}

pub fn get_delete_tasks_handler(
    metastore: Arc<dyn Metastore>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!(String / "delete-tasks")
        .and(warp::get())
        .and(with_arg(metastore))
        .then(get_delete_tasks)
        .and(extract_format_from_qs())
        .map(make_json_api_response)
}

#[utoipa::path(
    get,
    tag = "Delete Tasks",
    path = "/{index_id}/delete-tasks",
    responses(
        (status = 200, description = "Successfully fetched delete tasks.", body = [DeleteTask])
    ),
    params(
        ("index_id" = String, Path, description = "The index ID to retrieve delete tasks for."),
    )
)]
/// Get Delete Tasks
///
/// Returns delete tasks in json format for a given `index_id`.
// Note that `_delete_task_service_mailbox` is not used...
// Explanation: we don't want to expose any delete tasks endpoints without a running
// `DeleteTaskService`. This is ensured by requiring a `Mailbox<DeleteTaskService>` in
// `get_delete_tasks_handler` and consequently we get the mailbox in `get_delete_tasks` signature.
pub async fn get_delete_tasks(
    index_id: String,
    metastore: Arc<dyn Metastore>,
) -> Result<Vec<DeleteTask>, MetastoreError> {
    let index_uid: IndexUid = metastore.index_metadata(&index_id).await?.index_uid;
    let delete_tasks = metastore.list_delete_tasks(index_uid, 0).await?;
    Ok(delete_tasks)
}

pub fn post_delete_tasks_handler(
    metastore: Arc<dyn Metastore>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!(String / "delete-tasks")
        .and(warp::body::json())
        .and(warp::post())
        .and(with_arg(metastore))
        .then(post_delete_request)
        .and(extract_format_from_qs())
        .map(make_json_api_response)
}

#[utoipa::path(
    post,
    tag = "Delete Tasks",
    path = "/{index_id}/delete-tasks",
    request_body = DeleteQueryRequest,
    responses(
        (status = 200, description = "Successfully added a new delete task.", body = DeleteTask)
    ),
    params(
        ("index_id" = String, Path, description = "The index ID to add the delete task to."),
    )
)]
/// Create Delete Task
///
/// This operation will not be immediately executed, instead it will be added to a queue
/// and cleaned up in the near future.
pub async fn post_delete_request(
    index_id: String,
    delete_request: DeleteQueryRequest,
    metastore: Arc<dyn Metastore>,
) -> Result<DeleteTask, JanitorError> {
    let metadata = metastore.index_metadata(&index_id).await?;
    let index_uid: IndexUid = metadata.index_uid.clone();
    let query_ast = query_ast_from_user_text(&delete_request.query, Some(Vec::new()))
        .parse_user_query(&[])
        .map_err(|err| JanitorError::InvalidDeleteQuery(err.to_string()))?;
    let query_ast_json = serde_json::to_string(&query_ast).map_err(|_err| {
        JanitorError::InternalError("Failed to serialized delete query ast".to_string())
    })?;
    let delete_query = DeleteQuery {
        index_uid: index_uid.to_string(),
        start_timestamp: delete_request.start_timestamp,
        end_timestamp: delete_request.end_timestamp,
        query_ast: query_ast_json,
    };
    let index_config = metadata.into_index_config();
    // TODO should it be something else than a JanitorError?
    let doc_mapper = build_doc_mapper(&index_config.doc_mapping, &index_config.search_settings)
        .map_err(|error| JanitorError::InternalError(error.to_string()))?;
    let delete_search_request = SearchRequest::try_from(delete_query.clone())
        .map_err(|error| JanitorError::InvalidDeleteQuery(error.to_string()))?;

    // Validate the delete query against the current doc mapping configuration.
    let query_ast: QueryAst = serde_json::from_str(&delete_search_request.query_ast)
        .map_err(|err| JanitorError::InvalidDeleteQuery(err.to_string()))?;
    doc_mapper
        .query(doc_mapper.schema(), &query_ast, true)
        .map_err(|error| JanitorError::InvalidDeleteQuery(error.to_string()))?;
    let delete_task = metastore.create_delete_task(delete_query).await?;
    Ok(delete_task)
}

#[cfg(test)]
mod tests {
    use quickwit_indexing::TestSandbox;
    use quickwit_proto::metastore_api::DeleteTask;
    use warp::Filter;

    use crate::rest::recover_fn;

    #[tokio::test]
    async fn test_delete_task_api() {
        quickwit_common::setup_logging_for_tests();
        let index_id = "test-delete-task-rest";
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: body
                type: text
              - name: ts
                type: i64
                fast: true
        "#;
        let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["body"])
            .await
            .unwrap();
        let metastore = test_sandbox.metastore();
        let delete_query_api_handlers =
            super::delete_task_api_handlers(metastore).recover(recover_fn);
        let resp = warp::test::request()
            .path("/test-delete-task-rest/delete-tasks")
            .method("POST")
            .json(&true)
            .body(r#"{"query": "body:term", "start_timestamp": 1, "end_timestamp": 10}"#)
            .reply(&delete_query_api_handlers)
            .await;
        assert_eq!(resp.status(), 200);
        let created_delete_task: DeleteTask = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(created_delete_task.opstamp, 1);
        let created_delete_query = created_delete_task.delete_query.unwrap();
        assert_eq!(
            created_delete_query.index_uid,
            test_sandbox.index_uid().to_string()
        );
        assert_eq!(
            created_delete_query.query_ast,
            r#"{"type":"Phrase","field":"body","phrase":"term"}"#
        );
        assert_eq!(created_delete_query.start_timestamp, Some(1));
        assert_eq!(created_delete_query.end_timestamp, Some(10));

        // POST an invalid delete query.
        let resp = warp::test::request()
            .path("/test-delete-task-rest/delete-tasks")
            .method("POST")
            .json(&true)
            .body(r#"{"query": "unknown_field:test", "start_timestamp": 1, "end_timestamp": 10}"#)
            .reply(&delete_query_api_handlers)
            .await;
        assert_eq!(resp.status(), 400);
        assert!(String::from_utf8_lossy(resp.body()).contains("Invalid delete query"));

        // GET delete tasks.
        let resp = warp::test::request()
            .path("/test-delete-task-rest/delete-tasks")
            .reply(&delete_query_api_handlers)
            .await;
        assert_eq!(resp.status(), 200);
        let delete_tasks: Vec<DeleteTask> = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(delete_tasks.len(), 1);
        test_sandbox.assert_quit().await;
    }
}
