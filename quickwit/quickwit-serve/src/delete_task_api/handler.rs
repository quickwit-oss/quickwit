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

use quickwit_config::build_doc_mapper;
use quickwit_janitor::error::JanitorError;
use quickwit_metastore::IndexMetadataResponseExt;
use quickwit_proto::metastore::{
    DeleteQuery, DeleteTask, IndexMetadataRequest, ListDeleteTasksRequest, MetastoreResult,
    MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::search::SearchRequest;
use quickwit_proto::types::{IndexId, IndexUid};
use quickwit_query::query_ast::{QueryAst, query_ast_from_user_text};
use serde::Deserialize;
use warp::{Filter, Rejection};

use crate::format::extract_format_from_qs;
use crate::rest::recover_fn;
use crate::rest_api_response::into_rest_api_response;
use crate::with_arg;

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(get_delete_tasks, post_delete_request),
    components(schemas(DeleteQueryRequest, DeleteTask, DeleteQuery))
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
    #[serde(rename(deserialize = "search_field"))]
    #[serde(default)]
    pub search_fields: Option<Vec<String>>,
    /// If set, restrict delete to documents with a `timestamp >= start_timestamp`.
    pub start_timestamp: Option<i64>,
    /// If set, restrict delete to documents with a `timestamp < end_timestamp``.
    pub end_timestamp: Option<i64>,
}

/// Delete query API handlers.
pub fn delete_task_api_handlers(
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    get_delete_tasks_handler(metastore.clone())
        .or(post_delete_tasks_handler(metastore.clone()))
        .recover(recover_fn)
        .boxed()
}

pub fn get_delete_tasks_handler(
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!(String / "delete-tasks")
        .and(warp::get())
        .and(with_arg(metastore))
        .then(get_delete_tasks)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
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
    index_id: IndexId,
    metastore: MetastoreServiceClient,
) -> MetastoreResult<Vec<DeleteTask>> {
    let index_metadata_request = IndexMetadataRequest::for_index_id(index_id.to_string());
    let index_uid: IndexUid = metastore
        .index_metadata(index_metadata_request)
        .await?
        .deserialize_index_metadata()?
        .index_uid;
    let list_delete_tasks_request = ListDeleteTasksRequest::new(index_uid, 0);
    let delete_tasks = metastore
        .list_delete_tasks(list_delete_tasks_request)
        .await?
        .delete_tasks;
    Ok(delete_tasks)
}

pub fn post_delete_tasks_handler(
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!(String / "delete-tasks")
        .and(warp::body::json())
        .and(warp::post())
        .and(with_arg(metastore))
        .then(post_delete_request)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
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
    index_id: IndexId,
    delete_request: DeleteQueryRequest,
    metastore: MetastoreServiceClient,
) -> Result<DeleteTask, JanitorError> {
    let index_metadata_request = IndexMetadataRequest::for_index_id(index_id.to_string());
    let metadata = metastore
        .index_metadata(index_metadata_request)
        .await?
        .deserialize_index_metadata()?;
    let index_uid: IndexUid = metadata.index_uid.clone();
    let query_ast = query_ast_from_user_text(&delete_request.query, delete_request.search_fields)
        .parse_user_query(&metadata.index_config.search_settings.default_search_fields)
        .map_err(|err| JanitorError::InvalidDeleteQuery(err.to_string()))?;
    let query_ast_json = serde_json::to_string(&query_ast).map_err(|_err| {
        JanitorError::Internal("failed to serialized delete query ast".to_string())
    })?;
    let delete_query = DeleteQuery {
        index_uid: Some(index_uid),
        start_timestamp: delete_request.start_timestamp,
        end_timestamp: delete_request.end_timestamp,
        query_ast: query_ast_json,
    };
    let index_config = metadata.into_index_config();
    // TODO should it be something else than a JanitorError?
    let doc_mapper = build_doc_mapper(&index_config.doc_mapping, &index_config.search_settings)
        .map_err(|error| JanitorError::Internal(error.to_string()))?;
    let delete_search_request = SearchRequest::try_from(delete_query.clone())
        .map_err(|error| JanitorError::InvalidDeleteQuery(error.to_string()))?;

    // Validate the delete query against the current doc mapping configuration.
    let query_ast: QueryAst = serde_json::from_str(&delete_search_request.query_ast)
        .map_err(|err| JanitorError::InvalidDeleteQuery(err.to_string()))?;
    doc_mapper
        .query(doc_mapper.schema(), query_ast, true, None)
        .map_err(|error| JanitorError::InvalidDeleteQuery(error.to_string()))?;
    let delete_task = metastore.create_delete_task(delete_query).await?;
    Ok(delete_task)
}

#[cfg(test)]
mod tests {
    use quickwit_indexing::TestSandbox;
    use quickwit_proto::metastore::DeleteTask;
    use warp::Filter;

    use crate::rest::recover_fn;

    #[tokio::test]
    async fn test_delete_task_api() {
        let index_id = "test-delete-task-rest";
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: title
                type: text
              - name: body
                type: text
              - name: ts
                type: i64
                fast: true
            mode: lenient
        "#;
        let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "", &["title"])
            .await
            .unwrap();
        let metastore = test_sandbox.metastore();
        let delete_query_api_handlers =
            super::delete_task_api_handlers(metastore).recover(recover_fn);

        // POST a delete query with explicit field name in query
        let resp = warp::test::request()
            .path("/test-delete-task-rest/delete-tasks")
            .method("POST")
            .json(&true)
            .body(r#"{"query": "body:myterm", "start_timestamp": 1, "end_timestamp": 10}"#)
            .reply(&delete_query_api_handlers)
            .await;
        assert_eq!(resp.status(), 200);
        let created_delete_task: DeleteTask = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(created_delete_task.opstamp, 1);
        let created_delete_query = created_delete_task.delete_query.unwrap();
        assert_eq!(created_delete_query.index_uid(), &test_sandbox.index_uid());
        assert_eq!(
            created_delete_query.query_ast,
            r#"{"type":"full_text","field":"body","text":"myterm","params":{"mode":{"type":"phrase_fallback_to_intersection"}},"lenient":false}"#
        );
        assert_eq!(created_delete_query.start_timestamp, Some(1));
        assert_eq!(created_delete_query.end_timestamp, Some(10));

        // POST a delete query with specified default field
        let resp = warp::test::request()
            .path("/test-delete-task-rest/delete-tasks")
            .method("POST")
            .json(&true)
            .body(r#"{"query": "myterm", "start_timestamp": 1, "end_timestamp": 10, "search_field": ["body"]}"#)
            .reply(&delete_query_api_handlers)
            .await;
        assert_eq!(resp.status(), 200);
        let created_delete_task: DeleteTask = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(created_delete_task.opstamp, 2);
        let created_delete_query = created_delete_task.delete_query.unwrap();
        assert_eq!(created_delete_query.index_uid(), &test_sandbox.index_uid());
        assert_eq!(
            created_delete_query.query_ast,
            r#"{"type":"full_text","field":"body","text":"myterm","params":{"mode":{"type":"phrase_fallback_to_intersection"}},"lenient":false}"#
        );
        assert_eq!(created_delete_query.start_timestamp, Some(1));
        assert_eq!(created_delete_query.end_timestamp, Some(10));

        // POST a delete query using the config default field
        let resp = warp::test::request()
            .path("/test-delete-task-rest/delete-tasks")
            .method("POST")
            .json(&true)
            .body(r#"{"query": "myterm", "start_timestamp": 1, "end_timestamp": 10}"#)
            .reply(&delete_query_api_handlers)
            .await;
        assert_eq!(resp.status(), 200);
        let created_delete_task: DeleteTask = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(created_delete_task.opstamp, 3);
        let created_delete_query = created_delete_task.delete_query.unwrap();
        assert_eq!(created_delete_query.index_uid(), &test_sandbox.index_uid());
        assert_eq!(
            created_delete_query.query_ast,
            r#"{"type":"full_text","field":"title","text":"myterm","params":{"mode":{"type":"phrase_fallback_to_intersection"}},"lenient":false}"#
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
        assert!(String::from_utf8_lossy(resp.body()).contains("invalid delete query"));

        // GET delete tasks.
        let resp = warp::test::request()
            .path("/test-delete-task-rest/delete-tasks")
            .reply(&delete_query_api_handlers)
            .await;
        assert_eq!(resp.status(), 200);
        let delete_tasks: Vec<DeleteTask> = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(delete_tasks.len(), 3);

        test_sandbox.assert_quit().await;
    }
}
