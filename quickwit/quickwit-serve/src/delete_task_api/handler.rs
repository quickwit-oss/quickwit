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

use std::sync::Arc;

use quickwit_config::{build_doc_mapper, IndexConfig};
use quickwit_janitor::error::JanitorError;
use quickwit_metastore::Metastore;
use quickwit_proto::metastore_api::{DeleteQuery, DeleteTask};
use quickwit_proto::SearchRequest;
use serde::Deserialize;
use warp::{Filter, Rejection};

use crate::format::Format;
use crate::with_arg;

/// This struct represents the delete query passed to
/// the rest API.
#[derive(Deserialize, Debug, Eq, PartialEq, Default)]
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
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    get_delete_tasks_handler(metastore.clone()).or(post_delete_tasks_handler(metastore.clone()))
}

pub fn get_delete_tasks_handler(
    metastore: Arc<dyn Metastore>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    warp::path!(String / "delete-tasks")
        .and(warp::get())
        .and(with_arg(metastore))
        .then(get_delete_tasks)
}

// Returns delete tasks in json format for a given `index_id`.
// Note that `_delete_task_service_mailbox` is not used...
// Explanation: we don't want to expose any delete tasks endpoints without a running
// `DeleteTaskService`. This is ensured by requiring a `Mailbox<DeleteTaskService>` in
// `get_delete_tasks_handler` and consequently we get the mailbox in `get_delete_tasks` signature.
async fn get_delete_tasks(index_id: String, metastore: Arc<dyn Metastore>) -> impl warp::Reply {
    let delete_tasks = metastore.list_delete_tasks(&index_id, 0).await;
    Format::PrettyJson.make_rest_reply(delete_tasks)
}

pub fn post_delete_tasks_handler(
    metastore: Arc<dyn Metastore>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    warp::path!(String / "delete-tasks")
        .and(warp::body::json())
        .and(warp::post())
        .and(with_arg(metastore))
        .then(post_delete_request)
        .map(|create_delete_res| Format::PrettyJson.make_rest_reply(create_delete_res))
}

async fn post_delete_request(
    index_id: String,
    delete_request: DeleteQueryRequest,
    metastore: Arc<dyn Metastore>,
) -> Result<DeleteTask, JanitorError> {
    let delete_query = DeleteQuery {
        index_id: index_id.clone(),
        start_timestamp: delete_request.start_timestamp,
        end_timestamp: delete_request.end_timestamp,
        query: delete_request.query,
        search_fields: delete_request.search_fields,
    };
    let index_config: IndexConfig = metastore
        .index_metadata(&delete_query.index_id)
        .await?
        .into_index_config();
    // TODO should it be something else than a JanitorError?
    let doc_mapper = build_doc_mapper(&index_config.doc_mapping, &index_config.search_settings)
        .map_err(|error| JanitorError::InternalError(error.to_string()))?;
    let delete_search_request = SearchRequest::from(delete_query.clone());
    // Validate the delete query.
    doc_mapper
        .query(doc_mapper.schema(), &delete_search_request)
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
            .body(r#"{"query": "term", "start_timestamp": 1, "end_timestamp": 10}"#)
            .reply(&delete_query_api_handlers)
            .await;
        assert_eq!(resp.status(), 200);
        let created_delete_task: DeleteTask = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(created_delete_task.opstamp, 1);
        let created_delete_query = created_delete_task.delete_query.unwrap();
        assert_eq!(created_delete_query.index_id, index_id);
        assert_eq!(created_delete_query.query, "term");
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
        assert!(String::from_utf8_lossy(resp.body()).contains("InvalidDeleteQuery"));

        // GET delete tasks.
        let resp = warp::test::request()
            .path("/test-delete-task-rest/delete-tasks")
            .reply(&delete_query_api_handlers)
            .await;
        assert_eq!(resp.status(), 200);
        let delete_tasks: Vec<DeleteTask> = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(delete_tasks.len(), 1);
    }
}
