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

use std::convert::Infallible;
use std::sync::Arc;

use quickwit_actors::Mailbox;
use quickwit_janitor::actors::DeleteTaskService;
use quickwit_metastore::Metastore;
use quickwit_proto::metastore_api::DeleteQuery;
use serde::Deserialize;
use warp::{Filter, Rejection};

use crate::format::{Format, FormatError};
use crate::{require, with_arg};

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
    delete_task_service_mailbox_opt: Option<Mailbox<DeleteTaskService>>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    get_delete_tasks_handler(metastore.clone(), delete_task_service_mailbox_opt.clone())
        .or(post_delete_tasks_handler(delete_task_service_mailbox_opt))
}

pub fn get_delete_tasks_handler(
    metastore: Arc<dyn Metastore>,
    delete_task_service_mailbox: Option<Mailbox<DeleteTaskService>>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    warp::path!(String / "delete-tasks")
        .and(warp::get())
        .and(with_arg(metastore))
        .and(require(delete_task_service_mailbox))
        .and_then(get_delete_tasks)
}

// Returns delete tasks in json format for a given `index_id`.
// Note that `_delete_task_service_mailbox` is not used...
// Explanation: we don't want to expose any delete tasks endpoints without a running
// `DeleteTaskService`. This is ensured by requiring a `Mailbox<DeleteTaskService>` in
// `get_delete_tasks_handler` and consequently we get the mailbox in `get_delete_tasks` signature.
async fn get_delete_tasks(
    index_id: String,
    metastore: Arc<dyn Metastore>,
    _delete_task_service_mailbox: Mailbox<DeleteTaskService>,
) -> Result<impl warp::Reply, Infallible> {
    let delete_tasks = metastore.list_delete_tasks(&index_id, 0).await;
    Ok(Format::PrettyJson.make_rest_reply(delete_tasks))
}

pub fn post_delete_tasks_handler(
    delete_task_service_mailbox: Option<Mailbox<DeleteTaskService>>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    warp::path!(String / "delete-tasks")
        .and(warp::body::json())
        .and(warp::post())
        .and(require(delete_task_service_mailbox))
        .and_then(post_delete_request)
}

async fn post_delete_request(
    index_id: String,
    delete_request: DeleteQueryRequest,
    delete_task_service_mailbox: Mailbox<DeleteTaskService>,
) -> Result<impl warp::Reply, Infallible> {
    let delete_query = DeleteQuery {
        index_id: index_id.clone(),
        start_timestamp: delete_request.start_timestamp,
        end_timestamp: delete_request.end_timestamp,
        query: delete_request.query,
        search_fields: delete_request.search_fields,
    };
    let create_delete_task_reply = delete_task_service_mailbox
        .ask_for_res(delete_query)
        .await
        .map_err(FormatError::wrap);
    Ok(Format::PrettyJson.make_rest_reply(create_delete_task_reply))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_actors::Universe;
    use quickwit_indexing::TestSandbox;
    use quickwit_janitor::actors::DeleteTaskService;
    use quickwit_proto::metastore_api::DeleteTask;
    use quickwit_search::{MockSearchService, SearchClientPool};
    use quickwit_storage::StorageUriResolver;
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
        let metastore_uri = "ram:///delete-task-rest";
        let test_sandbox = TestSandbox::create(
            index_id,
            doc_mapping_yaml,
            "{}",
            &["body"],
            Some(metastore_uri),
        )
        .await
        .unwrap();
        let metastore = test_sandbox.metastore();
        let mock_search_service = MockSearchService::new();
        let client_pool = SearchClientPool::from_mocks(vec![Arc::new(mock_search_service)])
            .await
            .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir_path = temp_dir.path().to_path_buf();
        let delete_task_service = DeleteTaskService::new(
            metastore.clone(),
            client_pool,
            StorageUriResolver::for_test(),
            data_dir_path,
            4,
        );
        let universe = Universe::new();
        let (delete_task_service_mailbox, _delete_task_service_handler) =
            universe.spawn_builder().spawn(delete_task_service);

        let delete_query_api_handlers =
            super::delete_task_api_handlers(metastore, Some(delete_task_service_mailbox))
                .recover(recover_fn);
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

        let resp = warp::test::request()
            .path("/test-delete-task-rest/delete-tasks")
            .reply(&delete_query_api_handlers)
            .await;
        assert_eq!(resp.status(), 200);
        let delete_tasks: Vec<DeleteTask> = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(delete_tasks.len(), 1);
    }
}
