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

use quickwit_common::rand::append_random_suffix;
use quickwit_config::IndexConfig;
use quickwit_proto::metastore::{
    CreateIndexRequest, DeleteIndexRequest, DeleteQuery, EntityKind, LastDeleteOpstampRequest,
    ListDeleteTasksRequest, MetastoreError,
};
use quickwit_proto::types::IndexUid;
use quickwit_query::query_ast::qast_json_helper;

use super::DefaultForTest;
use crate::tests::cleanup_index;
use crate::{CreateIndexRequestExt, MetastoreServiceExt};

pub async fn test_metastore_create_delete_task<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;
    let index_id = append_random_suffix("add-delete-task");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);
    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();
    let delete_query = DeleteQuery {
        index_uid: Some(index_uid.clone()),
        query_ast: qast_json_helper("my_field:my_value", &[]),
        start_timestamp: Some(1),
        end_timestamp: Some(2),
    };

    // Create a delete task on non-existing index.
    let error = metastore
        .create_delete_task(DeleteQuery {
            index_uid: Some(IndexUid::new_with_random_ulid("does-not-exist")),
            ..delete_query.clone()
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    // Create a delete task on an index with wrong incarnation_id
    let error = metastore
        .create_delete_task(DeleteQuery {
            index_uid: Some(IndexUid::for_test(&index_id, 12345)),
            ..delete_query.clone()
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    // Create a delete task.
    let delete_task_1 = metastore
        .create_delete_task(delete_query.clone())
        .await
        .unwrap();
    assert!(delete_task_1.opstamp > 0);
    let delete_query_1 = delete_task_1.delete_query.unwrap();
    assert_eq!(delete_query_1.index_uid, delete_query.index_uid);
    assert_eq!(delete_query_1.start_timestamp, delete_query.start_timestamp);
    assert_eq!(delete_query_1.end_timestamp, delete_query.end_timestamp);
    let delete_task_2 = metastore
        .create_delete_task(delete_query.clone())
        .await
        .unwrap();
    assert!(delete_task_2.opstamp > delete_task_1.opstamp);

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_last_delete_opstamp<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;
    let index_id_1 = append_random_suffix("test-last-delete-opstamp-1");
    let index_uri_1 = format!("ram:///indexes/{index_id_1}");
    let index_config_1 = IndexConfig::for_test(&index_id_1, &index_uri_1);
    let index_id_2 = append_random_suffix("test-last-delete-opstamp-2");
    let index_uri_2 = format!("ram:///indexes/{index_id_2}");
    let index_config_2 = IndexConfig::for_test(&index_id_2, &index_uri_2);
    let index_uid_1 = metastore
        .create_index(CreateIndexRequest::try_from_index_config(&index_config_1).unwrap())
        .await
        .unwrap()
        .index_uid()
        .clone();
    let index_uid_2 = metastore
        .create_index(CreateIndexRequest::try_from_index_config(&index_config_2).unwrap())
        .await
        .unwrap()
        .index_uid()
        .clone();

    let delete_query_index_1 = DeleteQuery {
        index_uid: Some(index_uid_1.clone()),
        query_ast: qast_json_helper("my_field:my_value", &[]),
        start_timestamp: Some(1),
        end_timestamp: Some(2),
    };
    let delete_query_index_2 = DeleteQuery {
        index_uid: Some(index_uid_2.clone()),
        query_ast: qast_json_helper("my_field:my_value", &[]),
        start_timestamp: Some(1),
        end_timestamp: Some(2),
    };

    let last_opstamp_index_1_with_no_task = metastore
        .last_delete_opstamp(LastDeleteOpstampRequest {
            index_uid: Some(index_uid_1.clone()),
        })
        .await
        .unwrap()
        .last_delete_opstamp;
    assert_eq!(last_opstamp_index_1_with_no_task, 0);

    // Create a delete task.
    metastore
        .create_delete_task(delete_query_index_1.clone())
        .await
        .unwrap();
    let delete_task_2 = metastore
        .create_delete_task(delete_query_index_1.clone())
        .await
        .unwrap();
    let delete_task_3 = metastore
        .create_delete_task(delete_query_index_2.clone())
        .await
        .unwrap();

    let last_opstamp_index_1 = metastore
        .last_delete_opstamp(LastDeleteOpstampRequest {
            index_uid: Some(index_uid_1.clone()),
        })
        .await
        .unwrap()
        .last_delete_opstamp;
    let last_opstamp_index_2 = metastore
        .last_delete_opstamp(LastDeleteOpstampRequest {
            index_uid: Some(index_uid_2.clone()),
        })
        .await
        .unwrap()
        .last_delete_opstamp;
    assert_eq!(last_opstamp_index_1, delete_task_2.opstamp);
    assert_eq!(last_opstamp_index_2, delete_task_3.opstamp);
    cleanup_index(&mut metastore, index_uid_1).await;
    cleanup_index(&mut metastore, index_uid_2).await;
}

pub async fn test_metastore_delete_index_with_tasks<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let metastore = MetastoreToTest::default_for_test().await;
    let index_id = append_random_suffix("delete-delete-tasks");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);
    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();
    let delete_query = DeleteQuery {
        index_uid: Some(index_uid.clone()),
        query_ast: qast_json_helper("my_field:my_value", &[]),
        start_timestamp: Some(1),
        end_timestamp: Some(2),
    };
    let _ = metastore
        .create_delete_task(delete_query.clone())
        .await
        .unwrap();
    let _ = metastore
        .create_delete_task(delete_query.clone())
        .await
        .unwrap();

    metastore
        .delete_index(DeleteIndexRequest {
            index_uid: Some(index_uid),
        })
        .await
        .unwrap();
}

pub async fn test_metastore_list_delete_tasks<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;
    let index_id_1 = append_random_suffix("test-list-delete-tasks-1");
    let index_uri_1 = format!("ram:///indexes/{index_id_1}");
    let index_config_1 = IndexConfig::for_test(&index_id_1, &index_uri_1);
    let index_id_2 = append_random_suffix("test-list-delete-tasks-2");
    let index_uri_2 = format!("ram:///indexes/{index_id_2}");
    let index_config_2 = IndexConfig::for_test(&index_id_2, &index_uri_2);
    let index_uid_1 = metastore
        .create_index(CreateIndexRequest::try_from_index_config(&index_config_1).unwrap())
        .await
        .unwrap()
        .index_uid()
        .clone();
    let index_uid_2 = metastore
        .create_index(CreateIndexRequest::try_from_index_config(&index_config_2).unwrap())
        .await
        .unwrap()
        .index_uid()
        .clone();
    let delete_query_index_1 = DeleteQuery {
        index_uid: Some(index_uid_1.clone()),
        query_ast: qast_json_helper("my_field:my_value", &[]),
        start_timestamp: Some(1),
        end_timestamp: Some(2),
    };
    let delete_query_index_2 = DeleteQuery {
        index_uid: Some(index_uid_2.clone()),
        query_ast: qast_json_helper("my_field:my_value", &[]),
        start_timestamp: Some(1),
        end_timestamp: Some(2),
    };

    // Create a delete task.
    let delete_task_1 = metastore
        .create_delete_task(delete_query_index_1.clone())
        .await
        .unwrap();
    let delete_task_2 = metastore
        .create_delete_task(delete_query_index_1.clone())
        .await
        .unwrap();
    let _ = metastore
        .create_delete_task(delete_query_index_2.clone())
        .await
        .unwrap();

    let all_index_id_1_delete_tasks = metastore
        .list_delete_tasks(ListDeleteTasksRequest::new(index_uid_1.clone(), 0))
        .await
        .unwrap()
        .delete_tasks;
    assert_eq!(all_index_id_1_delete_tasks.len(), 2);

    let recent_index_id_1_delete_tasks = metastore
        .list_delete_tasks(ListDeleteTasksRequest::new(
            index_uid_1.clone(),
            delete_task_1.opstamp,
        ))
        .await
        .unwrap()
        .delete_tasks;
    assert_eq!(recent_index_id_1_delete_tasks.len(), 1);
    assert_eq!(
        recent_index_id_1_delete_tasks[0].opstamp,
        delete_task_2.opstamp
    );
    cleanup_index(&mut metastore, index_uid_1).await;
    cleanup_index(&mut metastore, index_uid_2).await;
}
