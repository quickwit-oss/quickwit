// Copyright (C) 2024 Quickwit, Inc.
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
    let create_index_request =
        CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid
        .into();
    let delete_query = DeleteQuery {
        index_uid: index_uid.clone().into(),
        query_ast: qast_json_helper("my_field:my_value", &[]),
        start_timestamp: Some(1),
        end_timestamp: Some(2),
    };

    // Create a delete task on non-existing index.
    let error = metastore
        .create_delete_task(DeleteQuery {
            index_uid: IndexUid::new_with_random_ulid("does-not-exist").to_string(),
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
            index_uid: IndexUid::from_parts(&index_id, "12345").to_string(),
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
    let index_uid_1: IndexUid = metastore
        .create_index(CreateIndexRequest::try_from_index_config(index_config_1.clone()).unwrap())
        .await
        .unwrap()
        .index_uid
        .into();
    let index_uid_2: IndexUid = metastore
        .create_index(CreateIndexRequest::try_from_index_config(index_config_2.clone()).unwrap())
        .await
        .unwrap()
        .index_uid
        .into();

    let delete_query_index_1 = DeleteQuery {
        index_uid: index_uid_1.to_string(),
        query_ast: qast_json_helper("my_field:my_value", &[]),
        start_timestamp: Some(1),
        end_timestamp: Some(2),
    };
    let delete_query_index_2 = DeleteQuery {
        index_uid: index_uid_2.to_string(),
        query_ast: qast_json_helper("my_field:my_value", &[]),
        start_timestamp: Some(1),
        end_timestamp: Some(2),
    };

    let last_opstamp_index_1_with_no_task = metastore
        .last_delete_opstamp(LastDeleteOpstampRequest {
            index_uid: index_uid_1.to_string(),
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
            index_uid: index_uid_1.to_string(),
        })
        .await
        .unwrap()
        .last_delete_opstamp;
    let last_opstamp_index_2 = metastore
        .last_delete_opstamp(LastDeleteOpstampRequest {
            index_uid: index_uid_2.to_string(),
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
    let mut metastore = MetastoreToTest::default_for_test().await;
    let index_id = append_random_suffix("delete-delete-tasks");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);
    let create_index_request =
        CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid
        .into();
    let delete_query = DeleteQuery {
        index_uid: index_uid.clone().into(),
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
            index_uid: index_uid.clone().into(),
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
    let index_uid_1: IndexUid = metastore
        .create_index(CreateIndexRequest::try_from_index_config(index_config_1.clone()).unwrap())
        .await
        .unwrap()
        .index_uid
        .into();
    let index_uid_2: IndexUid = metastore
        .create_index(CreateIndexRequest::try_from_index_config(index_config_2.clone()).unwrap())
        .await
        .unwrap()
        .index_uid
        .into();
    let delete_query_index_1 = DeleteQuery {
        index_uid: index_uid_1.to_string(),
        query_ast: qast_json_helper("my_field:my_value", &[]),
        start_timestamp: Some(1),
        end_timestamp: Some(2),
    };
    let delete_query_index_2 = DeleteQuery {
        index_uid: index_uid_2.to_string(),
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
