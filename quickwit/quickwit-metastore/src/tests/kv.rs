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

use quickwit_proto::metastore::{DeleteKvRequest, GetKvRequest, MetastoreService, SetKvRequest};

use super::DefaultForTest;

pub async fn test_metastore_kv_set_get<MetastoreToTest: MetastoreService + DefaultForTest>() {
    let metastore = MetastoreToTest::default_for_test().await;

    // Set a key-value pair
    let set_request = SetKvRequest {
        key: "test-key".to_string(),
        value: "test-value".to_string(),
    };
    metastore.set_kv(set_request).await.unwrap();

    // Get the key-value pair
    let get_request = GetKvRequest {
        key: "test-key".to_string(),
    };
    let response = metastore.get_kv(get_request).await.unwrap();
    assert_eq!(response.value, Some("test-value".to_string()));
}

pub async fn test_metastore_kv_get_non_existent<
    MetastoreToTest: MetastoreService + DefaultForTest,
>() {
    let metastore = MetastoreToTest::default_for_test().await;

    // Try to get a non-existent key
    let get_request = GetKvRequest {
        key: "non-existent-key".to_string(),
    };
    let response = metastore.get_kv(get_request).await.unwrap();
    assert_eq!(response.value, None);
}

pub async fn test_metastore_kv_set_overwrite<MetastoreToTest: MetastoreService + DefaultForTest>() {
    let metastore = MetastoreToTest::default_for_test().await;

    // Set a key-value pair
    let set_request = SetKvRequest {
        key: "test-key".to_string(),
        value: "original-value".to_string(),
    };
    metastore.set_kv(set_request).await.unwrap();

    // Overwrite with new value
    let set_request = SetKvRequest {
        key: "test-key".to_string(),
        value: "updated-value".to_string(),
    };
    metastore.set_kv(set_request).await.unwrap();

    // Verify the value was updated
    let get_request = GetKvRequest {
        key: "test-key".to_string(),
    };
    let response = metastore.get_kv(get_request).await.unwrap();
    assert_eq!(response.value, Some("updated-value".to_string()));
}

pub async fn test_metastore_kv_delete<MetastoreToTest: MetastoreService + DefaultForTest>() {
    let metastore = MetastoreToTest::default_for_test().await;

    // Set a key-value pair
    let set_request = SetKvRequest {
        key: "test-key".to_string(),
        value: "test-value".to_string(),
    };
    metastore.set_kv(set_request).await.unwrap();

    // Verify it exists
    let get_request = GetKvRequest {
        key: "test-key".to_string(),
    };
    let response = metastore.get_kv(get_request).await.unwrap();
    assert_eq!(response.value, Some("test-value".to_string()));

    // Delete the key
    let delete_request = DeleteKvRequest {
        key: "test-key".to_string(),
    };
    metastore.delete_kv(delete_request).await.unwrap();

    // Verify it no longer exists
    let get_request = GetKvRequest {
        key: "test-key".to_string(),
    };
    let response = metastore.get_kv(get_request).await.unwrap();
    assert_eq!(response.value, None);
}

pub async fn test_metastore_kv_delete_non_existent<
    MetastoreToTest: MetastoreService + DefaultForTest,
>() {
    let metastore = MetastoreToTest::default_for_test().await;

    // Delete a non-existent key (should succeed without error)
    let delete_request = DeleteKvRequest {
        key: "non-existent-key".to_string(),
    };
    metastore.delete_kv(delete_request).await.unwrap();
}

pub async fn test_metastore_kv_multiple_keys<MetastoreToTest: MetastoreService + DefaultForTest>() {
    let metastore = MetastoreToTest::default_for_test().await;

    // Set multiple key-value pairs
    let set_request_1 = SetKvRequest {
        key: "key-1".to_string(),
        value: "value-1".to_string(),
    };
    metastore.set_kv(set_request_1).await.unwrap();

    let set_request_2 = SetKvRequest {
        key: "key-2".to_string(),
        value: "value-2".to_string(),
    };
    metastore.set_kv(set_request_2).await.unwrap();

    let set_request_3 = SetKvRequest {
        key: "key-3".to_string(),
        value: "value-3".to_string(),
    };
    metastore.set_kv(set_request_3).await.unwrap();

    // Verify all keys exist
    let get_request_1 = GetKvRequest {
        key: "key-1".to_string(),
    };
    let response_1 = metastore.get_kv(get_request_1).await.unwrap();
    assert_eq!(response_1.value, Some("value-1".to_string()));

    let get_request_2 = GetKvRequest {
        key: "key-2".to_string(),
    };
    let response_2 = metastore.get_kv(get_request_2).await.unwrap();
    assert_eq!(response_2.value, Some("value-2".to_string()));

    let get_request_3 = GetKvRequest {
        key: "key-3".to_string(),
    };
    let response_3 = metastore.get_kv(get_request_3).await.unwrap();
    assert_eq!(response_3.value, Some("value-3".to_string()));

    // Delete one key
    let delete_request = DeleteKvRequest {
        key: "key-2".to_string(),
    };
    metastore.delete_kv(delete_request).await.unwrap();

    // Verify key-2 is deleted but others remain
    let get_request_1 = GetKvRequest {
        key: "key-1".to_string(),
    };
    let response_1 = metastore.get_kv(get_request_1).await.unwrap();
    assert_eq!(response_1.value, Some("value-1".to_string()));

    let get_request_2 = GetKvRequest {
        key: "key-2".to_string(),
    };
    let response_2 = metastore.get_kv(get_request_2).await.unwrap();
    assert_eq!(response_2.value, None);

    let get_request_3 = GetKvRequest {
        key: "key-3".to_string(),
    };
    let response_3 = metastore.get_kv(get_request_3).await.unwrap();
    assert_eq!(response_3.value, Some("value-3".to_string()));
}

pub async fn test_metastore_kv_empty_key<MetastoreToTest: MetastoreService + DefaultForTest>() {
    let metastore = MetastoreToTest::default_for_test().await;

    // Set a key-value pair with an empty key
    let set_request = SetKvRequest {
        key: "".to_string(),
        value: "empty-key-value".to_string(),
    };
    metastore.set_kv(set_request).await.unwrap();

    // Get the empty key
    let get_request = GetKvRequest {
        key: "".to_string(),
    };
    let response = metastore.get_kv(get_request).await.unwrap();
    assert_eq!(response.value, Some("empty-key-value".to_string()));

    // Delete the empty key
    let delete_request = DeleteKvRequest {
        key: "".to_string(),
    };
    metastore.delete_kv(delete_request).await.unwrap();

    // Verify it's deleted
    let get_request = GetKvRequest {
        key: "".to_string(),
    };
    let response = metastore.get_kv(get_request).await.unwrap();
    assert_eq!(response.value, None);
}

pub async fn test_metastore_kv_empty_value<MetastoreToTest: MetastoreService + DefaultForTest>() {
    let metastore = MetastoreToTest::default_for_test().await;

    // Set a key-value pair with an empty value
    let set_request = SetKvRequest {
        key: "test-key".to_string(),
        value: String::new(),
    };
    metastore.set_kv(set_request).await.unwrap();

    // Get the key with empty value
    let get_request = GetKvRequest {
        key: "test-key".to_string(),
    };
    let response = metastore.get_kv(get_request).await.unwrap();
    assert_eq!(response.value, Some(String::new()));
}
