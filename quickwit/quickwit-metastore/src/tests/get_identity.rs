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

// Index API tests
//
//  - create_index
//  - index_exists
//  - index_metadata
//  - list_indexes
//  - delete_index

use quickwit_proto::metastore::{GetClusterIdentityRequest, MetastoreService};
use uuid::Uuid;

use super::DefaultForTest;
use crate::MetastoreServiceExt;

pub async fn test_metastore_get_identity<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let metastore = MetastoreToTest::default_for_test().await;

    let identity_1 = metastore
        .get_cluster_identity(GetClusterIdentityRequest {})
        .await
        .unwrap()
        .uuid;

    let identity_2 = metastore
        .get_cluster_identity(GetClusterIdentityRequest {})
        .await
        .unwrap()
        .uuid;

    assert_eq!(identity_1, identity_2);
    assert_ne!(identity_1, Uuid::nil().hyphenated().to_string());
}
