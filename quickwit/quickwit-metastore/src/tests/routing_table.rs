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

// Index Routing Table API tests
//
//  - get_index_routing_table
//  - set_index_routing_table

use quickwit_proto::metastore::{
    GetIndexRoutingTableRequest, IndexRoutingRule, MetastoreService, SetIndexRoutingTableRequest,
};

use super::DefaultForTest;
use crate::MetastoreServiceExt;

pub async fn test_metastore_get_routing_table_empty<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let metastore = MetastoreToTest::default_for_test().await;

    let response = metastore
        .get_index_routing_table(GetIndexRoutingTableRequest {})
        .await
        .unwrap();

    assert!(
        response.rules.is_empty(),
        "Expected empty routing table on fresh metastore"
    );
}

pub async fn test_metastore_set_and_get_routing_table<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let metastore = MetastoreToTest::default_for_test().await;

    let rules = vec![IndexRoutingRule {
        filter: "*".to_string(),
        index_id: "default-index".to_string(),
    }];

    metastore
        .set_index_routing_table(SetIndexRoutingTableRequest {
            rules: rules.clone(),
        })
        .await
        .unwrap();

    let response = metastore
        .get_index_routing_table(GetIndexRoutingTableRequest {})
        .await
        .unwrap();

    assert_eq!(
        response.rules, rules,
        "Routing table should match what was set"
    );
}

pub async fn test_metastore_set_routing_table_overwrites<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let metastore = MetastoreToTest::default_for_test().await;

    let rules_a = vec![IndexRoutingRule {
        filter: "*".to_string(),
        index_id: "index-a".to_string(),
    }];

    let rules_b = vec![
        IndexRoutingRule {
            filter: "service:web".to_string(),
            index_id: "web-index".to_string(),
        },
        IndexRoutingRule {
            filter: "*".to_string(),
            index_id: "index-b".to_string(),
        },
    ];

    // Set first routing table
    metastore
        .set_index_routing_table(SetIndexRoutingTableRequest { rules: rules_a })
        .await
        .unwrap();

    // Overwrite with second routing table
    metastore
        .set_index_routing_table(SetIndexRoutingTableRequest {
            rules: rules_b.clone(),
        })
        .await
        .unwrap();

    let response = metastore
        .get_index_routing_table(GetIndexRoutingTableRequest {})
        .await
        .unwrap();

    assert_eq!(
        response.rules, rules_b,
        "Routing table should be overwritten with the latest value"
    );
}
