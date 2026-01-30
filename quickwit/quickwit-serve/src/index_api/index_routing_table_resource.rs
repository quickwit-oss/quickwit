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

use quickwit_proto::metastore::{
    GetIndexRoutingTableRequest, IndexRoutingRule, MetastoreResult, MetastoreService,
    MetastoreServiceClient, SetIndexRoutingTableRequest,
};
use serde::{Deserialize, Serialize};
use warp::reject::Rejection;
use warp::{Filter, Reply};

use crate::format::extract_format_from_qs;
use crate::rest::recover_fn;
use crate::rest_api_response::into_rest_api_response;
use crate::with_arg;

/// Response for GET /api/v1/index-routing-table
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct IndexRoutingTableResponse {
    pub rules: Vec<IndexRoutingRuleEntry>,
}

/// Request body for PUT /api/v1/index-routing-table
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct IndexRoutingTableRequest {
    pub rules: Vec<IndexRoutingRuleEntry>,
}

/// A routing rule that maps a filter to an index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct IndexRoutingRuleEntry {
    /// The filter pattern (e.g., "*" for catch-all, "service:foo" for specific match)
    pub filter: String,
    /// The target index ID
    pub index_id: String,
}

impl From<IndexRoutingRule> for IndexRoutingRuleEntry {
    fn from(rule: IndexRoutingRule) -> Self {
        Self {
            filter: rule.filter,
            index_id: rule.index_id,
        }
    }
}

impl From<IndexRoutingRuleEntry> for IndexRoutingRule {
    fn from(rule: IndexRoutingRuleEntry) -> Self {
        Self {
            filter: rule.filter,
            index_id: rule.index_id,
        }
    }
}

pub fn index_routing_table_handlers(
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    get_index_routing_table_handler(metastore.clone())
        .or(set_index_routing_table_handler(metastore))
        .recover(recover_fn)
        .boxed()
}

fn get_index_routing_table_handler(
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    warp::path!("index-routing-table")
        .and(warp::get())
        .and(with_arg(metastore))
        .then(get_index_routing_table)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
}

#[utoipa::path(
    get,
    tag = "Index Routing Table",
    path = "/index-routing-table",
    responses(
        (status = 200, description = "The index routing table was successfully retrieved.", body = IndexRoutingTableResponse)
    ),
)]
/// Retrieves the current index routing table.
async fn get_index_routing_table(
    metastore: MetastoreServiceClient,
) -> MetastoreResult<IndexRoutingTableResponse> {
    let response = metastore
        .get_index_routing_table(GetIndexRoutingTableRequest {})
        .await?;

    let rules = response
        .rules
        .into_iter()
        .map(IndexRoutingRuleEntry::from)
        .collect();
    Ok(IndexRoutingTableResponse { rules })
}

fn set_index_routing_table_handler(
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    warp::path!("index-routing-table")
        .and(warp::put())
        .and(warp::body::json())
        .and(with_arg(metastore))
        .then(set_index_routing_table)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
}

#[utoipa::path(
    put,
    tag = "Index Routing Table",
    path = "/index-routing-table",
    request_body = IndexRoutingTableRequest,
    responses(
        (status = 200, description = "The index routing table was successfully updated.", body = IndexRoutingTableResponse)
    ),
)]
/// Sets the index routing table, replacing any existing rules.
async fn set_index_routing_table(
    request: IndexRoutingTableRequest,
    metastore: MetastoreServiceClient,
) -> MetastoreResult<IndexRoutingTableResponse> {
    let rules: Vec<IndexRoutingRule> = request.rules.iter().cloned().map(Into::into).collect();

    metastore
        .set_index_routing_table(SetIndexRoutingTableRequest { rules })
        .await?;

    // Return the rules that were set
    Ok(IndexRoutingTableResponse {
        rules: request.rules,
    })
}

#[cfg(test)]
mod tests {
    use quickwit_proto::metastore::{
        EmptyResponse, GetIndexRoutingTableResponse, MockMetastoreService,
    };

    use super::*;

    #[tokio::test]
    async fn test_get_index_routing_table() {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_get_index_routing_table()
            .return_once(|_| {
                Ok(GetIndexRoutingTableResponse {
                    rules: vec![
                        IndexRoutingRule {
                            filter: "*".to_string(),
                            index_id: "default-index".to_string(),
                        },
                        IndexRoutingRule {
                            filter: "service:foo".to_string(),
                            index_id: "foo-index".to_string(),
                        },
                    ],
                })
            });

        let metastore = MetastoreServiceClient::from_mock(mock_metastore);
        let handler = index_routing_table_handlers(metastore);

        let response = warp::test::request()
            .path("/index-routing-table")
            .method("GET")
            .reply(&handler)
            .await;

        assert_eq!(response.status(), 200);
        let body: IndexRoutingTableResponse = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(
            body.rules,
            vec![
                IndexRoutingRuleEntry {
                    filter: "*".to_string(),
                    index_id: "default-index".to_string(),
                },
                IndexRoutingRuleEntry {
                    filter: "service:foo".to_string(),
                    index_id: "foo-index".to_string(),
                },
            ]
        );
    }

    #[tokio::test]
    async fn test_set_index_routing_table() {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_set_index_routing_table()
            .return_once(|request| {
                assert_eq!(
                    request.rules,
                    vec![IndexRoutingRule {
                        filter: "*".to_string(),
                        index_id: "datadog".to_string(),
                    },]
                );
                Ok(EmptyResponse {})
            });

        let metastore = MetastoreServiceClient::from_mock(mock_metastore);
        let handler = index_routing_table_handlers(metastore);

        let request_body = IndexRoutingTableRequest {
            rules: vec![IndexRoutingRuleEntry {
                filter: "*".to_string(),
                index_id: "datadog".to_string(),
            }],
        };

        let response = warp::test::request()
            .path("/index-routing-table")
            .method("PUT")
            .header("content-type", "application/json")
            .json(&request_body)
            .reply(&handler)
            .await;

        assert_eq!(response.status(), 200);
    }
}
