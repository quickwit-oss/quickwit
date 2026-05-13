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

//! This module implements the facet_info endpoint for the CloudPrem UI.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use quickwit_proto::search::{CountHits, SearchRequest, SearchResponse};
use quickwit_query::aggregations::{AggregationResult, BucketResult, Key};
use quickwit_search::SearchService;
use tantivy::aggregation::agg_req::{
    Aggregation as TantivyAggregation, AggregationVariants as TantivyAggregationVariants,
};
use tantivy::aggregation::bucket::{CustomOrder, Order, OrderTarget, TermsAggregation};
use tracing::debug;
use warp::Filter;
use warp::reject::Rejection;

use super::{
    CloudPremUiError, CloudPremUiResult, TantivyAggregationMap, Timeframe,
    try_into_aggregation_results, try_into_query_ast,
};
use crate::cloudprem::CLOUDPREM_INDEX_ID_PATTERN;
use crate::rest_api_response::into_rest_api_response;
use crate::{BodyFormat, with_arg};

#[derive(serde::Deserialize)]
struct FacetInfoRequest {
    #[serde(default)]
    query: String,
    #[serde(rename = "time")]
    timeframe: Timeframe,
    #[serde(rename = "path")]
    facet_group: String,
    limit: u32,
}

impl FacetInfoRequest {
    fn try_into_search_request(self) -> CloudPremUiResult<SearchRequest> {
        let start_timestamp = self.timeframe.from_timestamp_inclusive_millis;
        let end_timestamp = self.timeframe.to_timestamp_exclusive_millis;
        let query_ast =
            try_into_query_ast(&self.query, Some(start_timestamp), Some(end_timestamp))?;
        let query_ast_json = serde_json::to_string(&query_ast)?;
        let tantivy_aggregations = try_into_tantivy_aggregations(self.facet_group, self.limit)?;
        let tantivy_aggregations_json = serde_json::to_string(&tantivy_aggregations)?;
        let search_request = SearchRequest {
            index_id_patterns: vec![CLOUDPREM_INDEX_ID_PATTERN.to_string()],
            query_ast: query_ast_json,
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 0,
            start_offset: 0,
            aggregation_request: Some(tantivy_aggregations_json),
            sort_fields: Vec::new(),
            scroll_ttl_secs: None,
            search_after: None,
            snippet_fields: Vec::new(),
            count_hits: CountHits::CountAll as i32,
            ignore_missing_indexes: false,
            skip_aggregation_finalization: false,
            enable_request_batching: false,
        };
        Ok(search_request)
    }
}

fn try_into_tantivy_aggregations(
    group: String,
    limit: u32,
) -> CloudPremUiResult<HashMap<String, TantivyAggregation>> {
    let custom_order = CustomOrder {
        target: OrderTarget::Count,
        order: Order::Desc,
    };
    let terms_aggregation = TermsAggregation {
        field: group.clone(),
        size: Some(limit),
        order: Some(custom_order),
        missing: None,
        ..Default::default()
    };
    let aggregation = TantivyAggregation {
        agg: TantivyAggregationVariants::Terms(terms_aggregation),
        sub_aggregation: TantivyAggregationMap::default(),
    };
    let tantivy_aggregations = HashMap::from([(group, aggregation)]);
    Ok(tantivy_aggregations)
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct FacetInfoResponse {
    hit_count: u64,
    result: FacetResult,
}

impl FacetInfoResponse {
    fn try_from_search_response(search_response: SearchResponse) -> CloudPremUiResult<Self> {
        let mut facet_fields = Vec::new();
        let aggregation_results =
            try_into_aggregation_results(search_response.aggregation_postcard)?;

        let Some((_, result)) = aggregation_results.0.into_iter().next() else {
            return Err(CloudPremUiError::Invalid(
                "no aggregation results found".to_string(),
            ));
        };
        // aggregation should only contain one terms result. each term is a facet field
        let AggregationResult::BucketResult(BucketResult::Terms { buckets, .. }) = result else {
            return Err(CloudPremUiError::Invalid(format!(
                "unsupported aggregation result type: {:?}",
                result
            )));
        };
        for bucket in buckets {
            let Key::Str(bucket_key) = bucket.key else {
                return Err(CloudPremUiError::Invalid(format!(
                    "expected string key, got {:?}",
                    bucket.key
                )));
            };
            facet_fields.push(FacetField {
                field: bucket_key,
                value: bucket.doc_count,
            });
        }
        Ok(FacetInfoResponse {
            hit_count: search_response.num_hits,
            result: FacetResult {
                fields: facet_fields,
            },
        })
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct FacetResult {
    fields: Vec<FacetField>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
struct FacetField {
    field: String,
    value: u64,
}

pub(crate) fn facet_info_handler(
    search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("cloudprem" / "api" / "v1" / "facet_info")
        .and(warp::post())
        .and(warp::body::bytes())
        .and(with_arg(search_service))
        .then(cloudprem_ui_facet_info)
        .then(|response_result| {
            futures::future::ready(into_rest_api_response(response_result, BodyFormat::Json))
        })
}

#[utoipa::path(
    post,
    tag = "CloudPrem Search",
    path = "/cloudprem/api/v1/facet_info",
    request_body = CloudPremUiRequest,
    responses(
        (status = 200, description = "The search request was successfully executed.", body = CloudPremUiResponse)
    ),
)]
/// Executes a facets request issued by the CloudPrem UI.
async fn cloudprem_ui_facet_info(
    body: Bytes,
    search_service: Arc<dyn SearchService>,
) -> CloudPremUiResult<FacetInfoResponse> {
    debug!(?body, "received facet info request");
    let facet_info_request: FacetInfoRequest = serde_json::from_slice(&body)?;
    if facet_info_request.facet_group.is_empty() {
        return Err(CloudPremUiError::Invalid(
            "path cannot be empty".to_string(),
        ));
    }
    let search_request = facet_info_request.try_into_search_request()?;
    let search_response = search_service.root_search(search_request).await?;
    let facet_info_response = FacetInfoResponse::try_from_search_response(search_response)?;
    Ok(facet_info_response)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_query::aggregations::{AggregationResults, BucketEntry};
    use quickwit_search::MockSearchService;

    use super::*;

    #[tokio::test]
    async fn test_cloudprem_ui_facet_info() {
        let facet_info_body_json = r#"
        {
            "query": "",
            "path": "status",
            "limit": 50,
            "time": {
                "from_ts": 1763503244491,
                "to_ts": 1763504144491
            }
        }
        "#;

        let terms_buckets = vec![
            BucketEntry {
                key_as_string: None,
                key: Key::Str("info".to_string()),
                doc_count: 45,
                sub_aggregation: AggregationResults(vec![]),
            },
            BucketEntry {
                key_as_string: None,
                key: Key::Str("warn".to_string()),
                doc_count: 30,
                sub_aggregation: AggregationResults(vec![]),
            },
            BucketEntry {
                key_as_string: None,
                key: Key::Str("error".to_string()),
                doc_count: 15,
                sub_aggregation: AggregationResults(vec![]),
            },
        ];

        let terms_result = AggregationResult::BucketResult(BucketResult::Terms {
            buckets: terms_buckets,
            sum_other_doc_count: 0,
            doc_count_error_upper_bound: Some(0),
        });

        let aggregation_results = AggregationResults(vec![("status".to_string(), terms_result)]);
        let aggregation_postcard = postcard::to_allocvec(&aggregation_results).unwrap();

        let search_response = SearchResponse {
            hits: Vec::new(),
            num_hits: 1,
            elapsed_time_micros: 100,
            errors: Vec::new(),
            scroll_id: None,
            aggregation_postcard: Some(aggregation_postcard),
            failed_splits: Vec::new(),
            num_successful_splits: 0,
        };

        let mut search_service = MockSearchService::new();
        search_service
            .expect_root_search()
            .returning(move |search_request| {
                assert_eq!(search_request.max_hits, 0);
                assert_eq!(
                    search_request.query_ast,
                    r#"{"type":"bool","must":[{"type":"bool"},{"type":"range","field":"timestamp","lower_bound":{"Included":1763503244491},"upper_bound":{"Excluded":1763504144491}}]}"#
                );
                assert_eq!(
                    search_request.aggregation_request,
                    Some(r#"{"status":{"terms":{"field":"status","size":50,"order":{"_count":"desc"}}}}"#.to_string())
                );
                Ok(search_response.clone())
            });
        let bound_cloudprem_ui_facet_info_handler = facet_info_handler(Arc::new(search_service));

        let response = warp::test::request()
            .path("/cloudprem/api/v1/facet_info")
            .body(facet_info_body_json)
            .method("POST")
            .reply(&bound_cloudprem_ui_facet_info_handler)
            .await;

        assert_eq!(response.status(), 200);

        let body = response.body();
        let facet_info_response: FacetInfoResponse = serde_json::from_slice(body).unwrap();
        assert_eq!(
            facet_info_response.result.fields,
            vec![
                FacetField {
                    field: "info".to_string(),
                    value: 45
                },
                FacetField {
                    field: "warn".to_string(),
                    value: 30
                },
                FacetField {
                    field: "error".to_string(),
                    value: 15
                },
            ]
        );
    }

    #[tokio::test]
    async fn test_cloudprem_ui_facet_info_empty_path() {
        let facet_info_body_json = r#"
        {
            "query": "",
            "path": "",
            "limit": 50,
            "time": {
                "from_ts": 1763503244491,
                "to_ts": 1763504144491
            }
        }
        "#;

        let search_service = MockSearchService::new();
        let bound_cloudprem_ui_facet_info_handler = facet_info_handler(Arc::new(search_service));
        let response = warp::test::request()
            .path("/cloudprem/api/v1/facet_info")
            .body(facet_info_body_json)
            .method("POST")
            .reply(&bound_cloudprem_ui_facet_info_handler)
            .await;

        let body = response.body();
        let error_response: serde_json::Value = serde_json::from_slice(body).unwrap();
        let error_message = error_response["message"].as_str().unwrap();
        assert_eq!(error_message, "invalid argument: path cannot be empty");
        assert_eq!(response.status(), 400);
    }
}
