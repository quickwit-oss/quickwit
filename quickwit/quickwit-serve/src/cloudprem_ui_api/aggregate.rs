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

//! This module implements the aggregate endpoint for the CloudPrem UI.

use std::sync::Arc;

use bytes::Bytes;
use quickwit_proto::search::{CountHits, SearchRequest, SearchResponse};
use quickwit_query::aggregations::{AggregationResult, BucketEntries, BucketResult, Key};
use quickwit_search::SearchService;
use tantivy::aggregation::agg_req::{
    Aggregation as TantivyAggregation, AggregationVariants as TantivyAggregationVariants,
};
use tantivy::aggregation::bucket::{
    CustomOrder, DateHistogramAggregationReq, HistogramBounds, Order, OrderTarget, TermsAggregation,
};
use tracing::debug;
use warp::Filter;
use warp::reject::Rejection;

use super::{
    CloudPremUiError, CloudPremUiResult, SortOrder, TantivyAggregationMap, Timeframe,
    try_into_aggregation_results, try_into_query_ast,
};
use crate::cloudprem::CLOUDPREM_INDEX_ID_PATTERN;
use crate::rest_api_response::into_rest_api_response;
use crate::{BodyFormat, with_arg};

#[derive(Debug, Default, serde::Deserialize, PartialEq)]
struct Aggregations(Vec<Aggregation>);

#[derive(Debug, serde::Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Aggregation {
    Timeseries(TimeseriesAggregation),
}

impl Aggregation {
    fn as_timeseries(&self) -> &TimeseriesAggregation {
        match self {
            Aggregation::Timeseries(timeseries_aggregation) => timeseries_aggregation,
        }
    }
}

#[derive(Debug, serde::Deserialize, PartialEq)]
struct TimeseriesAggregation {
    output: String,
    #[serde(rename = "interval")]
    interval_millis: u64,
}

#[derive(serde::Deserialize, Debug, PartialEq)]
struct AggregateRequest {
    #[serde(default)]
    query: String,
    #[serde(rename = "compute")]
    aggregations: Aggregations,
    #[serde(rename = "groupBy")]
    group_by_exps: Vec<GroupByExp>,
    #[serde(rename = "time")]
    timeframe: Timeframe,
}

#[derive(Debug, serde::Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum GroupByExp {
    Field(GroupByField),
}

#[derive(Debug, serde::Deserialize, PartialEq)]
struct GroupByField {
    id: String,
    output: String,
    #[serde(rename = "sort")]
    sort_by: SortBy,
    limit: u32,
    missing: Option<String>,
}

#[derive(Debug, serde::Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum SortBy {
    Metric(SortByMetric),
}

impl SortBy {
    fn try_into_tantivy_custom_order(self) -> CloudPremUiResult<CustomOrder> {
        let custom_order = match self {
            SortBy::Metric(sort_by) => CustomOrder {
                target: match sort_by.id.as_str() {
                    "count:count" => OrderTarget::Count,
                    _ => {
                        return Err(CloudPremUiError::Invalid(format!(
                            "unsupported metric: {}",
                            sort_by.id
                        )));
                    }
                },
                order: match sort_by.order {
                    SortOrder::Asc => Order::Asc,
                    SortOrder::Desc => Order::Desc,
                },
            },
        };
        Ok(custom_order)
    }
}

#[derive(Debug, serde::Deserialize, PartialEq)]
struct SortByMetric {
    id: String,
    order: SortOrder,
}

impl AggregateRequest {
    fn try_into_search_request(self) -> CloudPremUiResult<SearchRequest> {
        let start_timestamp = self.timeframe.from_timestamp_inclusive_millis;
        let end_timestamp = self.timeframe.to_timestamp_exclusive_millis;
        let query_ast =
            try_into_query_ast(&self.query, Some(start_timestamp), Some(end_timestamp))?;
        let query_ast_json = serde_json::to_string(&query_ast)?;
        let tantivy_aggregations =
            try_into_tantivy_aggregations(self.aggregations, self.group_by_exps, self.timeframe)?;
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
    aggregations: Aggregations,
    group_by_exps: Vec<GroupByExp>,
    timeframe: Timeframe,
) -> CloudPremUiResult<TantivyAggregationMap> {
    let mut tantivy_aggregations = TantivyAggregationMap::default();
    let histogram_bounds = HistogramBounds {
        min: timeframe.from_timestamp_inclusive_millis as f64,
        max: timeframe.to_timestamp_exclusive_millis as f64,
    };
    for agg in aggregations.0 {
        match agg {
            Aggregation::Timeseries(timeseries_aggregation) => {
                let date_histogram_aggregation = DateHistogramAggregationReq {
                    field: "timestamp".to_string(),
                    fixed_interval: Some(format!("{}ms", timeseries_aggregation.interval_millis)),
                    extended_bounds: Some(histogram_bounds),
                    ..Default::default()
                };
                let aggregation = TantivyAggregation {
                    agg: TantivyAggregationVariants::DateHistogram(date_histogram_aggregation),
                    sub_aggregation: tantivy_aggregations,
                };
                tantivy_aggregations =
                    std::iter::once((timeseries_aggregation.output, aggregation)).collect();
            }
        };
    }

    for exp in group_by_exps {
        match exp {
            GroupByExp::Field(field) => {
                let missing = field.missing.map(tantivy::aggregation::Key::Str);
                let terms_aggregation = TermsAggregation {
                    field: field.id,
                    size: Some(field.limit),
                    order: Some(field.sort_by.try_into_tantivy_custom_order()?),
                    missing,
                    ..Default::default()
                };
                let aggregation = TantivyAggregation {
                    agg: TantivyAggregationVariants::Terms(terms_aggregation),
                    sub_aggregation: tantivy_aggregations,
                };
                tantivy_aggregations = std::iter::once((field.output, aggregation)).collect();
            }
        };
    }
    Ok(tantivy_aggregations)
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct AggregateResponse {
    series: Vec<Series>,
    times: Vec<f64>,
    values: Vec<Vec<f64>>,
    from_date: i64,
    to_date: i64,
    interval: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct Series {
    query_index: u64,
    group_tags: Vec<String>,
}

impl AggregateResponse {
    fn from_search_response(
        search_response: SearchResponse,
        timeframe: Timeframe,
        interval_millis: u64,
    ) -> CloudPremUiResult<Self> {
        let mut series: Vec<Series> = Vec::new();
        let mut times: Vec<f64> = Vec::new();
        let mut values: Vec<Vec<f64>> = Vec::new();
        let aggregation_results =
            try_into_aggregation_results(search_response.aggregation_postcard)?;

        let Some((group_key, result)) = aggregation_results.0.into_iter().next() else {
            return Err(CloudPremUiError::Invalid(
                "no aggregation results found".to_string(),
            ));
        };
        match result {
            // terms results contain the series tags. (e.g. status:error)
            AggregationResult::BucketResult(BucketResult::Terms { buckets, .. }) => {
                for bucket in buckets {
                    let Key::Str(bucket_key) = bucket.key else {
                        return Err(CloudPremUiError::Invalid(format!(
                            "expected string key, got {:?}",
                            bucket.key
                        )));
                    };
                    series.push(Series {
                        query_index: 0,
                        group_tags: vec![format!("{}:{}", group_key, bucket_key)],
                    });
                    let Some((_, sub_result)) = bucket.sub_aggregation.0.into_iter().next() else {
                        return Err(CloudPremUiError::Invalid(
                            "no sub aggregation results found".to_string(),
                        ));
                    };
                    match sub_result {
                        // histogram results contain the times and values for each series
                        AggregationResult::BucketResult(BucketResult::Histogram { buckets }) => {
                            let mut series_values = Vec::new();
                            let mut series_times = Vec::new();
                            let BucketEntries::Vec(buckets) = buckets else {
                                return Err(CloudPremUiError::Invalid(format!(
                                    "expected vector buckets, got {:?}",
                                    buckets
                                )));
                            };
                            for bucket in buckets {
                                let Key::F64(time) = bucket.key else {
                                    return Err(CloudPremUiError::Invalid(format!(
                                        "expected f64 key, got {:?}",
                                        bucket.key
                                    )));
                                };
                                series_times.push(time);
                                series_values.push(bucket.doc_count as f64);
                            }
                            values.push(series_values);
                            if times.is_empty() || times == series_times {
                                times = series_times;
                            } else {
                                return Err(CloudPremUiError::Internal(
                                    "times in different series are not equal".to_string().into(),
                                ));
                            }
                        }
                        _ => {
                            return Err(CloudPremUiError::Invalid(format!(
                                "unsupported aggregation result type: {:?}",
                                sub_result
                            )));
                        }
                    };
                }
            }
            _ => {
                return Err(CloudPremUiError::Invalid(format!(
                    "unsupported aggregation result type: {:?}",
                    result
                )));
            }
        };

        let from_date = timeframe.from_timestamp_inclusive_millis;
        let to_date = timeframe.to_timestamp_exclusive_millis;
        let interval = interval_millis;
        Ok(AggregateResponse {
            series,
            times,
            values,
            from_date,
            to_date,
            interval,
        })
    }
}

pub(crate) fn aggregate_handler(
    search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("cloudprem" / "api" / "v1" / "aggregate")
        .and(warp::post())
        .and(warp::body::bytes())
        .and(with_arg(search_service))
        .then(cloudprem_ui_aggregate)
        .then(|response_result| {
            futures::future::ready(into_rest_api_response(response_result, BodyFormat::Json))
        })
}

async fn cloudprem_ui_aggregate(
    body: Bytes,
    search_service: Arc<dyn SearchService>,
) -> CloudPremUiResult<AggregateResponse> {
    debug!(?body, "received aggregate request");
    let aggregate_request: AggregateRequest = serde_json::from_slice(&body)?;
    if aggregate_request.aggregations.0.is_empty() {
        return Err(CloudPremUiError::Invalid(
            "compute must have at least one aggregation".to_string(),
        ));
    }
    if aggregate_request.group_by_exps.is_empty() {
        return Err(CloudPremUiError::Invalid(
            "groupBy must have at least one expression".to_string(),
        ));
    }
    let timeframe = aggregate_request.timeframe;
    let interval_millis = aggregate_request.aggregations.0[0]
        .as_timeseries()
        .interval_millis;
    let search_request = aggregate_request.try_into_search_request()?;
    let search_response = search_service.root_search(search_request).await?;
    let aggregate_response =
        AggregateResponse::from_search_response(search_response, timeframe, interval_millis)?;
    Ok(aggregate_response)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_query::aggregations::{AggregationResults, BucketEntry};
    use quickwit_search::MockSearchService;

    use super::*;

    #[test]
    fn test_parse_aggregate_body() {
        let aggregate_body_json = r#"
        {
            "query": "service:quickwit",
            "compute": [
                {
                    "timeseries": {
                        "metric": "count",
                        "output": "count:count",
                        "aggregation": "count",
                        "interval": 10000
                    }
                }
            ],
            "time": {
                "from_ts": 1759395985200,
                "to_ts": 1759396885200
            },
            "groupBy": [
                {
                    "field": {
                        "id": "status",
                        "output": "status",
                        "sort": {
                            "metric": {
                                "id": "count:count",
                                "order": "desc"
                            }
                        },
                        "limit": 10
                    }
                }
            ],
            "executionInfo": {},
            "calculatedFields": [],
            "_authentication_token": "fd9e3bd2230f7435424d0d263f0b8500d82bb099"
        }
        "#;
        let aggregate_request: AggregateRequest =
            serde_json::from_str(aggregate_body_json).unwrap();

        let expected = AggregateRequest {
            query: "service:quickwit".to_string(),
            aggregations: Aggregations(vec![Aggregation::Timeseries(TimeseriesAggregation {
                output: "count:count".to_string(),
                interval_millis: 10000,
            })]),
            group_by_exps: vec![GroupByExp::Field(GroupByField {
                id: "status".to_string(),
                output: "status".to_string(),
                sort_by: SortBy::Metric(SortByMetric {
                    id: "count:count".to_string(),
                    order: SortOrder::Desc,
                }),
                limit: 10,
                missing: None,
            })],
            timeframe: Timeframe {
                from_timestamp_inclusive_millis: 1759395985200,
                to_timestamp_exclusive_millis: 1759396885200,
            },
        };
        assert_eq!(aggregate_request, expected);
    }

    #[tokio::test]
    async fn test_cloudprem_ui_aggregate() {
        let aggregate_body_json = r#"
        {
            "query": "service:quickwit",
            "compute": [
                {
                    "timeseries": {
                        "metric": "count",
                        "output": "count:count",
                        "aggregation": "count",
                        "interval": 10000
                    }
                }
            ],
            "time": {
                "from_ts": 1759395985200,
                "to_ts": 1759396885200
            },
            "groupBy": [
                {
                    "field": {
                        "id": "status",
                        "output": "status",
                        "sort": {
                            "metric": {
                                "id": "count:count",
                                "order": "desc"
                            }
                        },
                        "limit": 10
                    }
                }
            ],
            "executionInfo": {},
            "calculatedFields": [],
            "_authentication_token": "fd9e3bd2230f7435424d0d263f0b8500d82bb099"
        }
        "#;

        let histogram_buckets = vec![
            BucketEntry {
                key_as_string: None,
                key: Key::F64(1759395985200.0),
                doc_count: 10,
                sub_aggregation: AggregationResults(vec![]),
            },
            BucketEntry {
                key_as_string: None,
                key: Key::F64(1759395995200.0),
                doc_count: 15,
                sub_aggregation: AggregationResults(vec![]),
            },
            BucketEntry {
                key_as_string: None,
                key: Key::F64(1759396005200.0),
                doc_count: 20,
                sub_aggregation: AggregationResults(vec![]),
            },
        ];

        let histogram_result = AggregationResult::BucketResult(BucketResult::Histogram {
            buckets: BucketEntries::Vec(histogram_buckets),
        });

        let terms_buckets = vec![BucketEntry {
            key_as_string: None,
            key: Key::Str("error".to_string()),
            doc_count: 45,
            sub_aggregation: AggregationResults(vec![(
                "count:count".to_string(),
                histogram_result.clone(),
            )]),
        }];

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
                    r#"{"type":"bool","must":[{"type":"full_text","field":"service","text":"quickwit","params":{"mode":{"type":"phrase_fallback_to_intersection"}},"lenient":false},{"type":"range","field":"timestamp","lower_bound":{"Included":1759395985200},"upper_bound":{"Excluded":1759396885200}}]}"#
                );
                assert_eq!(
                    search_request.aggregation_request,
                    Some(r#"{"status":{"terms":{"field":"status","size":10,"order":{"_count":"desc"}},"aggs":{"count:count":{"date_histogram":{"interval":null,"calendar_interval":null,"field":"timestamp","format":null,"fixed_interval":"10000ms","offset":null,"min_doc_count":null,"hard_bounds":null,"extended_bounds":{"min":1759395985200.0,"max":1759396885200.0},"keyed":false}}}}}"#.to_string())
                );
                Ok(search_response.clone())
            });
        let bound_cloudprem_ui_aggregate_handler = aggregate_handler(Arc::new(search_service));

        let response = warp::test::request()
            .path("/cloudprem/api/v1/aggregate")
            .body(aggregate_body_json)
            .method("POST")
            .reply(&bound_cloudprem_ui_aggregate_handler)
            .await;

        assert_eq!(response.status(), 200);

        let body = response.body();
        let aggregate_response: AggregateResponse = serde_json::from_slice(body).unwrap();
        assert_eq!(
            aggregate_response.series[0].group_tags,
            vec!["status:error"]
        );
        assert_eq!(
            aggregate_response.times,
            vec![1759395985200.0, 1759395995200.0, 1759396005200.0]
        );
        assert_eq!(aggregate_response.values[0], vec![10.0, 15.0, 20.0]);
        assert_eq!(aggregate_response.from_date, 1759395985200);
        assert_eq!(aggregate_response.to_date, 1759396885200);
        assert_eq!(aggregate_response.interval, 10000);
    }

    #[tokio::test]
    async fn test_cloudprem_ui_aggregate_empty_compute() {
        let aggregate_body_json = r#"
        {
            "query": "service:quickwit",
            "compute": [],
            "time": {
                "from_ts": 1759395985200,
                "to_ts": 1759396885200
            },
            "groupBy": [
                {
                    "field": {
                        "id": "status",
                        "output": "status",
                        "sort": {
                            "metric": {
                                "id": "count:count",
                                "order": "desc"
                            }
                        },
                        "limit": 10
                    }
                }
            ],
            "executionInfo": {},
            "calculatedFields": [],
            "_authentication_token": "fd9e3bd2230f7435424d0d263f0b8500d82bb099"
        }
        "#;

        let search_service = MockSearchService::new();
        let bound_cloudprem_ui_aggregate_handler = aggregate_handler(Arc::new(search_service));
        let response = warp::test::request()
            .path("/cloudprem/api/v1/aggregate")
            .body(aggregate_body_json)
            .method("POST")
            .reply(&bound_cloudprem_ui_aggregate_handler)
            .await;

        let body = response.body();
        let error_response: serde_json::Value = serde_json::from_slice(body).unwrap();
        let error_message = error_response["message"].as_str().unwrap();
        assert_eq!(
            error_message,
            "invalid argument: compute must have at least one aggregation"
        );
        assert_eq!(response.status(), 400);
    }

    #[tokio::test]
    async fn test_cloudprem_ui_aggregate_invalid_compute() {
        let aggregate_body_json = r#"
        {
            "query": "service:quickwit",
            "compute": [
                {
                    "timeseries": {
                        "metric": "count",
                        "output": "count:count",
                        "aggregation": "count"
                    }
                }
            ],
            "time": {
                "from_ts": 1759395985200,
                "to_ts": 1759396885200
            },
            "groupBy": [
                {
                    "field": {
                        "id": "status",
                        "output": "status",
                        "sort": {
                            "metric": {
                                "id": "count:count",
                                "order": "desc"
                            }
                        },
                        "limit": 10
                    }
                }
            ],
            "executionInfo": {},
            "calculatedFields": [],
            "_authentication_token": "fd9e3bd2230f7435424d0d263f0b8500d82bb099"
        }
        "#;

        let search_service = MockSearchService::new();
        let bound_cloudprem_ui_aggregate_handler = aggregate_handler(Arc::new(search_service));
        let response = warp::test::request()
            .path("/cloudprem/api/v1/aggregate")
            .body(aggregate_body_json)
            .method("POST")
            .reply(&bound_cloudprem_ui_aggregate_handler)
            .await;

        assert_eq!(response.status(), 400);
    }

    #[tokio::test]
    async fn test_cloudprem_ui_aggregate_empty_group_by() {
        let aggregate_body_json = r#"
        {
            "query": "service:quickwit",
            "compute": [
                {
                    "timeseries": {
                        "metric": "count",
                        "output": "count:count",
                        "aggregation": "count",
                        "interval": 10000
                    }
                }
            ],
            "time": {
                "from_ts": 1759395985200,
                "to_ts": 1759396885200
            },
            "groupBy": [],
            "executionInfo": {},
            "calculatedFields": [],
            "_authentication_token": "fd9e3bd2230f7435424d0d263f0b8500d82bb099"
        }
        "#;

        let search_service = MockSearchService::new();
        let bound_cloudprem_ui_aggregate_handler = aggregate_handler(Arc::new(search_service));
        let response = warp::test::request()
            .path("/cloudprem/api/v1/aggregate")
            .body(aggregate_body_json)
            .method("POST")
            .reply(&bound_cloudprem_ui_aggregate_handler)
            .await;

        let body = response.body();
        let error_response: serde_json::Value = serde_json::from_slice(body).unwrap();
        let error_message = error_response["message"].as_str().unwrap();
        assert_eq!(
            error_message,
            "invalid argument: groupBy must have at least one expression"
        );
        assert_eq!(response.status(), 400);
    }
}
