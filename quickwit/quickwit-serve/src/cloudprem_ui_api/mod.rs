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

//! This module implements the HTTP API for the CloudPrem UI.

use std::ops::Bound;
use std::sync::Arc;

use quickwit_proto::{ServiceError, ServiceErrorCode};
use quickwit_query::JsonLiteral;
use quickwit_query::aggregations::AggregationResults;
use quickwit_query::query_ast::{BoolQuery, QueryAst, RangeQuery, query_ast_from_user_text};
use quickwit_search::{SearchError, SearchService};
use warp::reject::Rejection;
use warp::{Filter, Reply};

use crate::rest::recover_fn;

mod aggregate;
mod facet_info;
mod search;

pub(crate) use aggregate::aggregate_handler;
pub(crate) use facet_info::facet_info_handler;
pub(crate) use search::search_handler;
use tantivy::aggregation::agg_req::Aggregations as TantivyAggregationMap;

type CloudPremUiResult<T> = std::result::Result<T, CloudPremUiError>;

#[derive(Debug, thiserror::Error)]
enum CloudPremUiError {
    #[error("internal error: {0}")]
    Internal(Box<dyn std::error::Error>),
    #[error("invalid argument: {0}")]
    Invalid(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Search(#[from] SearchError),
}

impl ServiceError for CloudPremUiError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::Internal(_) => ServiceErrorCode::Internal,
            Self::Invalid(_) => ServiceErrorCode::BadRequest,
            Self::Json(_) => ServiceErrorCode::BadRequest,
            Self::Search(error) => error.error_code(),
        }
    }
}

#[derive(Debug, serde::Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SortOrder {
    #[serde(alias = "ASC", alias = "ascending")]
    Asc,
    #[serde(alias = "DESC", alias = "descending")]
    Desc,
}

#[derive(serde::Deserialize, Debug, Clone, Copy, PartialEq)]
struct Timeframe {
    #[serde(rename = "from_ts")]
    from_timestamp_inclusive_millis: i64,
    #[serde(rename = "to_ts")]
    to_timestamp_exclusive_millis: i64,
}

fn try_into_query_ast(
    query: &str,
    from_timestamp_inclusive_millis: Option<i64>,
    to_timestamp_exclusive_millis: Option<i64>,
) -> CloudPremUiResult<QueryAst> {
    let query_ast = query_ast_from_user_text(
        query,
        Some(vec!["message".to_string(), "error".to_string()]),
    );

    let bool_query = BoolQuery {
        must: vec![
            query_ast,
            RangeQuery {
                field: "timestamp".to_string(),
                lower_bound: Bound::Included(JsonLiteral::Number(
                    from_timestamp_inclusive_millis
                        .ok_or_else(|| {
                            CloudPremUiError::Invalid(
                                "from timestamp should be present".to_string(),
                            )
                        })?
                        .into(),
                )),
                upper_bound: Bound::Excluded(JsonLiteral::Number(
                    to_timestamp_exclusive_millis
                        .ok_or_else(|| {
                            CloudPremUiError::Invalid("to timestamp should be present".to_string())
                        })?
                        .into(),
                )),
            }
            .into(),
        ],
        must_not: Vec::new(),
        should: Vec::new(),
        filter: Vec::new(),
        minimum_should_match: None,
    };

    let query_ast = QueryAst::Bool(bool_query);
    Ok(query_ast)
}

fn try_into_aggregation_results(
    aggregation_postcard: Option<Vec<u8>>,
) -> CloudPremUiResult<AggregationResults> {
    let aggregation_postcard_bytes: Vec<u8> = aggregation_postcard.ok_or_else(|| {
        CloudPremUiError::Internal("request generated no aggregation result".to_string().into())
    })?;
    let aggregation_result: AggregationResults = postcard::from_bytes(&aggregation_postcard_bytes)
        .map_err(|err| {
            CloudPremUiError::Internal(format!("failed to deserialize agg result: {err}").into())
        })?;
    Ok(aggregation_result)
}

pub(crate) fn cloudprem_ui_api_handlers(
    search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    aggregate_handler(search_service.clone())
        .or(search_handler(search_service.clone()))
        .or(facet_info_handler(search_service))
        .recover(recover_fn)
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_try_into_query_ast() {
        let query_ast =
            try_into_query_ast("error", Some(1759325269270), Some(1759326169270)).unwrap();
        let query_ast_json = serde_json::to_string(&query_ast).unwrap();
        assert_eq!(
            query_ast_json,
            r#"{"type":"bool","must":[{"type":"user_input","user_text":"error","default_fields":["message","error"],"default_operator":"And","lenient":false},{"type":"range","field":"timestamp","lower_bound":{"Included":1759325269270},"upper_bound":{"Excluded":1759326169270}}]}"#
        );
    }

    #[tokio::test]
    async fn test_try_into_query_ast_invalid_input() {
        let result = try_into_query_ast("error", None, Some(1759326169270))
            .unwrap_err()
            .to_string();
        assert_eq!(result, "invalid argument: from timestamp should be present");

        let result = try_into_query_ast("error", Some(1759325269270), None)
            .unwrap_err()
            .to_string();
        assert_eq!(result, "invalid argument: to timestamp should be present");
    }
}
