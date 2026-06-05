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

use std::convert::Infallible;
use std::ops::Bound;
use std::sync::Arc;

use quickwit_proto::{ServiceError, ServiceErrorCode};
use quickwit_query::JsonLiteral;
use quickwit_query::aggregations::AggregationResults;
use quickwit_query::query_ast::{
    BoolQuery, FieldPresenceQuery, FullTextQuery, PhrasePrefixQuery, QueryAst, QueryAstTransformer,
    RangeQuery, RegexQuery, TermQuery, WildcardQuery, query_ast_from_user_text,
};
use quickwit_search::{SearchError, SearchService};
use warp::reject::Rejection;
use warp::{Filter, Reply};

use crate::rest::recover_fn;
use quickwit_query::cloudprem::TraceIdQueryRewriter;

mod aggregate;
mod facet_info;
mod search;

pub(crate) use aggregate::aggregate_handler;
pub(crate) use facet_info::facet_info_handler;
pub(crate) use search::search_handler;
use tantivy::aggregation::agg_req::Aggregations as TantivyAggregationMap;

type CloudPremUiResult<T> = std::result::Result<T, CloudPremUiError>;

const KNOWN_FIELDS: &[&str] = &[
    "status",
    "service",
    "service_type",
    "host",
    "trace_id",
    "span_id",
    "custom",
    "source",
    "error",
    "message",
];

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

struct FieldRemapper;

impl QueryAstTransformer for FieldRemapper {
    type Err = Infallible;
    fn transform_term(&mut self, mut term_query: TermQuery) -> Result<Option<QueryAst>, Self::Err> {
        term_query.field = try_remap_field(term_query.field);
        Ok(Some(term_query.into()))
    }
    fn transform_full_text(
        &mut self,
        mut full_text: FullTextQuery,
    ) -> Result<Option<QueryAst>, Self::Err> {
        full_text.field = try_remap_field(full_text.field);
        Ok(Some(full_text.into()))
    }
    fn transform_phrase_prefix(
        &mut self,
        mut phrase_query: PhrasePrefixQuery,
    ) -> Result<Option<QueryAst>, Self::Err> {
        phrase_query.field = try_remap_field(phrase_query.field);
        Ok(Some(phrase_query.into()))
    }
    fn transform_range(
        &mut self,
        mut range_query: RangeQuery,
    ) -> Result<Option<QueryAst>, Self::Err> {
        range_query.field = try_remap_field(range_query.field);
        Ok(Some(range_query.into()))
    }
    fn transform_exists(
        &mut self,
        mut exists_query: FieldPresenceQuery,
    ) -> Result<Option<QueryAst>, Self::Err> {
        exists_query.field = try_remap_field(exists_query.field);
        Ok(Some(exists_query.into()))
    }
    fn transform_wildcard(
        &mut self,
        mut wildcard_query: WildcardQuery,
    ) -> Result<Option<QueryAst>, Self::Err> {
        wildcard_query.field = try_remap_field(wildcard_query.field);
        Ok(Some(wildcard_query.into()))
    }
    fn transform_regex(
        &mut self,
        mut regex_query: RegexQuery,
    ) -> Result<Option<QueryAst>, Self::Err> {
        regex_query.field = try_remap_field(regex_query.field);
        Ok(Some(regex_query.into()))
    }
}

fn try_remap_field(field: String) -> String {
    if let Some(stripped_field) = field.strip_prefix("@") {
        format!("custom.{}", stripped_field)
    } else if !KNOWN_FIELDS.contains(&field.as_str()) {
        format!("tag.{}", field)
    } else {
        field
    }
    // TODO: maybe add support for *:value -> all: value
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
    let Ok(query_ast) = query_ast.parse_user_query(&["message".to_string(), "error".to_string()])
    else {
        return Err(CloudPremUiError::Internal(
            "failed to parse user query".to_string().into(),
        ));
    };

    let Ok(Some(query_ast)) = FieldRemapper.transform(query_ast) else {
        unreachable!()
    };
    let Ok(Some(query_ast)) = TraceIdQueryRewriter.transform(query_ast) else {
        unreachable!()
    };

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
            r#"{"type":"bool","must":[{"type":"bool","should":[{"type":"full_text","field":"message","text":"error","params":{"mode":{"type":"phrase_fallback_to_intersection"}},"lenient":false},{"type":"full_text","field":"error","text":"error","params":{"mode":{"type":"phrase_fallback_to_intersection"}},"lenient":false}]},{"type":"range","field":"timestamp","lower_bound":{"Included":1759325269270},"upper_bound":{"Excluded":1759326169270}}]}"#
        );
    }

    #[tokio::test]
    async fn test_try_into_query_ast_field_remap() {
        let query_ast =
            try_into_query_ast("@@agent:core", Some(1759325269270), Some(1759326169270)).unwrap();
        let query_ast_json = serde_json::to_string(&query_ast).unwrap();
        assert_eq!(
            query_ast_json,
            r#"{"type":"bool","must":[{"type":"full_text","field":"custom.@agent","text":"core","params":{"mode":{"type":"phrase_fallback_to_intersection"}},"lenient":false},{"type":"range","field":"timestamp","lower_bound":{"Included":1759325269270},"upper_bound":{"Excluded":1759326169270}}]}"#
        );

        let query_ast =
            try_into_query_ast("filename:foo", Some(1759325269270), Some(1759326169270)).unwrap();
        let query_ast_json = serde_json::to_string(&query_ast).unwrap();
        assert_eq!(
            query_ast_json,
            r#"{"type":"bool","must":[{"type":"full_text","field":"tag.filename","text":"foo","params":{"mode":{"type":"phrase_fallback_to_intersection"}},"lenient":false},{"type":"range","field":"timestamp","lower_bound":{"Included":1759325269270},"upper_bound":{"Excluded":1759326169270}}]}"#
        );
    }

    #[tokio::test]
    async fn test_trace_id_rewrite_32char_hex() {
        // 32-char hex: trace_id = hex, trace_id_low = lower-64 decimal.
        // Lower 64 of "69668a9f0000000024952c60529c35bb" = "24952c60529c35bb" = 2636061949109745083
        let query_ast = try_into_query_ast(
            "trace_id:69668a9f0000000024952c60529c35bb",
            Some(1759325269270),
            Some(1759326169270),
        )
        .unwrap();
        let json = serde_json::to_value(&query_ast).unwrap();
        let should = &json["must"][0]["should"];
        assert_eq!(
            should[0],
            serde_json::json!({"type": "term", "field": "trace_id", "value": "69668a9f0000000024952c60529c35bb"})
        );
        assert_eq!(
            should[1],
            serde_json::json!({"type": "term", "field": "trace_id_low", "value": "2636061949109745083"})
        );
        assert_eq!(json["must"][0]["minimum_should_match"], 1);
    }

    #[tokio::test]
    async fn test_trace_id_rewrite_128bit_small_decimal() {
        // 128-bit decimal with ≤ 32 digits: 18446744073709551616 = 2^64 =
        // 0x00000000000000010000000000000000. Lower 64 bits = 0.
        // Previously this fell through to the short-value branch where parse::<u64>()
        // fails (> u64::MAX), leaving only the direct trace_id match.
        let query_ast = try_into_query_ast(
            "trace_id:18446744073709551616",
            Some(1759325269270),
            Some(1759326169270),
        )
        .unwrap();
        let json = serde_json::to_value(&query_ast).unwrap();
        let should = &json["must"][0]["should"];
        assert_eq!(
            should[0],
            serde_json::json!({"type": "term", "field": "trace_id", "value": "00000000000000010000000000000000"})
        );
        assert_eq!(
            should[1],
            serde_json::json!({"type": "term", "field": "trace_id_low", "value": "0"})
        );
        assert_eq!(json["must"][0]["minimum_should_match"], 1);
    }

    #[tokio::test]
    async fn test_trace_id_rewrite_short_decimal() {
        // Short decimal: direct match + trace_id_low exact match.
        // 2636061949109745083 decimal is the lower-64 decimal for hex "24952c60529c35bb".
        let query_ast = try_into_query_ast(
            "trace_id:2636061949109745083",
            Some(1759325269270),
            Some(1759326169270),
        )
        .unwrap();
        let json = serde_json::to_value(&query_ast).unwrap();
        let should = &json["must"][0]["should"];
        assert_eq!(
            should[0],
            serde_json::json!({"type": "term", "field": "trace_id", "value": "2636061949109745083"})
        );
        assert_eq!(
            should[1],
            serde_json::json!({"type": "term", "field": "trace_id_low", "value": "2636061949109745083"})
        );
        assert_eq!(json["must"][0]["minimum_should_match"], 1);
    }

    #[tokio::test]
    async fn test_trace_id_rewrite_short_hex() {
        // 16-char hex: direct match + trace_id_low with the decimal equivalent.
        // "24952c60529c35bb" hex = 2636061949109745083 decimal.
        let query_ast = try_into_query_ast(
            "trace_id:24952c60529c35bb",
            Some(1759325269270),
            Some(1759326169270),
        )
        .unwrap();
        let json = serde_json::to_value(&query_ast).unwrap();
        let should = &json["must"][0]["should"];
        assert_eq!(
            should[0],
            serde_json::json!({"type": "term", "field": "trace_id", "value": "24952c60529c35bb"})
        );
        assert_eq!(
            should[1],
            serde_json::json!({"type": "term", "field": "trace_id_low", "value": "2636061949109745083"})
        );
    }

    #[tokio::test]
    async fn test_trace_id_no_rewrite_too_long() {
        // > 32 chars: pass through unchanged (no expansion).
        let query_ast = try_into_query_ast(
            "trace_id:69668a9f0000000024952c60529c35bb00",
            Some(1759325269270),
            Some(1759326169270),
        )
        .unwrap();
        let json = serde_json::to_value(&query_ast).unwrap();
        // No should clauses — the trace_id query was not expanded.
        assert!(json["must"][0]["should"].is_null());
    }

    #[tokio::test]
    async fn test_trace_id_no_rewrite_invalid_hex() {
        // 32 chars but not valid hex: pass through unchanged.
        let query_ast = try_into_query_ast(
            "trace_id:zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            Some(1759325269270),
            Some(1759326169270),
        )
        .unwrap();
        let json = serde_json::to_value(&query_ast).unwrap();
        assert!(json["must"][0]["should"].is_null());
    }

    #[tokio::test]
    async fn test_trace_id_rewrite_128bit_decimal() {
        // 128-bit decimal (39 digits): convert to 32-char hex + trace_id_low.
        // 184635789406270697830463680821029800615 == 0x8ae78f3f79c2d0540c39b8f0d87c8aa7
        // lower 64 decimal = 880938546691345063
        let query_ast = try_into_query_ast(
            "trace_id:184635789406270697830463680821029800615",
            Some(1759325269270),
            Some(1759326169270),
        )
        .unwrap();
        let json = serde_json::to_value(&query_ast).unwrap();
        let should = &json["must"][0]["should"];
        assert_eq!(
            should[0],
            serde_json::json!({"type": "term", "field": "trace_id", "value": "8ae78f3f79c2d0540c39b8f0d87c8aa7"})
        );
        assert_eq!(
            should[1],
            serde_json::json!({"type": "term", "field": "trace_id_low", "value": "880938546691345063"})
        );
        assert_eq!(json["must"][0]["minimum_should_match"], 1);
    }

    #[tokio::test]
    async fn test_trace_id_rewrite_does_not_affect_other_fields() {
        // Non-trace_id fields are not expanded.
        let query_ast =
            try_into_query_ast("service:mysvc", Some(1759325269270), Some(1759326169270)).unwrap();
        let json = serde_json::to_value(&query_ast).unwrap();
        assert!(json["must"][0]["should"].is_null());
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
