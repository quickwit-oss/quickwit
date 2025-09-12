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

use std::collections::HashMap;

use quickwit_common::rate_limited_warn;
use quickwit_config::{INGEST_V2_SOURCE_ID, validate_identifier, validate_index_id_pattern};
use quickwit_ingest::{CommitType, IngestServiceError};
use quickwit_proto::ingest::DocBatchV2;
use quickwit_proto::ingest::router::{
    IngestRequestV2, IngestRouterService, IngestRouterServiceClient, IngestSubrequest,
};
use quickwit_proto::opentelemetry::proto::common::v1::any_value::Value as OtlpValue;
use quickwit_proto::opentelemetry::proto::common::v1::{
    AnyValue as OtlpAnyValue, ArrayValue as OtlpArrayValue, KeyValue as OtlpKeyValue,
};
use serde_json::{Number as JsonNumber, Value as JsonValue};

mod logs;
mod metrics;
mod span_id;
#[cfg(any(test, feature = "testsuite"))]
mod test_utils;
mod trace_id;
mod traces;

pub use logs::{
    JsonLogIterator, OTEL_LOGS_INDEX_ID, OtlpGrpcLogsService, OtlpLogsError, parse_otlp_logs_json,
    parse_otlp_logs_protobuf,
};
pub use span_id::{SpanId, TryFromSpanIdError};
#[cfg(any(test, feature = "testsuite"))]
pub use test_utils::make_resource_spans_for_test;
use tonic::Status;
pub use trace_id::{TraceId, TryFromTraceIdError};
pub use traces::{
    Event, JsonSpanIterator, Link, OTEL_TRACES_INDEX_ID, OTEL_TRACES_INDEX_ID_PATTERN,
    OtlpGrpcTracesService, OtlpTracesError, Span, SpanFingerprint, SpanKind, SpanStatus,
    parse_otlp_spans_json, parse_otlp_spans_protobuf,
};

#[derive(Debug, Clone, Copy)]
pub enum OtelSignal {
    Logs,
    Traces,
}

impl OtelSignal {
    pub fn header_name(&self) -> &'static str {
        match self {
            OtelSignal::Logs => "qw-otel-logs-index",
            OtelSignal::Traces => "qw-otel-traces-index",
        }
    }

    pub fn default_index_id(&self) -> &'static str {
        match self {
            OtelSignal::Logs => OTEL_LOGS_INDEX_ID,
            OtelSignal::Traces => OTEL_TRACES_INDEX_ID,
        }
    }
}

impl From<OtlpLogsError> for tonic::Status {
    fn from(error: OtlpLogsError) -> Self {
        tonic::Status::invalid_argument(error.to_string())
    }
}

impl From<OtlpTracesError> for tonic::Status {
    fn from(error: OtlpTracesError) -> Self {
        tonic::Status::invalid_argument(error.to_string())
    }
}

impl From<TryFromSpanIdError> for tonic::Status {
    fn from(error: TryFromSpanIdError) -> Self {
        tonic::Status::invalid_argument(error.to_string())
    }
}

impl From<TryFromTraceIdError> for tonic::Status {
    fn from(error: TryFromTraceIdError) -> Self {
        tonic::Status::invalid_argument(error.to_string())
    }
}

// An `Attribute` is a key-value pair, which MUST have the following properties:
// - The attribute key MUST be a non-null and non-empty string.
// - The attribute value is either:
//  - A primitive type: string, boolean, double precision floating point (IEEE 754-1985) or signed
//    64 bit integer.
//  - An array of primitive type values. The array MUST be homogeneous, i.e., it MUST NOT contain
//    values of different types.
//
// <https://github.com/open-telemetry/opentelemetry-specification/tree/main/specification/common#attribute>
pub(crate) fn extract_attributes(attributes: Vec<OtlpKeyValue>) -> HashMap<String, JsonValue> {
    let mut attrs = HashMap::with_capacity(attributes.len());

    for attribute in attributes {
        if attribute.key.is_empty() {
            continue;
        }
        if let Some(value) = attribute
            .value
            .and_then(|any_value| any_value.value)
            .and_then(oltp_value_to_json_value)
        {
            attrs.insert(attribute.key, value);
        }
    }
    attrs
}

fn oltp_value_to_json_value(value: OtlpValue) -> Option<JsonValue> {
    match value {
        OtlpValue::ArrayValue(OtlpArrayValue { values }) => Some(
            values
                .into_iter()
                .filter_map(|value| match value.value {
                    Some(value) => oltp_value_to_json_value(value),
                    None => None,
                })
                .collect(),
        ),
        OtlpValue::BoolValue(bool_value) => Some(JsonValue::Bool(bool_value)),
        OtlpValue::DoubleValue(double_value) => {
            JsonNumber::from_f64(double_value).map(JsonValue::Number)
        }
        OtlpValue::IntValue(int_value) => Some(JsonValue::Number(JsonNumber::from(int_value))),
        OtlpValue::KvlistValue(key_values) => {
            let mut map = serde_json::Map::with_capacity(key_values.values.len());

            for key_value in key_values.values {
                if let Some(value) = key_value
                    .value
                    .and_then(|any_value| any_value.value)
                    .and_then(oltp_value_to_json_value)
                {
                    map.insert(key_value.key, value);
                }
            }
            Some(JsonValue::Object(map))
        }
        OtlpValue::StringValue(string_value) => Some(JsonValue::String(string_value)),
        OtlpValue::BytesValue(_) => {
            rate_limited_warn!(limit_per_min = 10, "ignoring unsupported OTLP bytes value");
            None
        }
    }
}

pub(crate) fn parse_log_record_body(body: OtlpAnyValue) -> Option<JsonValue> {
    body.value.and_then(oltp_value_to_json_value).map(|value| {
        if value.is_string() {
            let mut map = serde_json::Map::with_capacity(1);
            map.insert("message".to_string(), value);
            JsonValue::Object(map)
        } else {
            value
        }
    })
}

fn is_zero(count: &u32) -> bool {
    *count == 0
}

#[allow(clippy::result_large_err)]
pub fn extract_otel_traces_index_id_patterns_from_metadata(
    metadata: &tonic::metadata::MetadataMap,
) -> Result<Vec<String>, Status> {
    let comma_separated_index_id_patterns = metadata
        .get(OtelSignal::Traces.header_name())
        .map(|index| index.to_str())
        .transpose()
        .map_err(|error| {
            Status::internal(format!(
                "failed to extract index ID from request header: {error}",
            ))
        })?
        .unwrap_or(OTEL_TRACES_INDEX_ID_PATTERN);
    let mut index_id_patterns = Vec::new();
    for index_id_pattern in comma_separated_index_id_patterns.split(',') {
        if index_id_pattern.is_empty() {
            continue;
        }
        validate_index_id_pattern(index_id_pattern, true).map_err(|error| {
            Status::internal(format!(
                "invalid index ID pattern in request header: {error}",
            ))
        })?;
        index_id_patterns.push(index_id_pattern.to_string());
    }
    Ok(index_id_patterns)
}

#[allow(clippy::result_large_err)]
pub(crate) fn extract_otel_index_id_from_metadata(
    metadata: &tonic::metadata::MetadataMap,
    otel_signal: OtelSignal,
) -> Result<String, Status> {
    let index_id = metadata
        .get(otel_signal.header_name())
        .map(|index: &tonic::metadata::MetadataValue<tonic::metadata::Ascii>| index.to_str())
        .transpose()
        .map_err(|error| {
            Status::internal(format!(
                "failed to extract index ID from request metadata: {error}",
            ))
        })?
        .unwrap_or_else(|| otel_signal.default_index_id());
    validate_identifier("index_id", index_id).map_err(|error| {
        Status::internal(format!(
            "invalid index ID pattern in request metadata: {error}",
        ))
    })?;
    Ok(index_id.to_string())
}

async fn ingest_doc_batch_v2(
    ingest_router: IngestRouterServiceClient,
    index_id: String,
    doc_batch: DocBatchV2,
    commit_type: CommitType,
) -> Result<(), IngestServiceError> {
    let subrequest = IngestSubrequest {
        subrequest_id: 0,
        index_id,
        source_id: INGEST_V2_SOURCE_ID.to_string(),
        doc_batch: Some(doc_batch),
    };
    let request = IngestRequestV2 {
        commit_type: commit_type.into(),
        subrequests: vec![subrequest],
    };
    let mut response = ingest_router.ingest(request).await?;
    let num_responses = response.successes.len() + response.failures.len();
    if num_responses != 1 {
        return Err(IngestServiceError::Internal(format!(
            "expected a single failure or success, got {num_responses}"
        )));
    }
    if response.successes.pop().is_some() {
        return Ok(());
    }
    let ingest_failure = response.failures.pop().unwrap();
    Err(ingest_failure.into())
}

#[cfg(test)]
mod tests {
    use quickwit_proto::opentelemetry::proto::common::v1::any_value::{
        Value as OtlpValue, Value as OtlpAnyValueValue,
    };
    use quickwit_proto::opentelemetry::proto::common::v1::{
        ArrayValue as OtlpArrayValue, KeyValueList as OtlpKeyValueList,
    };
    use serde_json::{Value as JsonValue, json};

    use super::*;
    use crate::otlp::{extract_attributes, oltp_value_to_json_value, parse_log_record_body};

    #[test]
    fn test_oltp_value_to_json_value() {
        assert_eq!(
            oltp_value_to_json_value(OtlpValue::ArrayValue(OtlpArrayValue { values: Vec::new() })),
            Some(json!([]))
        );
        assert_eq!(
            oltp_value_to_json_value(OtlpValue::ArrayValue(OtlpArrayValue {
                values: vec![
                    OtlpAnyValue {
                        value: Some(OtlpAnyValueValue::IntValue(1337))
                    },
                    OtlpAnyValue {
                        value: Some(OtlpAnyValueValue::StringValue("1337".to_string()))
                    }
                ]
            })),
            Some(json!([1337, "1337"]))
        );
        assert_eq!(
            oltp_value_to_json_value(OtlpValue::BoolValue(true)),
            Some(json!(true))
        );
        assert_eq!(
            oltp_value_to_json_value(OtlpValue::DoubleValue(12.0)),
            Some(json!(12.0))
        );
        assert_eq!(
            oltp_value_to_json_value(OtlpValue::IntValue(42)),
            Some(json!(42))
        );
        assert_eq!(
            oltp_value_to_json_value(OtlpValue::KvlistValue(OtlpKeyValueList {
                values: Vec::new()
            })),
            Some(json!({}))
        );
        assert_eq!(
            oltp_value_to_json_value(OtlpValue::KvlistValue(OtlpKeyValueList {
                values: vec![
                    OtlpKeyValue {
                        key: "foo".to_string(),
                        value: Some(OtlpAnyValue {
                            value: Some(OtlpAnyValueValue::IntValue(1337))
                        })
                    },
                    OtlpKeyValue {
                        key: "bar".to_string(),
                        value: Some(OtlpAnyValue {
                            value: Some(OtlpAnyValueValue::StringValue("1337".to_string()))
                        })
                    }
                ]
            })),
            Some(json!({
                "foo": 1337,
                "bar": "1337"
            }))
        );
        assert_eq!(
            oltp_value_to_json_value(OtlpValue::StringValue("foo".to_string())),
            Some(json!("foo"))
        );
    }

    #[test]
    fn test_extract_attributes() {
        assert!(extract_attributes(Vec::new()).is_empty());

        let attributes = vec![
            OtlpKeyValue {
                key: "".to_string(),
                value: None,
            },
            OtlpKeyValue {
                key: "".to_string(),
                value: Some(OtlpAnyValue {
                    value: Some(OtlpAnyValueValue::BoolValue(true)),
                }),
            },
            OtlpKeyValue {
                key: "empty_value".to_string(),
                value: None,
            },
            OtlpKeyValue {
                key: "empty_value_value".to_string(),
                value: Some(OtlpAnyValue { value: None }),
            },
        ];
        assert!(extract_attributes(attributes).is_empty());

        let attributes = vec![
            OtlpKeyValue {
                key: "array_key".to_string(),
                value: Some(OtlpAnyValue {
                    value: Some(OtlpAnyValueValue::ArrayValue(OtlpArrayValue {
                        values: vec![OtlpAnyValue {
                            value: Some(OtlpAnyValueValue::IntValue(1337)),
                        }],
                    })),
                }),
            },
            OtlpKeyValue {
                key: "bool_key".to_string(),
                value: Some(OtlpAnyValue {
                    value: Some(OtlpAnyValueValue::BoolValue(true)),
                }),
            },
            OtlpKeyValue {
                key: "double_key".to_string(),
                value: Some(OtlpAnyValue {
                    value: Some(OtlpAnyValueValue::DoubleValue(12.0)),
                }),
            },
            OtlpKeyValue {
                key: "int_key".to_string(),
                value: Some(OtlpAnyValue {
                    value: Some(OtlpAnyValueValue::IntValue(42)),
                }),
            },
            OtlpKeyValue {
                key: "string_key".to_string(),
                value: Some(OtlpAnyValue {
                    value: Some(OtlpAnyValueValue::StringValue("foo".to_string())),
                }),
            },
        ];
        let expected_attributes = HashMap::from_iter([
            ("array_key".to_string(), json!([1337])),
            ("bool_key".to_string(), json!(true)),
            ("double_key".to_string(), json!(12.0)),
            ("int_key".to_string(), json!(42)),
            ("string_key".to_string(), json!("foo")),
        ]);
        assert_eq!(extract_attributes(attributes), expected_attributes);
    }

    #[test]
    fn test_parse_log_record_body() {
        let value = parse_log_record_body(OtlpAnyValue {
            value: Some(OtlpAnyValueValue::StringValue("body".to_string())),
        })
        .unwrap();
        let JsonValue::Object(map) = value else {
            panic!("Expected object, got {value:?}");
        };
        assert_eq!(map.len(), 1);
        assert_eq!(map["message"], json!("body"));
    }

    #[test]
    fn test_extract_otel_index_id_patterns_from_metadata() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("qw-otel-traces-index", "foo,bar".parse().unwrap());
        let index_id_patterns =
            extract_otel_traces_index_id_patterns_from_metadata(&metadata).unwrap();
        assert_eq!(
            index_id_patterns,
            vec!["foo".to_string(), "bar".to_string()]
        );

        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("bad-header", "foo,bar".parse().unwrap());
        let index_id_patterns =
            extract_otel_traces_index_id_patterns_from_metadata(&metadata).unwrap();
        assert_eq!(index_id_patterns, vec![OTEL_TRACES_INDEX_ID_PATTERN]);

        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("qw-otel-traces-index", "foo,bar".parse().unwrap());
        let index_id_patterns =
            extract_otel_traces_index_id_patterns_from_metadata(&metadata).unwrap();
        assert_eq!(
            index_id_patterns,
            vec!["foo".to_string(), "bar".to_string()]
        );

        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("qw-otel-traces-index", "foo,bar,".parse().unwrap());
        let index_id_patterns =
            extract_otel_traces_index_id_patterns_from_metadata(&metadata).unwrap();
        assert_eq!(
            index_id_patterns,
            vec!["foo".to_string(), "bar".to_string()]
        );

        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("qw-otel-traces-index", "foo,bar,,".parse().unwrap());
        let index_id_patterns =
            extract_otel_traces_index_id_patterns_from_metadata(&metadata).unwrap();
        assert_eq!(
            index_id_patterns,
            vec!["foo".to_string(), "bar".to_string()]
        );

        // invalid index ID pattern
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("qw-otel-traces-index", "foo,bar, ,".parse().unwrap());
        let extract_res = extract_otel_traces_index_id_patterns_from_metadata(&metadata);
        assert!(extract_res.is_err());
    }

    #[test]
    fn test_extract_otel_index_id_from_metadata() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("qw-otel-logs-index", "foo".parse().unwrap());
        let index_id = extract_otel_index_id_from_metadata(&metadata, OtelSignal::Logs).unwrap();
        assert_eq!(index_id, "foo");

        // default index ID
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("wrong-header", "foo".parse().unwrap());
        let index_id = extract_otel_index_id_from_metadata(&metadata, OtelSignal::Logs).unwrap();
        assert_eq!(index_id, OTEL_LOGS_INDEX_ID);

        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("qw-otel-traces-index", "foo".parse().unwrap());
        let index_id = extract_otel_index_id_from_metadata(&metadata, OtelSignal::Traces).unwrap();
        assert_eq!(index_id, "foo");

        // default index ID
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("wrong-header", "foo".parse().unwrap());
        let index_id = extract_otel_index_id_from_metadata(&metadata, OtelSignal::Traces).unwrap();
        assert_eq!(index_id, OTEL_TRACES_INDEX_ID);

        // invalid index ID
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("qw-otel-traces-index", "foo bar".parse().unwrap());
        let extract_res = extract_otel_index_id_from_metadata(&metadata, OtelSignal::Traces);
        assert!(extract_res.is_err());
    }
}
