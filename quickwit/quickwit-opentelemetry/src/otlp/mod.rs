// Copyright (C) 2023 Quickwit, Inc.
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

use std::collections::HashMap;

use quickwit_proto::opentelemetry::proto::common::v1::any_value::Value as OtlpValue;
use quickwit_proto::opentelemetry::proto::common::v1::{
    AnyValue as OtlpAnyValue, ArrayValue as OtlpArrayValue, KeyValue as OtlpKeyValue,
};
use serde_json::{Number as JsonNumber, Value as JsonValue};

mod logs;
mod metrics;
mod span_id;
mod trace_id;
mod traces;

pub use logs::{OtlpGrpcLogsService, OTEL_LOGS_INDEX_ID};
pub use span_id::{SpanId, TryFromSpanIdError};
pub use trace_id::{TraceId, TryFromTraceIdError};
pub use traces::{
    parse_otlp_spans_json, parse_otlp_spans_protobuf, Event, JsonSpanIterator, Link,
    OtlpGrpcTracesService, OtlpTraceError, Span, SpanFingerprint, SpanKind, SpanStatus,
    OTEL_TRACES_INDEX_ID,
};

impl From<OtlpTraceError> for tonic::Status {
    fn from(error: OtlpTraceError) -> Self {
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
            .and_then(to_json_value)
        {
            attrs.insert(attribute.key, value);
        }
    }
    attrs
}

fn to_json_value(value: OtlpValue) -> Option<JsonValue> {
    match value {
        OtlpValue::ArrayValue(OtlpArrayValue { values }) => Some(
            values
                .into_iter()
                .flat_map(to_json_value_from_primitive_any_value)
                .collect(),
        ),
        OtlpValue::BoolValue(value) => Some(JsonValue::Bool(value)),
        OtlpValue::DoubleValue(value) => JsonNumber::from_f64(value).map(JsonValue::Number),
        OtlpValue::IntValue(value) => Some(JsonValue::Number(JsonNumber::from(value))),
        OtlpValue::StringValue(value) => Some(JsonValue::String(value)),
        OtlpValue::BytesValue(_) | OtlpValue::KvlistValue(_) => {
            // These attribute types are not supported for attributes according to the OpenTelemetry
            // specification.
            None
        }
    }
}

fn to_json_value_from_primitive_any_value(any_value: OtlpAnyValue) -> Option<JsonValue> {
    match any_value.value {
        Some(OtlpValue::BoolValue(value)) => Some(JsonValue::Bool(value)),
        Some(OtlpValue::DoubleValue(value)) => JsonNumber::from_f64(value).map(JsonValue::Number),
        Some(OtlpValue::IntValue(value)) => Some(JsonValue::Number(JsonNumber::from(value))),
        Some(OtlpValue::StringValue(value)) => Some(JsonValue::String(value)),
        _ => None,
    }
}

pub(crate) fn parse_log_record_body(body: OtlpAnyValue) -> Option<JsonValue> {
    body.value.and_then(to_json_value).map(|value| {
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

#[cfg(test)]
mod tests {
    use quickwit_proto::opentelemetry::proto::common::v1::any_value::Value as OtlpAnyValueValue;
    use quickwit_proto::opentelemetry::proto::common::v1::ArrayValue as OtlpArrayValue;
    use serde_json::json;

    use super::*;

    #[test]
    fn test_to_json_value() {
        assert_eq!(
            to_json_value(OtlpValue::ArrayValue(OtlpArrayValue { values: Vec::new() })),
            Some(json!([]))
        );
        assert_eq!(
            to_json_value(OtlpValue::ArrayValue(OtlpArrayValue {
                values: vec![OtlpAnyValue {
                    value: Some(OtlpAnyValueValue::IntValue(1337))
                }]
            })),
            Some(json!([1337]))
        );
        assert_eq!(to_json_value(OtlpValue::BoolValue(true)), Some(json!(true)));
        assert_eq!(
            to_json_value(OtlpValue::DoubleValue(12.0)),
            Some(json!(12.0))
        );
        assert_eq!(to_json_value(OtlpValue::IntValue(42)), Some(json!(42)));
        assert_eq!(
            to_json_value(OtlpValue::StringValue("foo".to_string())),
            Some(json!("foo"))
        );
    }

    #[test]
    fn test_extract_attributes() {
        assert!(extract_attributes(vec![]).is_empty());

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
}
