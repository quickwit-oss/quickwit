// Copyright (C) 2024 Quickwit, Inc.
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

use base64::prelude::{Engine, BASE64_STANDARD};
use http_serde::http::StatusCode;
use itertools::Itertools;
use prost_types::{Duration, Timestamp};
use quickwit_proto::jaeger::api_v2::{KeyValue, Log, Process, Span, SpanRef, ValueType};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use serde_with::serde_as;

pub(super) const DEFAULT_NUMBER_OF_TRACES: i32 = 20;

pub(super) fn build_jaeger_traces(spans: Vec<JaegerSpan>) -> anyhow::Result<Vec<JaegerTrace>> {
    let jaeger_traces: Vec<JaegerTrace> = spans
        .into_iter()
        .chunk_by(|span| span.trace_id.clone())
        .into_iter()
        .map(|(span_id, group)| JaegerTrace::new(span_id, group.collect()))
        .collect();
    Ok(jaeger_traces)
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct JaegerResponseBody<T> {
    pub data: T,
}

#[serde_with::skip_serializing_none]
#[derive(Clone, Default, Debug, Serialize, Deserialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct TracesSearchQueryParams {
    #[serde(default)]
    pub service: Option<String>,
    #[serde(default)]
    pub operation: Option<String>,
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub tags: Option<String>,
    pub min_duration: Option<String>,
    pub max_duration: Option<String>,
    pub lookback: Option<String>,
    pub limit: Option<i32>,
}

// Jaeger Model for UI
// Source: https://github.com/jaegertracing/jaeger/blob/main/model/json/model.go#L82

#[derive(Clone, Default, Debug, PartialEq, Serialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct JaegerTrace {
    #[serde(rename = "traceID")]
    #[serde(serialize_with = "serialize_bytes_to_hex")]
    trace_id: Vec<u8>,
    spans: Vec<JaegerSpan>,
    processes: HashMap<String, JaegerProcess>,
    warnings: Vec<String>,
}

impl JaegerTrace {
    pub fn new(trace_id: Vec<u8>, mut spans: Vec<JaegerSpan>) -> Self {
        let processes = Self::build_process_map(&mut spans);
        JaegerTrace {
            trace_id,
            spans,
            processes,
            warnings: Vec::new(),
        }
    }

    /// Processes a collection of spans, updating the `process_id` field based on the unique
    /// `service_name` values. The function uses an accumulator (`acc`) to keep track of
    /// processed `JaegerProcess` objects and assigns a new key to each unique `service_name` value.
    /// The logic has been replicated from
    /// <https://github.com/jaegertracing/jaeger/blob/995231c42cadd70bce2bbbf02579e33f6e6329c8/model/converter/json/process_hashtable.go#L37>
    /// TODO: use also tags to identify processes.
    fn build_process_map(spans: &mut [JaegerSpan]) -> HashMap<String, JaegerProcess> {
        let mut service_name_to_process_id: HashMap<String, String> = HashMap::new();
        let mut process_map: HashMap<String, JaegerProcess> = HashMap::new();
        let mut process_counter: i32 = 0;
        for span in spans.iter_mut() {
            let Some(current_process) = span.process.as_mut() else {
                continue;
            };
            if let Some(process_id) = service_name_to_process_id.get(&current_process.service_name)
            {
                span.process_id = Some(process_id.clone());
            } else {
                process_counter += 1;
                current_process.key = format!("p{}", process_counter);
                span.process_id = Some(current_process.key.clone());
                process_map.insert(current_process.key.clone(), current_process.clone());
                service_name_to_process_id.insert(
                    current_process.service_name.clone(),
                    current_process.key.clone(),
                );
            }
        }
        process_map
    }
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JaegerSpan {
    #[serde(rename = "traceID")]
    #[serde(serialize_with = "serialize_bytes_to_hex")]
    pub trace_id: Vec<u8>,
    #[serde(rename = "spanID")]
    #[serde(serialize_with = "serialize_bytes_to_hex")]
    span_id: Vec<u8>,
    operation_name: String,
    references: Vec<JaegerSpanRef>,
    #[serde(default)]
    flags: u32,
    start_time: i64, // start_time since Unix epoch
    duration: i64,   // microseconds
    tags: Vec<JaegerKeyValue>,
    logs: Vec<JaegerLog>,
    #[serde(default)]
    #[serde(skip_serializing)]
    process: Option<JaegerProcess>,
    #[serde(rename = "processID")]
    pub process_id: Option<String>,
    pub warnings: Vec<String>,
}

impl TryFrom<Span> for JaegerSpan {
    type Error = anyhow::Error;
    fn try_from(span: Span) -> Result<Self, Self::Error> {
        let references: Vec<JaegerSpanRef> =
            span.references.iter().map(JaegerSpanRef::from).collect();
        let tags: Vec<JaegerKeyValue> = span.tags.iter().map(JaegerKeyValue::from).collect();
        let logs: Vec<JaegerLog> = span.logs.iter().map(JaegerLog::from).collect();
        Ok(Self {
            trace_id: span.trace_id,
            span_id: span.span_id,
            operation_name: span.operation_name.clone(),
            references,
            flags: span.flags,
            start_time: span
                .start_time
                .as_ref()
                .map(convert_timestamp_to_microsecs)
                .unwrap_or(0),
            duration: span
                .duration
                .map(convert_duration_to_microsecs)
                .unwrap_or(0),
            tags,
            logs,
            process: span.process.map(JaegerProcess::from),
            process_id: None,
            warnings: span.warnings.iter().map(|s| s.to_string()).collect(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JaegerSpanRef {
    #[serde(rename = "traceID")]
    #[serde(serialize_with = "serialize_bytes_to_hex")]
    trace_id: Vec<u8>,
    #[serde(rename = "spanID")]
    #[serde(serialize_with = "serialize_bytes_to_hex")]
    span_id: Vec<u8>,
    ref_type: String,
}

impl From<&SpanRef> for JaegerSpanRef {
    fn from(sr: &SpanRef) -> Self {
        Self {
            trace_id: sr.trace_id.clone(),
            span_id: sr.span_id.clone(),
            ref_type: if sr.ref_type == 0 {
                "CHILD_OF".to_string()
            } else {
                "FOLLOWS_FROM".to_string()
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JaegerKeyValue {
    key: String,
    #[serde(rename = "type")]
    value_type: String,
    value: Value,
}

impl From<&KeyValue> for JaegerKeyValue {
    fn from(kv: &KeyValue) -> Self {
        match kv.v_type {
            // String = 0,
            0 => Self {
                key: kv.key.to_string(),
                value_type: ValueType::String.as_str_name().to_lowercase(),
                value: json!(kv.v_str.to_string()),
            },
            // Bool = 1,
            1 => Self {
                key: kv.key.to_string(),
                value_type: ValueType::Bool.as_str_name().to_lowercase(),
                value: json!(kv.v_bool),
            },
            // Int64 = 2,
            2 => {
                if kv.v_int64 > 9007199254740991 {
                    Self {
                        key: kv.key.to_string(),
                        value_type: ValueType::Int64.as_str_name().to_lowercase(),
                        value: json!(kv.v_int64.to_string()),
                    }
                } else {
                    Self {
                        key: kv.key.to_string(),
                        value_type: ValueType::Int64.as_str_name().to_lowercase(),
                        value: json!(kv.v_int64),
                    }
                }
            }
            // Float64 = 3,
            3 => Self {
                key: kv.key.to_string(),
                value_type: ValueType::Float64.as_str_name().to_lowercase(),
                value: json!(kv.v_float64),
            },
            // Binary = 4,
            4 => Self {
                key: kv.key.to_string(),
                value_type: ValueType::Binary.as_str_name().to_lowercase(),
                value: serde_json::Value::String(BASE64_STANDARD.encode(kv.v_binary.as_slice())),
            },
            _ => Self {
                key: "no_value".to_string(),
                value_type: "unsupported_type".to_string(),
                value: Default::default(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JaegerLog {
    timestamp: i64, // microseconds since Unix epoch
    fields: Vec<JaegerKeyValue>,
}

impl From<&Log> for JaegerLog {
    fn from(log: &Log) -> Self {
        Self {
            timestamp: log
                .timestamp
                .as_ref()
                .map(convert_timestamp_to_microsecs)
                .unwrap_or(0),
            fields: log.fields.iter().map(JaegerKeyValue::from).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JaegerProcess {
    service_name: String,
    key: String,
    tags: Vec<JaegerKeyValue>,
}

impl Default for JaegerProcess {
    fn default() -> Self {
        Self {
            service_name: "none".to_string(),
            key: "".to_string(),
            tags: Vec::new(),
        }
    }
}

impl From<Process> for JaegerProcess {
    fn from(process: Process) -> Self {
        Self {
            service_name: process.service_name.to_string(),
            key: "".to_string(),
            tags: process.tags.iter().map(JaegerKeyValue::from).collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JaegerError {
    #[serde(with = "http_serde::status_code")]
    pub status: StatusCode,
    pub message: String,
}

impl From<anyhow::Error> for JaegerError {
    fn from(error: anyhow::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: error.to_string(),
        }
    }
}

fn serialize_bytes_to_hex<S>(bytes: &Vec<u8>, s: S) -> Result<S::Ok, S::Error>
where S: serde::Serializer {
    s.serialize_str(&format!("{:0>16}", hex::encode(bytes)))
}

fn convert_timestamp_to_microsecs(timestamp: &Timestamp) -> i64 {
    timestamp.seconds * 1_000_000 + i64::from(timestamp.nanos / 1000)
}

fn convert_duration_to_microsecs(duration: Duration) -> i64 {
    duration.seconds * 1_000_000 + i64::from(duration.nanos / 1000)
}

#[cfg(test)]
mod tests {
    use quickwit_proto::jaeger::api_v2::Log;

    use crate::jaeger_api::model::{build_jaeger_traces, JaegerSpan};

    #[test]
    fn test_convert_grpc_jaeger_spans_into_jaeger_ui_model() {
        let file_content = std::fs::read_to_string(get_jaeger_ui_trace_filepath()).unwrap();
        let expected_jaeger_trace: serde_json::Value = serde_json::from_str(&file_content).unwrap();
        let grpc_spans = create_grpc_spans();
        let jaeger_spans: Vec<JaegerSpan> = grpc_spans
            .iter()
            .map(|span| super::JaegerSpan::try_from(span.clone()).unwrap())
            .collect();
        let traces = build_jaeger_traces(jaeger_spans).unwrap();
        let trace_json: serde_json::Value = serde_json::to_value(traces[0].clone()).unwrap();
        assert_json_diff::assert_json_eq!(expected_jaeger_trace, trace_json);
    }

    fn get_jaeger_ui_trace_filepath() -> String {
        format!(
            "{}/resources/tests/jaeger_ui_trace.json",
            env!("CARGO_MANIFEST_DIR"),
        )
    }

    fn create_grpc_spans() -> Vec<quickwit_proto::jaeger::api_v2::Span> {
        let span_0 = quickwit_proto::jaeger::api_v2::Span {
            trace_id: vec![1],
            span_id: vec![1],
            operation_name: "test-general-conversion".to_string(),
            start_time: Some(prost_types::Timestamp {
                seconds: 1485467191,
                nanos: 639875000,
            }),
            duration: Some(prost_types::Duration {
                seconds: 0,
                nanos: 5000,
            }),
            process: Some(quickwit_proto::jaeger::api_v2::Process {
                service_name: "service-x".to_string(),
                tags: Vec::new(),
            }),
            logs: vec![
                Log {
                    timestamp: Some(prost_types::Timestamp {
                        seconds: 1485467191,
                        nanos: 639875000,
                    }),
                    fields: vec![quickwit_proto::jaeger::api_v2::KeyValue {
                        key: "event".to_string(),
                        v_type: 0,
                        v_str: "some-event".to_string(),
                        ..Default::default()
                    }],
                },
                Log {
                    timestamp: Some(prost_types::Timestamp {
                        seconds: 1485467191,
                        nanos: 639875000,
                    }),
                    fields: vec![quickwit_proto::jaeger::api_v2::KeyValue {
                        key: "x".to_string(),
                        v_type: 0,
                        v_str: "y".to_string(),
                        ..Default::default()
                    }],
                },
            ],
            ..Default::default()
        };
        let span_1 = quickwit_proto::jaeger::api_v2::Span {
            operation_name: "some-operation".to_string(),
            trace_id: vec![1],
            span_id: vec![2],
            start_time: Some(prost_types::Timestamp {
                seconds: 1485467191,
                nanos: 639875000,
            }),
            duration: Some(prost_types::Duration {
                seconds: 0,
                nanos: 5000,
            }),
            process: Some(quickwit_proto::jaeger::api_v2::Process {
                service_name: "service-x".to_string(),
                tags: Vec::new(),
            }),
            process_id: "".to_string(),
            tags: vec![
                quickwit_proto::jaeger::api_v2::KeyValue {
                    key: "peer.service".to_string(),
                    v_type: 0,
                    v_str: "service-y".to_string(),
                    ..Default::default()
                },
                quickwit_proto::jaeger::api_v2::KeyValue {
                    key: "peer.ipv4".to_string(),
                    v_type: 2,
                    v_int64: 23456,
                    ..Default::default()
                },
                quickwit_proto::jaeger::api_v2::KeyValue {
                    key: "error".to_string(),
                    v_type: 1,
                    v_bool: true,
                    ..Default::default()
                },
                quickwit_proto::jaeger::api_v2::KeyValue {
                    key: "temperature".to_string(),
                    v_type: 3,
                    v_float64: 72.5,
                    ..Default::default()
                },
                quickwit_proto::jaeger::api_v2::KeyValue {
                    key: "javascript_limit".to_string(),
                    v_type: 2,
                    v_int64: 9223372036854775222,
                    ..Default::default()
                },
                quickwit_proto::jaeger::api_v2::KeyValue {
                    key: "blob".to_string(),
                    v_type: 4,
                    v_binary: vec![0b0, 0b0, 0b00110000, 0b00111001],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let span_2 = quickwit_proto::jaeger::api_v2::Span {
            operation_name: "some-operation".to_string(),
            trace_id: vec![1],
            span_id: vec![3],
            references: vec![quickwit_proto::jaeger::api_v2::SpanRef {
                trace_id: vec![1],
                span_id: vec![2],
                ref_type: 0,
            }],
            start_time: Some(prost_types::Timestamp {
                seconds: 1485467191,
                nanos: 639875000,
            }),
            duration: Some(prost_types::Duration {
                seconds: 0,
                nanos: 5000,
            }),
            process: Some(quickwit_proto::jaeger::api_v2::Process {
                service_name: "service-y".to_string(),
                tags: Vec::new(),
            }),
            process_id: "".to_string(),
            ..Default::default()
        };
        let span_3 = quickwit_proto::jaeger::api_v2::Span {
            operation_name: "reference-test".to_string(),
            trace_id: vec![1],
            span_id: vec![4],
            references: vec![
                quickwit_proto::jaeger::api_v2::SpanRef {
                    trace_id: vec![255],
                    span_id: vec![255],
                    ref_type: 0,
                },
                quickwit_proto::jaeger::api_v2::SpanRef {
                    trace_id: vec![1],
                    span_id: vec![2],
                    ref_type: 0,
                },
                quickwit_proto::jaeger::api_v2::SpanRef {
                    trace_id: vec![1],
                    span_id: vec![2],
                    ref_type: 1,
                },
            ],
            start_time: Some(prost_types::Timestamp {
                seconds: 1485467191,
                nanos: 639875000,
            }),
            duration: Some(prost_types::Duration {
                seconds: 0,
                nanos: 5000,
            }),
            process: Some(quickwit_proto::jaeger::api_v2::Process {
                service_name: "service-y".to_string(),
                tags: Vec::new(),
            }),
            process_id: "".to_string(),
            warnings: vec!["some span warning".to_string()],
            ..Default::default()
        };
        let span_4 = quickwit_proto::jaeger::api_v2::Span {
            operation_name: "preserveParentID-test".to_string(),
            trace_id: vec![1],
            span_id: vec![5],
            references: vec![quickwit_proto::jaeger::api_v2::SpanRef {
                trace_id: vec![1],
                span_id: vec![4],
                ref_type: 0,
            }],
            start_time: Some(prost_types::Timestamp {
                seconds: 1485467191,
                nanos: 639875000,
            }),
            duration: Some(prost_types::Duration {
                seconds: 0,
                nanos: 4000,
            }),
            process: Some(quickwit_proto::jaeger::api_v2::Process {
                service_name: "service-y".to_string(),
                tags: Vec::new(),
            }),
            process_id: "".to_string(),
            warnings: vec!["some span warning".to_string()],
            ..Default::default()
        };
        vec![span_0, span_1, span_2, span_3, span_4]
    }
}
