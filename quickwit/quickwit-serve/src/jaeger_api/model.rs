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

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::ops::Add;

use hyper::StatusCode;
use itertools::Itertools;
use quickwit_proto::jaeger::api_v2::{KeyValue, Log, Process, Span, SpanRef, ValueType};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::jaeger_api::util::{
    bytes_to_hex_string, from_well_known_duration, from_well_known_timestamp,
};

pub(super) const ALL_OPERATIONS: &str = "";
pub(super) const DEFAULT_NUMBER_OF_TRACES: i32 = 20;

pub(super) fn build_jaeger_traces(spans: Vec<JaegerSpan>) -> anyhow::Result<Vec<JaegerTrace>> {
    let jaeger_traces = spans
        .into_iter()
        .group_by(|span| span.trace_id.clone())
        .into_iter()
        .map(|(span_id, group)| JaegerTrace::new(span_id, group.collect_vec()))
        .collect_vec();
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
    pub min_duration: Option<String>,
    pub max_duration: Option<String>,
    pub lookback: Option<String>,
    pub limit: Option<i32>,
}

// Jaeger Model for UI
// Source: https://github.com/jaegertracing/jaeger/blob/main/model/json/model.go#L82

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct JaegerTrace {
    #[serde(rename = "traceID")]
    trace_id: String,
    spans: Vec<JaegerSpan>,
    processes: HashMap<String, JaegerProcess>,
    warnings: Vec<String>,
}

impl JaegerTrace {
    pub fn new(trace_id: String, mut spans: Vec<JaegerSpan>) -> Self {
        let processes = Self::create_processes(&mut spans);
        JaegerTrace {
            trace_id,
            spans,
            processes,
            warnings: vec![],
        }
    }

    /// Processes a collection of spans, updating the `process_id` field based on the unique
    /// `service_name` values. The function uses an accumulator (`acc`) to keep track of
    /// processed `JaegerProcess` objects and assigns a new key to each unique `service_name` value.
    /// The logic has been replicated from
    /// https://github.com/jaegertracing/jaeger/blob/995231c42cadd70bce2bbbf02579e33f6e6329c8/model/converter/json/process_hashtable.go#L37
    fn create_processes(spans: &mut [JaegerSpan]) -> HashMap<String, JaegerProcess> {
        let mut hash_process_map: HashMap<u64, Vec<JaegerProcess>> = HashMap::new();
        let count: i32 = 0;
        for span in spans.iter_mut() {
            let mut current_process = span.process.clone();
            let hash = current_process.service_hash();
            if let Some(entries) = hash_process_map.get_mut(&hash) {
                if let Some(existing_p) = entries
                    .iter_mut()
                    .find(|p| p.service_name == current_process.service_name)
                {
                    current_process.key = existing_p.key.clone();
                    span.update_process_id(existing_p.key.clone());
                } else {
                    let new_key = JaegerProcess::key(&count.add(1));
                    span.update_process_id(new_key.clone());
                    current_process.key = new_key.clone();
                    entries.push(current_process.clone());
                }
            } else {
                let new_key = JaegerProcess::key(&count.add(1));
                current_process.key = new_key.clone();
                span.update_process_id(new_key.clone());
                hash_process_map.insert(hash, vec![current_process.clone()]);
            }
        }

        // Get the accumulated mapping of `key` to the corresponding `JaegerProcess`
        // The logic has been replicated from
        // https://github.com/jaegertracing/jaeger/blob/995231c42cadd70bce2bbbf02579e33f6e6329c8/model/converter/json/process_hashtable.go#L59
        let mut process_map: HashMap<String, JaegerProcess> = HashMap::new();
        for processes in hash_process_map.into_values() {
            for process in processes {
                process_map.insert(process.key.clone(), process);
            }
        }
        process_map
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JaegerSpan {
    #[serde(rename = "traceID")]
    pub trace_id: String,
    #[serde(rename = "spanID")]
    span_id: String,
    operation_name: String,
    references: Vec<JaegerSpanRef>,
    flags: u32,
    start_time: i64, // start_time since Unix epoch
    duration: i64,   // microseconds
    tags: Vec<JaegerKeyValue>,
    logs: Vec<JaegerLog>,
    #[serde(skip_serializing)]
    process: JaegerProcess,
    #[serde(rename = "processID")]
    process_id: String,
    pub warnings: Vec<String>,
}

impl TryFrom<Span> for JaegerSpan {
    type Error = anyhow::Error;
    fn try_from(span: Span) -> Result<Self, Self::Error> {
        let references = span
            .references
            .iter()
            .map(JaegerSpanRef::from)
            .collect_vec();

        let tags = span.tags.iter().map(JaegerKeyValue::from).collect_vec();

        let logs = span.logs.iter().map(JaegerLog::from).collect_vec();

        // TODO what's the best way to handle unwrap here?
        let process: JaegerProcess = span
            .process
            .map(JaegerProcess::from)
            .unwrap_or_else(JaegerProcess::default);

        Ok(Self {
            trace_id: bytes_to_hex_string(&span.trace_id),
            span_id: bytes_to_hex_string(&span.span_id),
            operation_name: span.operation_name.clone(),
            references,
            flags: span.flags,
            start_time: from_well_known_timestamp(&span.start_time),
            duration: from_well_known_duration(&span.duration),
            tags,
            logs,
            process,
            process_id: "no_value".to_string(), /* TODO we need to initialize it somehow to
                                                 * mutate it further */
            warnings: span.warnings.iter().map(|s| s.to_string()).collect_vec(),
        })
    }
}

impl JaegerSpan {
    pub fn update_process_id(&mut self, new_process_id: String) {
        self.process_id = new_process_id
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JaegerSpanRef {
    trace_id: String,
    span_id: String,
    ref_type: String,
}

impl From<&SpanRef> for JaegerSpanRef {
    fn from(sr: &SpanRef) -> Self {
        Self {
            trace_id: bytes_to_hex_string(sr.trace_id.as_slice()),
            span_id: bytes_to_hex_string(sr.span_id.as_slice()),
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
            2 => Self {
                key: kv.key.to_string(),
                value_type: ValueType::Int64.as_str_name().to_lowercase(),
                value: json!(kv.v_int64),
            },
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
                value: json!(kv.v_binary),
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
            timestamp: from_well_known_timestamp(&log.timestamp),
            fields: log.fields.iter().map(JaegerKeyValue::from).collect_vec(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JaegerProcess {
    service_name: String,
    #[serde(skip_serializing)]
    key: String,
    tags: Vec<JaegerKeyValue>,
}

impl Default for JaegerProcess {
    fn default() -> Self {
        Self {
            service_name: "none".to_string(),
            key: "".to_string(),
            tags: vec![],
        }
    }
}

impl From<Process> for JaegerProcess {
    fn from(process: Process) -> Self {
        Self {
            service_name: process.service_name.to_string(),
            key: "".to_string(),
            tags: process.tags.iter().map(JaegerKeyValue::from).collect_vec(),
        }
    }
}

impl JaegerProcess {
    fn key(count: &i32) -> String {
        format!("p{}", count)
    }

    fn service_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.service_name.hash(&mut hasher);
        hasher.finish()
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

#[cfg(test)]
mod tests {
    use super::JaegerTrace;
    use crate::jaeger_api::model::{build_jaeger_traces, JaegerSpan};

    #[test]
    fn test_convert_grpc_jaeger_spans_into_jaeger_ui_model() {
        let file_content = std::fs::read_to_string(get_jaeger_ui_trace_filepath()).unwrap();
        let expected_jaeger_trace: JaegerTrace = serde_json::from_str(&file_content).unwrap();
        let grpc_spans = create_grpc_spans();
        let jaeger_spans: Vec<JaegerSpan> = grpc_spans
            .iter()
            .map(|span| super::JaegerSpan::try_from(span.clone()).unwrap())
            .collect();
        let traces = build_jaeger_traces(jaeger_spans).unwrap();
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0], expected_jaeger_trace);
    }

    fn get_jaeger_ui_trace_filepath() -> String {
        format!(
            "{}/resources/tests/jaeger_ui_trace.json",
            env!("CARGO_MANIFEST_DIR"),
        )
    }

    fn create_grpc_spans() -> Vec<quickwit_proto::jaeger::api_v2::Span> {
        let mut spans = vec![];
        let mut span = quickwit_proto::jaeger::api_v2::Span::default();
        span.operation_name = "operation_name".to_string();
        span.span_id = vec![1, 2, 3];
        span.trace_id = vec![1, 2, 3];
        span.start_time = Some(prost_types::Timestamp {
            seconds: 1,
            nanos: 1,
        });
        span.duration = Some(prost_types::Duration {
            seconds: 1,
            nanos: 1,
        });
        span.process = Some(quickwit_proto::jaeger::api_v2::Process {
            service_name: "service_name".to_string(),
            tags: vec![],
        });
        spans.push(span);
        spans
    }
}
