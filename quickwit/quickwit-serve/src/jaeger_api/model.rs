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

pub const ALL_OPERATIONS: &str = "";
pub const DEFAULT_NUMBER_OF_TRACES: i32 = 20;

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

#[derive(Clone, Default, Debug, Serialize, Deserialize, utoipa::IntoParams)]
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
        let mut count = 0;
        let mut hashcode_to_processes: HashMap<u64, Vec<JaegerProcess>> = HashMap::new();
        Self::handle_span_processes(&mut spans, &mut count, &mut hashcode_to_processes);
        JaegerTrace {
            trace_id,
            spans,
            processes: Self::create_key_to_process_mapping(&hashcode_to_processes),
            warnings: vec![],
        }
    }

    /// Processes a collection of spans, updating the `process_id` field based on the unique
    /// `service_name` values. The function uses an accumulator (`acc`) to keep track of
    /// processed `JaegerProcess` objects and assigns a new key to each unique `service_name` value.
    /// The logic has been replicated from
    /// https://github.com/jaegertracing/jaeger/blob/995231c42cadd70bce2bbbf02579e33f6e6329c8/model/converter/json/process_hashtable.go#L37
    fn handle_span_processes(
        spans: &mut [JaegerSpan],
        count: &mut i32,
        acc: &mut HashMap<u64, Vec<JaegerProcess>>,
    ) {
        for span in spans.iter_mut() {
            let mut current_process = span.process.clone();
            let hash = current_process.hash_code();
            if let Some(entries) = acc.get_mut(&hash) {
                if let Some(existing_p) = entries
                    .iter_mut()
                    .find(|p| p.service_name == current_process.service_name)
                {
                    current_process.key = existing_p.key.clone();
                    span.update_process_id(existing_p.key.clone());
                } else {
                    let new_key = JaegerProcess::next_key(&count.add(1));
                    span.update_process_id(new_key.clone());
                    current_process.key = new_key.clone();
                    entries.push(current_process.clone());
                }
            } else {
                let new_key = JaegerProcess::next_key(&count.add(1));
                current_process.key = new_key.clone();
                span.update_process_id(new_key.clone());
                acc.insert(hash, vec![current_process.clone()]);
            }
        }
    }

    /// Get the accumulated mapping of `key` to the corresponding `JaegerProcess`
    /// The logic has been replicated from
    // https://github.com/jaegertracing/jaeger/blob/995231c42cadd70bce2bbbf02579e33f6e6329c8/model/converter/json/process_hashtable.go#L59
    fn create_key_to_process_mapping(
        data: &HashMap<u64, Vec<JaegerProcess>>,
    ) -> HashMap<String, JaegerProcess> {
        let mut result: HashMap<String, JaegerProcess> = HashMap::new();
        for processes in data.values() {
            for process in processes {
                result.insert(process.key.clone(), process.clone());
            }
        }
        result
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl JaegerSpan {
    pub fn find_better_name_for_pb_convert(span: &Span) -> Self {
        let references = span
            .references
            .iter()
            .map(JaegerSpanRef::convert_from_proto)
            .collect_vec();

        let tags = span
            .tags
            .iter()
            .map(JaegerKeyValue::convert_from_proto)
            .collect_vec();

        let logs = span
            .logs
            .iter()
            .map(JaegerLog::convert_from_proto)
            .collect_vec();

        // TODO what's the best way to handle unwrap here?
        let process: JaegerProcess =
            JaegerProcess::convert_from_proto(span.process.clone().unwrap());

        Self {
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
        }
    }

    pub fn update_process_id(&mut self, new_process_id: String) {
        self.process_id = new_process_id
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JaegerSpanRef {
    trace_id: String,
    span_id: String,
    ref_type: String,
}

impl JaegerSpanRef {
    fn convert_from_proto(sr: &SpanRef) -> Self {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JaegerKeyValue {
    key: String,
    #[serde(rename = "type")]
    type_: String,
    value: Value,
}

impl JaegerKeyValue {
    fn convert_from_proto(kv: &KeyValue) -> Self {
        match kv.v_type {
            // String = 0,
            0 => Self {
                key: kv.key.to_string(),
                type_: ValueType::String.as_str_name().to_lowercase(),
                value: json!(kv.v_str.to_string()),
            },
            // Bool = 1,
            1 => Self {
                key: kv.key.to_string(),
                type_: ValueType::Bool.as_str_name().to_lowercase(),
                value: json!(kv.v_bool),
            },
            // Int64 = 2,
            2 => Self {
                key: kv.key.to_string(),
                type_: ValueType::Int64.as_str_name().to_lowercase(),
                value: json!(kv.v_int64),
            },
            // Float64 = 3,
            3 => Self {
                key: kv.key.to_string(),
                type_: ValueType::Float64.as_str_name().to_lowercase(),
                value: json!(kv.v_float64),
            },
            // Binary = 4,
            4 => Self {
                key: kv.key.to_string(),
                type_: ValueType::Binary.as_str_name().to_lowercase(),
                value: json!(kv.v_binary),
            },
            _ => Self {
                key: "no_value".to_string(),
                type_: "unsupported_type".to_string(),
                value: Default::default(),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JaegerLog {
    timestamp: i64, // microseconds since Unix epoch
    fields: Vec<JaegerKeyValue>,
}

impl JaegerLog {
    fn convert_from_proto(log: &Log) -> Self {
        Self {
            timestamp: from_well_known_timestamp(&log.timestamp),
            fields: log
                .fields
                .iter()
                .map(JaegerKeyValue::convert_from_proto)
                .collect_vec(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JaegerProcess {
    service_name: String,
    #[serde(skip_serializing)]
    key: String,
    tags: Vec<JaegerKeyValue>,
}

impl JaegerProcess {
    fn convert_from_proto(process: Process) -> Self {
        Self {
            service_name: process.service_name.to_string(),
            key: "".to_string(),
            tags: process
                .tags
                .iter()
                .map(JaegerKeyValue::convert_from_proto)
                .collect_vec(),
        }
    }

    pub fn next_key(count: &i32) -> String {
        format!("p{}", count)
    }

    pub fn hash_code(&self) -> u64 {
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

// impl JaegerError { // TODO remove?
//     pub fn internal_jaeger_error() -> Self {
//         JaegerError {
//             status: StatusCode::INTERNAL_SERVER_ERROR,
//             message: "Jaeger is not available".to_string(),
//         }
//     }
// }
