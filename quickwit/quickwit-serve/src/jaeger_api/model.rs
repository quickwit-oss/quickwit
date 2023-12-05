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
use hyper::StatusCode;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use quickwit_proto::jaeger::api_v2::{KeyValue, Span, ValueType};
use crate::jaeger_api::util::{bytes_to_hex_string, from_well_known_duration, from_well_known_timestamp};

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct JaegerSearchBody { // TODO remove
    #[serde(default)]
    pub data: Option<Vec<String>>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct JaegerResponseBody<T> {
    pub data: T,
}

#[serde_with::skip_serializing_none]
#[derive(Clone,Default, Debug, Serialize, Deserialize, utoipa::IntoParams)]
#[serde(deny_unknown_fields)]
pub struct TracesSearchQueryParams {
    #[serde(default)]
    pub service: Option<String>,
    #[serde(default)]
    pub operation: Option<String>,
    pub start: Option<i64>,
    pub end: Option<i64>,
    #[serde(rename = "minDuration")]
    pub min_duration: Option<String>,
    #[serde(rename = "maxDuration")]
    pub max_duration: Option<String>,
    pub lookback: Option<String>,
    pub limit: Option<i32>,
}

// Jaeger Model for UI
// Source: https://github.com/jaegertracing/jaeger/blob/main/model/json/model.go#L82

#[derive(Clone, Default, Debug, Serialize, Deserialize, utoipa::IntoParams)]
pub struct JaegerTrace {
    #[serde(rename = "traceID")]
    trace_id: String,
    spans: Vec<JaegerSpan>,
    processes: HashMap<String, JaegerProcess>,
    warnings: Vec<String>,
}

impl JaegerTrace {

    pub fn new(trace_id: String, spans: Vec<JaegerSpan>) -> Self {
        let processes: HashMap<String, JaegerProcess> = spans.clone()
            .into_iter()
            .fold(HashMap::new(), |mut acc, span| {
                let process_id = span.process_id.clone();
                let process = span.process;
                acc.entry(process_id)
                    .or_insert(process);
                acc
            });
        JaegerTrace {
            trace_id,
            spans,
            processes,
            warnings: vec![],
        }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JaegerSpan {
    #[serde(rename = "traceID")]
    pub trace_id: String,
    #[serde(rename = "spanID")]
    span_id: String,
    #[serde(rename = "operationName")]
    operation_name: String,
    references: Vec<JaegerSpanRef>,
    flags: u32,
    #[serde(rename = "startTime")]
    start_time: i64, // start_time since Unix epoch
    duration: i64, // microseconds
    tags: Vec<JaegerKeyValue>,
    logs: Vec<JaegerLog>,
    process: JaegerProcess,
    #[serde(rename = "processID")]
    process_id: String,
    pub warnings: Vec<String>,
}

impl JaegerSpan {
    pub fn find_better_name_for_pb_convert(span: &Span) -> Self {
        dbg!(span.process_id.clone());
        let references = span.references.iter().map(|r|
            JaegerSpanRef::new(
                bytes_to_hex_string(r.trace_id.as_slice()),
                bytes_to_hex_string(r.span_id.as_slice()),
                if r.ref_type == 0 { "CHILD_OF".to_string() } else { "FOLLOWS_FROM".to_string() }
            )
        )
            .collect_vec();

        let tags = span.tags
            .iter()
            .map(JaegerKeyValue::from_pb_key_value)
            .collect_vec();

        let logs = span.logs.iter().map(|log|
            JaegerLog::new(
                from_well_known_timestamp(&log.timestamp),
                log.fields
                    .iter()
                    .map(JaegerKeyValue::from_pb_key_value)
                    .collect_vec()
            )
        )
            .collect_vec();

        let process_pb = span.process.clone().unwrap();
        let process = JaegerProcess::new(
            process_pb.service_name.to_string(),
            process_pb.tags.iter().map(JaegerKeyValue::from_pb_key_value)
                .collect_vec()
        );
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
            process_id: span.process_id.to_string(),
            warnings: span.warnings.iter().map(|s|s.to_string()).collect_vec()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JaegerSpanRef {
    trace_id: String,
    span_id: String,
    ref_type: String,
}

impl JaegerSpanRef {
    pub fn new (
        trace_id: String,
        span_id: String,
        ref_type: String,
    ) -> Self {
        Self {
            trace_id,
            span_id,
            ref_type,
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
    // TODO a better name required
    fn from_pb_key_value(kv: &KeyValue) -> Self{
       match kv.v_type {
           // String = 0,
           0 => {
               Self {
                   key: kv.key.to_string(),
                   type_: ValueType::String.as_str_name().to_lowercase(),
                   value: json!(kv.v_str.to_string()),
               }
           },
           // Bool = 1,
           1 => {
               Self {
                   key: kv.key.to_string(),
                   type_: ValueType::Bool.as_str_name().to_lowercase(),
                   value: json!(kv.v_bool),
               }
           },
           // Int64 = 2,
           2 => {
               Self {
                   key: kv.key.to_string(),
                   type_: ValueType::Int64.as_str_name().to_lowercase(),
                   value: json!(kv.v_int64),
               }
           },
           // Float64 = 3,
           3 => {
               Self {
                   key: kv.key.to_string(),
                   type_: ValueType::Float64.as_str_name().to_lowercase(),
                   value: json!(kv.v_float64),
               }
           },
           // Binary = 4,
           4 => {
               Self {
                   key: kv.key.to_string(),
                   type_: ValueType::Binary.as_str_name().to_lowercase(),
                   value: json!(kv.v_binary),
               }
           }
           _ => {
               Self {
                   key: "no_value".to_string(),
                   type_: "unsupported_type".to_string(),
                   value: Default::default(),
               }
           }
       }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JaegerLog {
    timestamp: i64, // microseconds since Unix epoch
    fields: Vec<JaegerKeyValue>,
}

impl JaegerLog {
    pub fn new(timestamp: i64,
               fields: Vec<JaegerKeyValue>) -> Self {
        Self {
            timestamp,
            fields,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JaegerProcess {
    #[serde(rename = "serviceName")]
    service_name: String,
    tags: Vec<JaegerKeyValue>,
}

impl JaegerProcess {
    pub fn new(
        service_name: String,
        tags: Vec<JaegerKeyValue>
    ) -> Self {
        Self {
            service_name,
            tags
        }
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
