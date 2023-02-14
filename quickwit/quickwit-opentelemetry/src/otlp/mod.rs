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

use base64::prelude::{Engine, BASE64_STANDARD};
use quickwit_proto::opentelemetry::proto::common::v1::any_value::Value as OtlpValue;
use quickwit_proto::opentelemetry::proto::common::v1::{
    AnyValue as OtlpAnyValue, KeyValue as OtlpKeyValue,
};
use serde_json::{Number as JsonNumber, Value as JsonValue};
use tracing::warn;

mod logs;
mod metrics;
mod trace;

pub use logs::{OtlpGrpcLogsService, OTEL_LOGS_INDEX_CONFIG, OTEL_LOGS_INDEX_ID};
pub use trace::{
    Base64, Event, Link, OtlpGrpcTraceService, Span, SpanFingerprint, SpanKind, SpanStatus,
    OTEL_TRACE_INDEX_CONFIG, OTEL_TRACE_INDEX_ID,
};

pub(crate) fn extract_attributes(attributes: Vec<OtlpKeyValue>) -> HashMap<String, JsonValue> {
    let mut attrs = HashMap::with_capacity(attributes.len());
    for attribute in attributes {
        // Filtering out empty attribute values is fine according to the OTel spec: <https://github.com/open-telemetry/opentelemetry-specification/tree/main/specification/common#attribute>
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

pub(crate) fn to_json_value(value: OtlpValue) -> Option<JsonValue> {
    match value {
        OtlpValue::StringValue(value) => Some(JsonValue::String(value)),
        OtlpValue::BoolValue(value) => Some(JsonValue::Bool(value)),
        OtlpValue::IntValue(value) => Some(JsonValue::Number(JsonNumber::from(value))),
        OtlpValue::DoubleValue(value) => JsonNumber::from_f64(value).map(JsonValue::Number),
        OtlpValue::BytesValue(bytes) => Some(JsonValue::String(BASE64_STANDARD.encode(bytes))),
        OtlpValue::ArrayValue(_) | OtlpValue::KvlistValue(_) => {
            warn!(value=?value, "Skipping unsupported OTLP value type.");
            None
        }
    }
}
