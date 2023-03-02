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
use std::fmt;
use std::str::FromStr;

use base64::prelude::{Engine, BASE64_STANDARD};
use quickwit_proto::opentelemetry::proto::common::v1::any_value::Value as OtlpValue;
use quickwit_proto::opentelemetry::proto::common::v1::{
    AnyValue as OtlpAnyValue, KeyValue as OtlpKeyValue,
};
use serde;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Number as JsonNumber, Value as JsonValue};
use tracing::warn;

mod logs;
mod metrics;
mod trace;

pub use logs::{OtlpGrpcLogsService, OTEL_LOGS_INDEX_CONFIG, OTEL_LOGS_INDEX_ID};
pub use trace::{
    Event, Link, OtlpGrpcTraceService, Span, SpanFingerprint, SpanKind, SpanStatus,
    OTEL_TRACE_INDEX_CONFIG, OTEL_TRACE_INDEX_ID,
};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(into = "B64TraceId", from = "B64TraceId")]
pub struct TraceId([u8; 16]);

impl TraceId {
    pub fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    pub fn b64_encode(&self) -> B64TraceId {
        let mut buffer = [0u8; 24];
        BASE64_STANDARD
            .encode_slice(self.0, &mut buffer)
            .expect("Buffer should be large enough.");
        B64TraceId(buffer)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Trace ID must be 16 bytes long, got {0}.")]
pub struct TryFromTraceIdError(usize);

impl TryFrom<&[u8]> for TraceId {
    type Error = TryFromTraceIdError;

    fn try_from(slice: &[u8]) -> Result<Self, Self::Error> {
        let trace_id = slice
            .try_into()
            .map_err(|_| TryFromTraceIdError(slice.len()))?;
        Ok(TraceId(trace_id))
    }
}

impl TryFrom<Vec<u8>> for TraceId {
    type Error = TryFromTraceIdError;

    fn try_from(vec: Vec<u8>) -> Result<Self, Self::Error> {
        let trace_id = vec
            .try_into()
            .map_err(|vec: Vec<u8>| TryFromTraceIdError(vec.len()))?;
        Ok(TraceId(trace_id))
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Base64 trace ID must be 24 bytes long, got {0}.")]
pub struct TryFromB64TraceIdError(usize);

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct B64TraceId([u8; 24]);

impl B64TraceId {
    pub fn new(bytes: [u8; 24]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).expect("Base64 should be valid UTF-8.")
    }

    pub fn b64_decode(&self) -> TraceId {
        let mut buffer = [0u8; 16];
        BASE64_STANDARD
            // Using the unchecked version here because otherwise the engine gets the wrong size
            // estimate and fails.
            .decode_slice_unchecked(self.0, &mut buffer)
            .expect("Base64 trace ID should be valid Base64.");
        TraceId(buffer)
    }
}

impl fmt::Debug for B64TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "B64TraceId({})", self.as_str())
    }
}

impl ToString for B64TraceId {
    fn to_string(&self) -> String {
        String::from_utf8(self.0.to_vec()).expect("Base64 should be valid UTF-8.")
    }
}

impl From<TraceId> for B64TraceId {
    fn from(trace_id: TraceId) -> Self {
        trace_id.b64_encode()
    }
}

impl From<B64TraceId> for TraceId {
    fn from(b64_trace_id: B64TraceId) -> Self {
        b64_trace_id.b64_decode()
    }
}

impl TryFrom<&[u8]> for B64TraceId {
    type Error = TryFromB64TraceIdError;

    fn try_from(slice: &[u8]) -> Result<Self, Self::Error> {
        let trace_id = slice
            .try_into()
            .map_err(|_| TryFromB64TraceIdError(slice.len()))?;
        Ok(B64TraceId(trace_id))
    }
}

impl TryFrom<Vec<u8>> for B64TraceId {
    type Error = TryFromB64TraceIdError;

    fn try_from(vec: Vec<u8>) -> Result<Self, Self::Error> {
        let trace_id = vec
            .try_into()
            .map_err(|vec: Vec<u8>| TryFromB64TraceIdError(vec.len()))?;
        Ok(B64TraceId(trace_id))
    }
}

impl FromStr for B64TraceId {
    type Err = TryFromB64TraceIdError;

    fn from_str(trace_id: &str) -> Result<Self, Self::Err> {
        trace_id.as_bytes().try_into()
    }
}

impl Serialize for B64TraceId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for B64TraceId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        String::deserialize(deserializer)?
            .into_bytes()
            .try_into()
            .map_err(de::Error::custom)
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_id_serde() {
        let expected_trace_id = TraceId::new([1; 16]);
        let trace_id_json = serde_json::to_string(&expected_trace_id).unwrap();
        assert_eq!(trace_id_json, r#""AQEBAQEBAQEBAQEBAQEBAQ==""#);

        let trace_id = serde_json::from_str::<TraceId>(&trace_id_json).unwrap();
        assert_eq!(trace_id, expected_trace_id,);
    }

    #[test]
    fn test_b64trace_id_serde() {
        let expected_b64trace_id = TraceId::new([1; 16]).b64_encode();
        let b64trace_id_json = serde_json::to_string(&expected_b64trace_id).unwrap();
        assert_eq!(b64trace_id_json, r#""AQEBAQEBAQEBAQEBAQEBAQ==""#);

        let b64trace_id = serde_json::from_str::<B64TraceId>(&b64trace_id_json).unwrap();
        assert_eq!(b64trace_id, expected_b64trace_id);
    }
}
