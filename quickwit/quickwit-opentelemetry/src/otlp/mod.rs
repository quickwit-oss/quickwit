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
use std::str::FromStr;

use base64::display::Base64Display;
use base64::engine::GeneralPurpose;
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

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TraceId([u8; 16]);

impl TraceId {
    pub const BASE64_LENGTH: usize = 24;

    pub fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    pub fn base64_display(&self) -> Base64Display<'_, '_, GeneralPurpose> {
        Base64Display::new(&self.0, &BASE64_STANDARD)
    }
}

impl Serialize for TraceId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let b64trace_id = BASE64_STANDARD.encode(self.0);
        serializer.serialize_str(&b64trace_id)
    }
}

impl<'de> Deserialize<'de> for TraceId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TryFromTraceIdError {
    #[error("Trace ID must be 16 bytes long, got {0}.")]
    InvalidLength(usize),
    #[error("Invalid Base64 trace ID: {0}.")]
    InvalidBase64(#[from] base64::DecodeError),
}

impl TryFrom<&[u8]> for TraceId {
    type Error = TryFromTraceIdError;

    fn try_from(slice: &[u8]) -> Result<Self, Self::Error> {
        let trace_id = slice
            .try_into()
            .map_err(|_| TryFromTraceIdError::InvalidLength(slice.len()))?;
        Ok(TraceId(trace_id))
    }
}

impl TryFrom<Vec<u8>> for TraceId {
    type Error = TryFromTraceIdError;

    fn try_from(vec: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(&vec[..])
    }
}

impl FromStr for TraceId {
    type Err = TryFromTraceIdError;

    fn from_str(b64_trace_id: &str) -> Result<Self, Self::Err> {
        if b64_trace_id.len() != Self::BASE64_LENGTH {
            return Err(TryFromTraceIdError::from(
                base64::DecodeError::InvalidLength,
            ));
        }
        let mut trace_id = [0u8; 16];
        BASE64_STANDARD
            // Using the unchecked version here because otherwise the engine gets the wrong size
            // estimate and fails.
            .decode_slice_unchecked(b64_trace_id, &mut trace_id)?;
        Ok(TraceId(trace_id))
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
        let expected_trace_id = TraceId([1; 16]);
        let trace_id_json = serde_json::to_string(&expected_trace_id).unwrap();
        assert_eq!(trace_id_json, r#""AQEBAQEBAQEBAQEBAQEBAQ==""#);

        let trace_id = serde_json::from_str::<TraceId>(&trace_id_json).unwrap();
        assert_eq!(trace_id, expected_trace_id,);
    }

    #[test]
    fn test_trace_id_try_from() {
        let expected_trace_id = TraceId([1; 16]);
        let trace_id_json = serde_json::to_string(&expected_trace_id).unwrap();
        assert_eq!(trace_id_json, r#""AQEBAQEBAQEBAQEBAQEBAQ==""#);

        let trace_id = serde_json::from_str::<TraceId>(&trace_id_json).unwrap();
        assert_eq!(trace_id, expected_trace_id,);
    }

    #[test]
    fn test_trace_id_from_str() {
        let expected_trace_id = TraceId([1; 16]);
        let trace_id: TraceId = "AQEBAQEBAQEBAQEBAQEBAQ==".parse().unwrap();
        assert_eq!(trace_id, expected_trace_id);

        let error = "AQEBAQEBAQEBAQEBAQEBAQEB==".parse::<TraceId>().unwrap_err();
        assert!(matches!(
            error,
            TryFromTraceIdError::InvalidBase64(base64::DecodeError::InvalidLength)
        ));
    }
}
