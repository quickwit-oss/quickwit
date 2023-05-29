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

use base64::display::Base64Display;
use base64::engine::GeneralPurpose;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

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
        let b64trace_id = String::deserialize(deserializer)?;

        if b64trace_id.len() != TraceId::BASE64_LENGTH {
            let message = format!(
                "Base64 trace ID must be {} bytes long, got {}.",
                TraceId::BASE64_LENGTH,
                b64trace_id.len()
            );
            return Err(de::Error::custom(message));
        }
        let mut trace_id = [0u8; 16];
        BASE64_STANDARD
            // Using the unchecked version here because otherwise the engine gets the wrong size
            // estimate and fails.
            .decode_slice_unchecked(b64trace_id.as_bytes(), &mut trace_id)
            .map_err(|error| {
                let message = format!("Failed to decode Base64 trace ID: {:?}.", error);
                de::Error::custom(message)
            })?;
        Ok(TraceId(trace_id))
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
        Self::try_from(&vec[..])
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
    fn test_trace_id_try_from() {
        let expected_trace_id = TraceId::new([1; 16]);
        let trace_id = TraceId::try_from([1; 16].as_slice()).unwrap();
        assert_eq!(trace_id, expected_trace_id);

        let error = TraceId::try_from([1; 17].as_slice()).unwrap_err();
        assert_eq!(error.0, 17);
    }
}
