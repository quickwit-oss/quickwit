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

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TraceId([u8; 16]);

impl TraceId {
    pub const HEX_LENGTH: usize = 32;

    pub fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub fn into_bytes(self) -> [u8; 16] {
        self.0
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    pub fn hex_display(&self) -> String {
        hex::encode(self.0)
    }
}

impl Serialize for TraceId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            let hextrace_id = hex::encode(self.0);
            serializer.serialize_str(&hextrace_id)
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for TraceId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        if deserializer.is_human_readable() {
            let hextrace_id = String::deserialize(deserializer)?;
            if hextrace_id.len() != TraceId::HEX_LENGTH {
                let message = format!(
                    "hex trace ID must be {} bytes long, got {}",
                    TraceId::HEX_LENGTH,
                    hextrace_id.len()
                );
                return Err(de::Error::custom(message));
            }
            let mut trace_id_bytes = [0u8; 16];
            hex::decode_to_slice(hextrace_id, &mut trace_id_bytes).map_err(|error| {
                let message = format!("failed to decode hex span ID: {error:?}");
                de::Error::custom(message)
            })?;
            Ok(TraceId(trace_id_bytes))
        } else {
            let trace_id_bytes: [u8; 16] = <[u8; 16]>::deserialize(deserializer)?;
            Ok(TraceId(trace_id_bytes))
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("trace ID must be 16 bytes long, got {0}")]
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
        assert_eq!(trace_id_json, r#""01010101010101010101010101010101""#);

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
