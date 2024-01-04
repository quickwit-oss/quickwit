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

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SpanId([u8; 8]);

impl SpanId {
    pub const HEX_LENGTH: usize = 16;

    pub fn new(bytes: [u8; 8]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }
}

impl Serialize for SpanId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let hexspan_id = hex::encode(self.0);
        serializer.serialize_str(&hexspan_id)
    }
}

impl<'de> Deserialize<'de> for SpanId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let hexspan_id = String::deserialize(deserializer)?;

        if hexspan_id.len() != SpanId::HEX_LENGTH {
            let message = format!(
                "hex span ID must be {} bytes long, got {}",
                SpanId::HEX_LENGTH,
                hexspan_id.len()
            );
            return Err(de::Error::custom(message));
        }
        let mut span_id = [0u8; 8];
        hex::decode_to_slice(hexspan_id, &mut span_id).map_err(|error| {
            let message = format!("failed to decode hex span ID: {error:?}");
            de::Error::custom(message)
        })?;
        Ok(SpanId(span_id))
    }
}

#[derive(Debug, thiserror::Error)]
#[error("span ID must be 8 bytes long, got {0}")]
pub struct TryFromSpanIdError(usize);

impl TryFrom<&[u8]> for SpanId {
    type Error = TryFromSpanIdError;

    fn try_from(slice: &[u8]) -> Result<Self, Self::Error> {
        let span_id = slice
            .try_into()
            .map_err(|_| TryFromSpanIdError(slice.len()))?;
        Ok(SpanId(span_id))
    }
}

impl TryFrom<Vec<u8>> for SpanId {
    type Error = TryFromSpanIdError;

    fn try_from(vec: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(&vec[..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_span_id_serde() {
        let expected_span_id = SpanId::new([1; 8]);
        let span_id_json = serde_json::to_string(&expected_span_id).unwrap();
        assert_eq!(span_id_json, r#""0101010101010101""#);

        let span_id = serde_json::from_str::<SpanId>(&span_id_json).unwrap();
        assert_eq!(span_id, expected_span_id,);
    }

    #[test]
    fn test_span_id_try_from() {
        let expected_span_id = SpanId::new([1; 8]);
        let span_id = SpanId::try_from([1; 8].as_slice()).unwrap();
        assert_eq!(span_id, expected_span_id);

        let error = SpanId::try_from([1; 9].as_slice()).unwrap_err();
        assert_eq!(error.0, 9);
    }
}
