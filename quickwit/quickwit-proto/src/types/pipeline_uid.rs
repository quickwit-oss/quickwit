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

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// The size of a ULID in bytes.
const ULID_SIZE: usize = 16;

/// A pipeline uid identify an indexing pipeline and an indexing task.
#[derive(Clone, Copy, Default, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct PipelineUid(Ulid);

impl std::fmt::Debug for PipelineUid {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Pipeline({})", self.0)
    }
}

impl PipelineUid {
    pub fn from_u128(ulid_u128: u128) -> PipelineUid {
        PipelineUid(Ulid::from_bytes(ulid_u128.to_le_bytes()))
    }

    /// Creates a new random pipeline uid.
    pub fn new() -> Self {
        Self(Ulid::new())
    }
}

impl FromStr for PipelineUid {
    type Err = &'static str;

    fn from_str(pipeline_uid_str: &str) -> Result<PipelineUid, Self::Err> {
        let pipeline_ulid =
            Ulid::from_string(pipeline_uid_str).map_err(|_| "invalid pipeline uid")?;
        Ok(PipelineUid(pipeline_ulid))
    }
}

impl Display for PipelineUid {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Serialize for PipelineUid {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for PipelineUid {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let ulid_str = String::deserialize(deserializer)?;
        let ulid = Ulid::from_string(&ulid_str)
            .map_err(|error| serde::de::Error::custom(error.to_string()))?;
        Ok(Self(ulid))
    }
}

impl prost::Message for PipelineUid {
    fn encode_raw<B>(&self, buf: &mut B)
    where B: prost::bytes::BufMut {
        // TODO: when `bytes::encode` supports `&[u8]`, we can remove this allocation.
        prost::encoding::bytes::encode(1u32, &self.0.to_bytes().to_vec(), buf);
    }

    fn merge_field<B>(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut B,
        ctx: prost::encoding::DecodeContext,
    ) -> ::core::result::Result<(), prost::DecodeError>
    where
        B: prost::bytes::Buf,
    {
        const STRUCT_NAME: &str = "PipelineUid";

        match tag {
            1u32 => {
                let mut buffer = Vec::with_capacity(ULID_SIZE);

                prost::encoding::bytes::merge(wire_type, &mut buffer, buf, ctx).map_err(
                    |mut error| {
                        error.push(STRUCT_NAME, "pipeline_uid");
                        error
                    },
                )?;
                let ulid_bytes: [u8; ULID_SIZE] =
                    buffer.try_into().map_err(|buffer: Vec<u8>| {
                        prost::DecodeError::new(format!(
                            "invalid length for field `pipeline_uid`, expected 16 bytes, got {}",
                            buffer.len()
                        ))
                    })?;
                self.0 = Ulid::from_bytes(ulid_bytes);
                Ok(())
            }
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        prost::encoding::key_len(1u32)
            + prost::encoding::encoded_len_varint(ULID_SIZE as u64)
            + ULID_SIZE
    }

    fn clear(&mut self) {
        self.0 = Ulid::default();
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use prost::Message;

    use super::*;

    #[test]
    fn test_pipeline_uid_json_serde_roundtrip() {
        let pipeline_uid = PipelineUid::default();
        let serialized = serde_json::to_string(&pipeline_uid).unwrap();
        assert_eq!(serialized, r#""00000000000000000000000000""#);

        let deserialized: PipelineUid = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, pipeline_uid);
    }

    #[test]
    fn test_pipeline_uid_prost_serde_roundtrip() {
        let pipeline_uid = PipelineUid::new();

        let encoded = pipeline_uid.encode_to_vec();
        assert_eq!(
            PipelineUid::decode(Bytes::from(encoded)).unwrap(),
            pipeline_uid
        );

        let encoded = pipeline_uid.encode_length_delimited_to_vec();
        assert_eq!(
            PipelineUid::decode_length_delimited(Bytes::from(encoded)).unwrap(),
            pipeline_uid
        );
    }
}
