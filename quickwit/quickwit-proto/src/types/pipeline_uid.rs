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

use std::borrow::Cow;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use bytes::{Buf, BufMut};
use serde::de::Error;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::ULID_SIZE;

/// A pipeline UID identifies an indexing pipeline and an indexing task.
#[derive(Clone, Copy, Default, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct PipelineUid(Ulid);

impl fmt::Debug for PipelineUid {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Pipeline({})", self.0)
    }
}

impl Display for PipelineUid {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl PipelineUid {
    /// Creates a new random pipeline UID.
    pub fn random() -> Self {
        Self(Ulid::new())
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(ulid_u128: u128) -> PipelineUid {
        Self(Ulid::from(ulid_u128))
    }
}

impl FromStr for PipelineUid {
    type Err = &'static str;

    fn from_str(pipeline_uid_str: &str) -> Result<PipelineUid, Self::Err> {
        let pipeline_ulid =
            Ulid::from_string(pipeline_uid_str).map_err(|_| "invalid pipeline UID")?;
        Ok(PipelineUid(pipeline_ulid))
    }
}

impl Serialize for PipelineUid {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for PipelineUid {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let ulid_str: Cow<'de, str> = Cow::deserialize(deserializer)?;
        let ulid = Ulid::from_string(&ulid_str).map_err(D::Error::custom)?;
        Ok(Self(ulid))
    }
}

impl prost::Message for PipelineUid {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        // TODO: when `bytes::encode` supports `&[u8]`, we can remove this allocation.
        prost::encoding::bytes::encode(1u32, &self.0.to_bytes().to_vec(), buf);
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> ::core::result::Result<(), prost::DecodeError> {
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
        let pipeline_uid = PipelineUid::random();

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
