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

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::IndexId;

/// Index identifiers that uniquely identify not only the index, but also
/// its incarnation allowing to distinguish between deleted and recreated indexes.
/// It is represented as a stiring in index_id:incarnation_id format.
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct IndexUid {
    pub index_id: IndexId,
    pub incarnation_id: Ulid,
}

impl fmt::Display for IndexUid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.index_id, self.incarnation_id)
    }
}

impl IndexUid {
    /// Creates a new index UID.
    pub fn new(index_id: impl Into<String>, incarnation_id: impl Into<Ulid>) -> Self {
        Self {
            index_id: index_id.into(),
            incarnation_id: incarnation_id.into(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("failed to parse index UID `{0}`")]
pub struct ParseIndexUidError(String);

impl FromStr for IndexUid {
    type Err = ParseIndexUidError;

    fn from_str(index_uid_str: &str) -> Result<Self, Self::Err> {
        let mut parts = index_uid_str.split(':');
        let index_id = parts
            .next()
            .ok_or_else(|| ParseIndexUidError(index_uid_str.to_string()))?
            .to_string();
        let incarnation_id = parts
            .next()
            .ok_or_else(|| ParseIndexUidError(index_uid_str.to_string()))?
            .parse::<Ulid>()
            .map_err(|_| ParseIndexUidError(index_uid_str.to_string()))?;
        Ok(Self {
            index_id,
            incarnation_id,
        })
    }
}

impl Serialize for IndexUid {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for IndexUid {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        unimplemented!()
    }
}

/// Encodes an index UID into a protobuf message composed of one string and two u64 integers.
impl ::prost::Message for IndexUid {
    fn encode_raw<B>(&self, buf: &mut B)
    where B: ::prost::bytes::BufMut {
        if self.index_id != "" {
            ::prost::encoding::string::encode(1u32, &self.index_id, buf);
        }
        let (ulid_high, ulid_low): (u64, u64) = self.incarnation_id.into();

        if ulid_high != 0u64 {
            ::prost::encoding::uint64::encode(2u32, &ulid_high, buf);
        }
        if ulid_low != 0u64 {
            ::prost::encoding::uint64::encode(3u32, &ulid_low, buf);
        }
    }

    fn merge_field<B>(
        &mut self,
        tag: u32,
        wire_type: ::prost::encoding::WireType,
        buf: &mut B,
        ctx: ::prost::encoding::DecodeContext,
    ) -> ::core::result::Result<(), ::prost::DecodeError>
    where
        B: ::prost::bytes::Buf,
    {
        const STRUCT_NAME: &'static str = "IndexUid";

        match tag {
            1u32 => {
                let value = &mut self.index_id;
                ::prost::encoding::string::merge(wire_type, value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "index_id");
                    error
                })
            }
            2u32 => {
                let (mut ulid_high, ulid_low) = self.incarnation_id.into();
                ::prost::encoding::uint64::merge(wire_type, &mut ulid_high, buf, ctx).map_err(
                    |mut error| {
                        error.push(STRUCT_NAME, "incarnation_id_high");
                        error
                    },
                )?;
                self.incarnation_id = (ulid_high, ulid_low).into();
                Ok(())
            }
            3u32 => {
                let (ulid_high, mut ulid_low) = self.incarnation_id.into();
                ::prost::encoding::uint64::merge(wire_type, &mut ulid_low, buf, ctx).map_err(
                    |mut error| {
                        error.push(STRUCT_NAME, "incarnation_id_low");
                        error
                    },
                )?;
                self.incarnation_id = (ulid_high, ulid_low).into();
                Ok(())
            }
            _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        let mut len = 0;

        if self.index_id != "" {
            len += ::prost::encoding::string::encoded_len(1u32, &self.index_id);
        }
        let (ulid_high, ulid_low): (u64, u64) = self.incarnation_id.into();

        if ulid_high != 0u64 {
            len += ::prost::encoding::uint64::encoded_len(2u32, &ulid_high);
        }
        if ulid_low != 0u64 {
            len += ::prost::encoding::uint64::encoded_len(3u32, &ulid_low);
        }
        len
    }

    fn clear(&mut self) {
        self.index_id.clear();
        self.incarnation_id = Ulid::nil();
    }
}

#[macro_export]
macro_rules! index_uid {
    ($value: expr) => {
        $value
            .index_uid
            .expect("`index_uid` should be a required field")
    };
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;

    #[test]
    fn test_index_uid_serde() {
        // TODO
    }

    #[test]
    fn test_index_uid_proto() {
        let index_uid = IndexUid::new("test-index", Ulid::new());

        let mut buffer = Vec::new();
        index_uid.encode_raw(&mut buffer);

        let decoded_index_uid = IndexUid::decode(&buffer[..]).unwrap();

        assert_eq!(index_uid, decoded_index_uid);
    }
}
