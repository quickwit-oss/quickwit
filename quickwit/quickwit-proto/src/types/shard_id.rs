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

use std::fmt;
use std::fmt::Debug;

use bytes::{Buf, BufMut};
use bytestring::ByteString;
use prost::DecodeError;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Shard ID.
/// Shard ID are required to be globally unique.
///
/// In other words, there cannot be two shards belonging to two different sources
/// with the same shard ID.
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct ShardId(ByteString);

impl ShardId {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_u64(&self) -> Option<u64> {
        self.0.parse().ok()
    }
}

impl fmt::Display for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.0)
    }
}

impl From<&str> for ShardId {
    fn from(shard_id: &str) -> Self {
        Self(ByteString::from(shard_id))
    }
}

impl From<String> for ShardId {
    fn from(shard_id: String) -> Self {
        Self(ByteString::from(shard_id))
    }
}

impl From<u64> for ShardId {
    fn from(shard_id: u64) -> Self {
        Self(ByteString::from(format!("{shard_id:0>20}")))
    }
}

impl From<Ulid> for ShardId {
    fn from(shard_id: Ulid) -> Self {
        Self(ByteString::from(shard_id.to_string()))
    }
}

impl Serialize for ShardId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ShardId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let shard_id = String::deserialize(deserializer)?;
        Ok(Self::from(shard_id))
    }
}

impl prost::Message for ShardId {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        prost::encoding::bytes::encode(1u32, &self.0.as_bytes().clone(), buf);
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> ::core::result::Result<(), prost::DecodeError> {
        const STRUCT_NAME: &str = "ShardId";

        match tag {
            1u32 => {
                let mut value = Vec::new();
                prost::encoding::bytes::merge(wire_type, &mut value, buf, ctx).map_err(
                    |mut error| {
                        error.push(STRUCT_NAME, "position");
                        error
                    },
                )?;
                let byte_string = ByteString::try_from(value)
                    .map_err(|_| DecodeError::new("shard_id is not valid UTF-8"))?;
                *self = Self(byte_string);
                Ok(())
            }
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        prost::encoding::bytes::encoded_len(1u32, &self.0.as_bytes().clone())
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

impl PartialEq<ShardId> for &ShardId {
    #[inline]
    fn eq(&self, other: &ShardId) -> bool {
        *self == other
    }
}

#[cfg(feature = "postgres")]
impl sqlx::Type<sqlx::Postgres> for ShardId {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("VARCHAR")
    }
}

#[cfg(feature = "postgres")]
impl sqlx::Encode<'_, sqlx::Postgres> for ShardId {
    fn encode_by_ref(&self, buf: &mut sqlx::postgres::PgArgumentBuffer) -> sqlx::encode::IsNull {
        sqlx::Encode::<sqlx::Postgres>::encode(self.as_str(), buf)
    }
}

#[cfg(feature = "postgres")]
impl sqlx::postgres::PgHasArrayType for ShardId {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("VARCHAR[]")
    }
}

#[cfg(test)]
mod tests {

    use bytes::Bytes;
    use prost::Message;

    use super::*;

    #[test]
    fn test_shard_id_json_serde_roundtrip() {
        let serialized = serde_json::to_string(&ShardId::from(0)).unwrap();
        assert_eq!(serialized, r#""00000000000000000000""#);
        let deserialized: ShardId = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, ShardId::from(0));
    }

    #[test]
    fn test_shard_id_prost_serde_roundtrip() {
        let ulid = Ulid::new();
        let encoded = ShardId::from(ulid).encode_to_vec();
        assert_eq!(
            ShardId::decode(Bytes::from(encoded)).unwrap(),
            ShardId::from(ulid)
        );
        let encoded = ShardId::from(ulid).encode_length_delimited_to_vec();
        assert_eq!(
            ShardId::decode_length_delimited(Bytes::from(encoded)).unwrap(),
            ShardId::from(ulid)
        );
    }
}
