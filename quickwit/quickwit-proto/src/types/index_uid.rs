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
use std::str::FromStr;

use bytes::{Buf, BufMut};
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;
pub use ulid::Ulid;

use super::ULID_SIZE;
use crate::types::IndexId;

/// Index identifiers that uniquely identify not only the index, but also
/// its incarnation allowing to distinguish between deleted and recreated indexes.
/// It is represented as a string in index_id:incarnation_id format.
#[derive(Clone, Debug, Default, PartialEq, Eq, Ord, PartialOrd, Hash)]
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
    /// Creates a new index UID from an index ID using a random ULID as incarnation ID.
    pub fn new_with_random_ulid(index_id: &str) -> Self {
        Self::new(index_id, Ulid::new())
    }

    fn new(index_id: &str, incarnation_id: impl Into<Ulid>) -> Self {
        assert!(!index_id.contains(':'), "index ID may not contain `:`");

        Self {
            index_id: index_id.to_string(),
            incarnation_id: incarnation_id.into(),
        }
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(index_id: &str, incarnation_id: u128) -> Self {
        Self {
            index_id: index_id.to_string(),
            incarnation_id: incarnation_id.into(),
        }
    }
}

#[derive(Error, Debug)]
#[error("invalid index UID `{0}`")]
pub struct InvalidIndexUid(String);

impl FromStr for IndexUid {
    type Err = InvalidIndexUid;

    fn from_str(index_uid_str: &str) -> Result<Self, Self::Err> {
        let Some((index_id, incarnation_id_str)) = index_uid_str.split_once(':') else {
            return Err(InvalidIndexUid(index_uid_str.to_string()));
        };
        let incarnation_id = Ulid::from_string(incarnation_id_str)
            .map_err(|_| InvalidIndexUid(index_uid_str.to_string()))?;
        let index_uid = IndexUid {
            index_id: index_id.to_string(),
            incarnation_id,
        };
        Ok(index_uid)
    }
}

impl<'de> Deserialize<'de> for IndexUid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let index_uid_str: Cow<'de, str> = Cow::deserialize(deserializer)?;
        let index_uid = IndexUid::from_str(&index_uid_str).map_err(D::Error::custom)?;
        Ok(index_uid)
    }
}

impl Serialize for IndexUid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.collect_str(&self)
    }
}

impl prost::Message for IndexUid {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        if !self.index_id.is_empty() {
            prost::encoding::string::encode(1u32, &self.index_id, buf);
        }
        // TODO: when `bytes::encode` supports `&[u8]`, we can remove this allocation.
        prost::encoding::bytes::encode(2u32, &self.incarnation_id.to_bytes().to_vec(), buf);
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> ::core::result::Result<(), prost::DecodeError> {
        const STRUCT_NAME: &str = "IndexUid";

        match tag {
            1u32 => {
                let value = &mut self.index_id;
                prost::encoding::string::merge(wire_type, value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "index_id");
                    error
                })
            }
            2u32 => {
                let mut buffer = Vec::with_capacity(ULID_SIZE);

                prost::encoding::bytes::merge(wire_type, &mut buffer, buf, ctx).map_err(
                    |mut error| {
                        error.push(STRUCT_NAME, "incarnation_id");
                        error
                    },
                )?;
                let ulid_bytes: [u8; ULID_SIZE] =
                    buffer.try_into().map_err(|buffer: Vec<u8>| {
                        prost::DecodeError::new(format!(
                            "invalid length for field `incarnation_id`, expected 16 bytes, got {}",
                            buffer.len()
                        ))
                    })?;
                self.incarnation_id = Ulid::from_bytes(ulid_bytes);
                Ok(())
            }
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        let mut len = 0;

        if !self.index_id.is_empty() {
            len += prost::encoding::string::encoded_len(1u32, &self.index_id);
        }

        len += prost::encoding::key_len(2u32)
            + prost::encoding::encoded_len_varint(ULID_SIZE as u64)
            + ULID_SIZE;
        len
    }

    fn clear(&mut self) {
        self.index_id.clear();
        self.incarnation_id = Ulid::nil();
    }
}

#[cfg(feature = "postgres")]
impl TryFrom<String> for IndexUid {
    type Error = InvalidIndexUid;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

#[cfg(feature = "postgres")]
impl sqlx::Type<sqlx::Postgres> for IndexUid {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("VARCHAR")
    }
}

#[cfg(feature = "postgres")]
impl sqlx::Encode<'_, sqlx::Postgres> for IndexUid {
    fn encode_by_ref(&self, buf: &mut sqlx::postgres::PgArgumentBuffer) -> sqlx::encode::IsNull {
        let _ = sqlx::Encode::<sqlx::Postgres>::encode(&self.index_id, buf);
        let _ = sqlx::Encode::<sqlx::Postgres>::encode(":", buf);
        sqlx::Encode::<sqlx::Postgres>::encode(&self.incarnation_id.to_string(), buf)
    }
}

#[cfg(feature = "postgres")]
impl sqlx::postgres::PgHasArrayType for IndexUid {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("VARCHAR[]")
    }
}

impl PartialEq<(&'static str, u128)> for IndexUid {
    fn eq(&self, (index_id, incarnation_id): &(&str, u128)) -> bool {
        self.index_id == *index_id && self.incarnation_id == Ulid::from(*incarnation_id)
    }
}

#[cfg(feature = "postgres")]
impl From<IndexUid> for sea_query::Value {
    fn from(index_uid: IndexUid) -> Self {
        index_uid.to_string().into()
    }
}

#[cfg(feature = "postgres")]
impl From<&IndexUid> for sea_query::Value {
    fn from(index_uid: &IndexUid) -> Self {
        index_uid.to_string().into()
    }
}
