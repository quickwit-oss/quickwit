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

use anyhow::Context;
use bytes::{Buf, BufMut};
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
pub use ulid::Ulid;

use super::ULID_SIZE;

/// Unique identifier for a document mapping.
#[derive(Clone, Copy, Default, Hash, Eq, PartialEq, Ord, PartialOrd, utoipa::ToSchema)]
pub struct DocMappingUid(Ulid);

impl fmt::Debug for DocMappingUid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DocMapping({})", self.0)
    }
}

impl fmt::Display for DocMappingUid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<Ulid> for DocMappingUid {
    fn from(ulid: Ulid) -> Self {
        Self(ulid)
    }
}

impl DocMappingUid {
    /// Creates a new random doc mapping UID.
    pub fn random() -> Self {
        Self(Ulid::new())
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(ulid_u128: u128) -> DocMappingUid {
        Self(Ulid::from(ulid_u128))
    }
}

impl<'de> Deserialize<'de> for DocMappingUid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let doc_mapping_uid_str: Cow<'de, str> = Cow::deserialize(deserializer)?;
        doc_mapping_uid_str.parse().map_err(D::Error::custom)
    }
}

impl Serialize for DocMappingUid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.collect_str(&self.0)
    }
}

impl prost::Message for DocMappingUid {
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
        const STRUCT_NAME: &str = "DocMappingUid";

        match tag {
            1u32 => {
                let mut buffer = Vec::with_capacity(ULID_SIZE);

                prost::encoding::bytes::merge(wire_type, &mut buffer, buf, ctx).map_err(
                    |mut error| {
                        error.push(STRUCT_NAME, "doc_mapping_uid");
                        error
                    },
                )?;
                let ulid_bytes: [u8; ULID_SIZE] =
                    buffer.try_into().map_err(|buffer: Vec<u8>| {
                        prost::DecodeError::new(format!(
                            "invalid length for field `doc_mapping_uid`, expected 16 bytes, got {}",
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
        self.0 = Ulid::nil();
    }
}

impl FromStr for DocMappingUid {
    type Err = anyhow::Error;

    fn from_str(doc_mapping_uid_str: &str) -> Result<Self, Self::Err> {
        Ulid::from_string(doc_mapping_uid_str)
            .map(Self)
            .with_context(|| format!("failed to parse doc mapping UID `{doc_mapping_uid_str}`"))
    }
}

#[cfg(feature = "postgres")]
impl TryFrom<String> for DocMappingUid {
    type Error = anyhow::Error;

    fn try_from(doc_mapping_uid_str: String) -> Result<Self, Self::Error> {
        doc_mapping_uid_str.parse()
    }
}

#[cfg(feature = "postgres")]
impl sqlx::Type<sqlx::Postgres> for DocMappingUid {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("VARCHAR(26)")
    }
}

#[cfg(feature = "postgres")]
impl sqlx::Encode<'_, sqlx::Postgres> for DocMappingUid {
    fn encode_by_ref(&self, buf: &mut sqlx::postgres::PgArgumentBuffer) -> sqlx::encode::IsNull {
        sqlx::Encode::<sqlx::Postgres>::encode(&self.0.to_string(), buf)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use prost::Message;

    use super::*;

    #[test]
    fn test_doc_mapping_uid_json_serde_roundtrip() {
        let doc_mapping_uid = DocMappingUid::default();
        let serialized = serde_json::to_string(&doc_mapping_uid).unwrap();
        assert_eq!(serialized, r#""00000000000000000000000000""#);

        let deserialized: DocMappingUid = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, doc_mapping_uid);
    }

    #[test]
    fn test_doc_mapping_uid_prost_serde_roundtrip() {
        let doc_mapping_uid = DocMappingUid::random();

        let encoded = doc_mapping_uid.encode_to_vec();
        assert_eq!(
            DocMappingUid::decode(Bytes::from(encoded)).unwrap(),
            doc_mapping_uid
        );

        let encoded = doc_mapping_uid.encode_length_delimited_to_vec();
        assert_eq!(
            DocMappingUid::decode_length_delimited(Bytes::from(encoded)).unwrap(),
            doc_mapping_uid
        );
    }
}
