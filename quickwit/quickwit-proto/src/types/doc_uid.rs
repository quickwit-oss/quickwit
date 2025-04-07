// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;
use std::fmt;

use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
pub use ulid::Ulid;

use super::ULID_SIZE;

/// A doc UID identifies a document across segments, splits, and indexes.
#[derive(Clone, Copy, Default, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct DocUid(Ulid);

impl fmt::Debug for DocUid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Doc({})", self.0)
    }
}

impl fmt::Display for DocUid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<Ulid> for DocUid {
    fn from(ulid: Ulid) -> Self {
        Self(ulid)
    }
}

impl DocUid {
    /// Creates a new random doc UID.
    pub fn random() -> Self {
        Self(Ulid::new())
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(ulid_u128: u128) -> DocUid {
        Self(Ulid::from(ulid_u128))
    }
}

impl<'de> Deserialize<'de> for DocUid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let doc_uid_str: Cow<'de, str> = Cow::deserialize(deserializer)?;
        let doc_uid = Ulid::from_string(&doc_uid_str).map_err(D::Error::custom)?;
        Ok(Self(doc_uid))
    }
}

impl Serialize for DocUid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.collect_str(&self.0)
    }
}

impl prost::Message for DocUid {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut) {
        // TODO: when `bytes::encode` supports `&[u8]`, we can remove this allocation.
        prost::encoding::bytes::encode(1u32, &self.0.to_bytes().to_vec(), buf);
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl prost::bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> ::core::result::Result<(), prost::DecodeError> {
        const STRUCT_NAME: &str = "DocUid";

        match tag {
            1u32 => {
                let mut buffer = Vec::with_capacity(ULID_SIZE);

                prost::encoding::bytes::merge(wire_type, &mut buffer, buf, ctx).map_err(
                    |mut error| {
                        error.push(STRUCT_NAME, "doc_uid");
                        error
                    },
                )?;
                let ulid_bytes: [u8; ULID_SIZE] =
                    buffer.try_into().map_err(|buffer: Vec<u8>| {
                        prost::DecodeError::new(format!(
                            "invalid length for field `doc_uid`, expected 16 bytes, got {}",
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

/// Generates monotonically increasing doc UIDs. It is not `Clone` nor `Copy` on purpose.
#[derive(Debug)]
pub struct DocUidGenerator {
    next_ulid: Ulid,
}

impl Default for DocUidGenerator {
    fn default() -> Self {
        Self {
            next_ulid: Ulid::new(),
        }
    }
}

impl DocUidGenerator {
    /// Generates a new doc UID.
    #[allow(clippy::unwrap_or_default)]
    pub fn next_doc_uid(&mut self) -> DocUid {
        let doc_uid = DocUid(self.next_ulid);
        // Clippy insists on using `unwrap_or_default`, but that's really not what we want here:
        // https://github.com/rust-lang/rust-clippy/issues/11631
        self.next_ulid = self.next_ulid.increment().unwrap_or_else(Ulid::new);
        doc_uid
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use prost::Message;

    use super::*;

    #[test]
    fn test_doc_uid_json_serde_roundtrip() {
        let doc_uid = DocUid::default();
        let serialized = serde_json::to_string(&doc_uid).unwrap();
        assert_eq!(serialized, r#""00000000000000000000000000""#);

        let deserialized: DocUid = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, doc_uid);
    }

    #[test]
    fn test_doc_uid_prost_serde_roundtrip() {
        let doc_uid = DocUid::random();

        let encoded = doc_uid.encode_to_vec();
        assert_eq!(DocUid::decode(Bytes::from(encoded)).unwrap(), doc_uid);

        let encoded = doc_uid.encode_length_delimited_to_vec();
        assert_eq!(
            DocUid::decode_length_delimited(Bytes::from(encoded)).unwrap(),
            doc_uid
        );
    }

    #[test]
    fn test_doc_uid_generator() {
        let mut generator = DocUidGenerator::default();
        let doc_uids: Vec<DocUid> = (0..10_000).map(|_| generator.next_doc_uid()).collect();
        assert!(doc_uids.windows(2).all(|window| window[0] < window[1]));
    }
}
