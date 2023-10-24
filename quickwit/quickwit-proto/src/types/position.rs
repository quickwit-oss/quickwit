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

use bytes::Bytes;
use bytestring::ByteString;
use prost::{self, DecodeError};
use serde::{Deserialize, Serialize};

const BEGINNING: &str = "";

const EOF: &str = "~eof";

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Offset(ByteString);

impl Offset {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_i64(&self) -> Option<i64> {
        self.0.parse::<i64>().ok()
    }

    pub fn as_u64(&self) -> Option<u64> {
        self.0.parse::<u64>().ok()
    }

    pub fn as_usize(&self) -> Option<usize> {
        self.0.parse::<usize>().ok()
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.0)
    }
}

/// Marks a position within a specific partition/shard of a source.
///
/// The nature of the position depends on the source.
/// Each source must encode it as a `String` in such a way that
/// the lexicographical order matches the natural order of the
/// position.
///
/// For instance, for u64, a 20-left-padded decimal representation
/// can be used. Alternatively, a base64 representation of their
/// big-endian representation can be used.
///
/// The empty string can be used to represent the beginning of the source,
/// if no position makes sense. It can be built via `Position::default()`.
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum Position {
    #[default]
    Beginning,
    Offset(Offset),
    Eof,
}

impl Position {
    fn as_bytes(&self) -> Bytes {
        match self {
            Self::Beginning => Bytes::from_static(BEGINNING.as_bytes()),
            Self::Offset(offset) => offset.0.as_bytes().clone(),
            Self::Eof => Bytes::from_static(EOF.as_bytes()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Beginning => BEGINNING,
            Self::Offset(offset) => offset.as_str(),
            Self::Eof => EOF,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Beginning => None,
            Self::Offset(offset) => offset.as_i64(),
            Self::Eof => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Beginning => None,
            Self::Offset(offset) => offset.as_u64(),
            Self::Eof => None,
        }
    }

    pub fn as_usize(&self) -> Option<usize> {
        match self {
            Self::Beginning => None,
            Self::Offset(offset) => offset.as_usize(),
            Self::Eof => None,
        }
    }
}

impl From<i64> for Position {
    fn from(offset: i64) -> Self {
        assert!(offset >= 0);
        Self::from(format!("{offset:0>20}"))
    }
}

impl From<u64> for Position {
    fn from(offset: u64) -> Self {
        Self::from(format!("{offset:0>20}"))
    }
}

impl From<usize> for Position {
    fn from(offset: usize) -> Self {
        Self::from(format!("{offset:0>20}"))
    }
}

impl<T> From<Option<T>> for Position
where Position: From<T>
{
    fn from(offset_opt: Option<T>) -> Self {
        match offset_opt {
            Some(offset) => Self::from(offset),
            None => Self::Beginning,
        }
    }
}

impl PartialEq<i64> for Position {
    fn eq(&self, other: &i64) -> bool {
        self.as_i64() == Some(*other)
    }
}

impl PartialEq<u64> for Position {
    fn eq(&self, other: &u64) -> bool {
        self.as_u64() == Some(*other)
    }
}

impl PartialEq<usize> for Position {
    fn eq(&self, other: &usize) -> bool {
        self.as_usize() == Some(*other)
    }
}

impl From<String> for Position {
    fn from(string: String) -> Self {
        Self::from(ByteString::from(string))
    }
}

impl From<&'static str> for Position {
    fn from(string: &'static str) -> Self {
        Self::from(ByteString::from_static(string))
    }
}

impl From<ByteString> for Position {
    fn from(byte_string: ByteString) -> Self {
        if byte_string.is_empty() {
            Self::Beginning
        } else if byte_string == EOF {
            Self::Eof
        } else {
            Self::Offset(Offset(byte_string))
        }
    }
}

impl PartialEq<&str> for Position {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for Position {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == *other
    }
}

impl Serialize for Position {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Position {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let string = String::deserialize(deserializer)?;
        Ok(Self::from(string))
    }
}

impl prost::Message for Position {
    fn encode_raw<B>(&self, buf: &mut B)
    where B: prost::bytes::BufMut {
        prost::encoding::bytes::encode(1u32, &self.as_bytes(), buf);
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
        const STRUCT_NAME: &str = "Position";

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
                    .map_err(|_| DecodeError::new("position is not valid UTF-8"))?;
                *self = Self::from(byte_string);
                Ok(())
            }
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        prost::encoding::bytes::encoded_len(1u32, &self.as_bytes())
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;

    #[test]
    fn test_position_partial_eq() {
        assert_eq!(Position::Beginning, "");
        assert_eq!(Position::Beginning, "".to_string());
        assert_eq!(Position::from(0u64), 0i64);
        assert_eq!(Position::from(0u64), 0u64);
        assert_eq!(Position::from(0u64), 0usize);
    }

    #[test]
    fn test_position_json_serde_roundtrip() {
        let serialized = serde_json::to_string(&Position::Beginning).unwrap();
        assert_eq!(serialized, r#""""#);
        let deserialized: Position = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, Position::Beginning);

        let serialized = serde_json::to_string(&Position::from(0u64)).unwrap();
        assert_eq!(serialized, r#""00000000000000000000""#);
        let deserialized: Position = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, Position::from(0u64));

        let serialized = serde_json::to_string(&Position::Eof).unwrap();
        assert_eq!(serialized, r#""~eof""#);
        let deserialized: Position = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, Position::Eof);
    }

    #[test]
    fn test_position_prost_serde_roundtrip() {
        let mut buffer = Vec::new();
        Position::Beginning.encode(&mut buffer).unwrap();
        assert_eq!(
            Position::decode(Bytes::from(buffer)).unwrap(),
            Position::Beginning
        );

        let mut buffer = Vec::new();
        Position::from("0").encode(&mut buffer).unwrap();
        assert_eq!(
            Position::decode(Bytes::from(buffer)).unwrap(),
            Position::from("0")
        );

        let mut buffer = Vec::new();
        Position::from(0u64).encode(&mut buffer).unwrap();
        assert_eq!(
            Position::decode(Bytes::from(buffer)).unwrap(),
            Position::from(0u64)
        );

        let mut buffer = Vec::new();
        Position::Eof.encode(&mut buffer).unwrap();
        assert_eq!(
            Position::decode(Bytes::from(buffer)).unwrap(),
            Position::Eof
        );
    }
}
