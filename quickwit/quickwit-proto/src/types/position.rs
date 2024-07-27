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

use std::fmt::{Debug, Display};
use std::{fmt, mem};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;
use prost::{self, DecodeError};
use serde::{Deserialize, Serialize};

const BEGINNING: &str = "";

const EOF_PREFIX: &str = "~";

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

impl From<i64> for Offset {
    fn from(offset: i64) -> Self {
        Self(ByteString::from(format!("{offset:0>20}")))
    }
}

impl From<u64> for Offset {
    fn from(offset: u64) -> Self {
        Self(ByteString::from(format!("{offset:0>20}")))
    }
}

impl From<usize> for Offset {
    fn from(offset: usize) -> Self {
        Self(ByteString::from(format!("{offset:0>20}")))
    }
}

impl From<&str> for Offset {
    fn from(offset: &str) -> Self {
        Self(ByteString::from(offset))
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
#[derive(Clone, Default, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum Position {
    #[default]
    Beginning,
    Offset(Offset),
    /// End of partition/shard at the given offset. `Eof(None)` means no records were ever written.
    Eof(Option<Offset>),
}

impl Debug for Position {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Beginning => write!(f, "Position::Beginning"),
            // The derive implementation would show `Offset(Offset(0000001u64))` here.
            Self::Offset(offset) => write!(f, "Position::Offset({offset})"),
            Self::Eof(Some(offset)) => write!(f, "Position::Eof({offset})"),
            Self::Eof(None) => write!(f, "Position::Eof"),
        }
    }
}

impl Position {
    pub fn offset(offset: impl Into<Offset>) -> Self {
        Self::Offset(offset.into())
    }

    pub fn eof(offset: impl Into<Offset>) -> Self {
        Self::Eof(Some(offset.into()))
    }

    pub fn as_eof(&self) -> Self {
        match self {
            Self::Beginning => Self::Eof(None),
            Self::Offset(offset) => Self::Eof(Some(offset.clone())),
            _ => self.clone(),
        }
    }

    pub fn to_eof(&mut self) {
        match self {
            Self::Beginning => *self = Self::Eof(None),
            Self::Offset(offset) => *self = Self::Eof(Some(mem::take(offset))),
            _ => (),
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Offset(offset) | Self::Eof(Some(offset)) => offset.as_i64(),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Offset(offset) | Self::Eof(Some(offset)) => offset.as_u64(),
            _ => None,
        }
    }

    pub fn as_usize(&self) -> Option<usize> {
        match self {
            Self::Offset(offset) | Self::Eof(Some(offset)) => offset.as_usize(),
            _ => None,
        }
    }

    pub fn is_beginning(&self) -> bool {
        matches!(self, Self::Beginning)
    }

    pub fn is_eof(&self) -> bool {
        matches!(self, Self::Eof(_))
    }

    fn as_bytes(&self) -> Bytes {
        match self {
            Self::Beginning => Bytes::from_static(BEGINNING.as_bytes()),
            Self::Offset(offset) => offset.0.as_bytes().clone(),
            Self::Eof(Some(offset)) => {
                let mut bytes = BytesMut::with_capacity(EOF_PREFIX.len() + offset.0.len());
                bytes.extend_from_slice(EOF_PREFIX.as_bytes());
                bytes.extend_from_slice(offset.0.as_bytes());
                bytes.freeze()
            }
            Self::Eof(None) => Bytes::from_static(EOF_PREFIX.as_bytes()),
        }
    }
}

impl Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Beginning => write!(f, "{BEGINNING}"),
            Self::Offset(offset) => write!(f, "{offset}"),
            Self::Eof(Some(offset)) => write!(f, "{EOF_PREFIX}{offset}"),
            Self::Eof(None) => write!(f, "{EOF_PREFIX}"),
        }
    }
}

impl From<ByteString> for Position {
    fn from(position: ByteString) -> Self {
        match &position[..] {
            BEGINNING => Self::Beginning,
            EOF_PREFIX => Self::Eof(None),
            offset if offset.starts_with(EOF_PREFIX) => {
                let offset = ByteString::from(&offset[EOF_PREFIX.len()..]);
                Self::Eof(Some(Offset(offset)))
            }
            _ => Self::Offset(Offset(position)),
        }
    }
}

impl From<String> for Position {
    fn from(position: String) -> Self {
        Self::from(ByteString::from(position))
    }
}

impl Serialize for Position {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Position {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let position_str = String::deserialize(deserializer)?;
        Ok(Self::from(position_str))
    }
}

impl PartialEq<Position> for &Position {
    #[inline]
    fn eq(&self, other: &Position) -> bool {
        *self == other
    }
}

impl prost::Message for Position {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        prost::encoding::bytes::encode(1u32, &self.as_bytes(), buf);
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> ::core::result::Result<(), prost::DecodeError> {
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
    #[allow(clippy::cmp_owned)]
    fn test_position_ord() {
        assert!(Position::Beginning < Position::offset(0u64));
        assert!(Position::Beginning < Position::Eof(None));
        assert!(Position::Beginning < Position::eof(0u64));

        assert!(Position::offset(0u64) < Position::offset(1u64));

        assert!(Position::Eof(None) < Position::eof(0u64));
        assert!(Position::eof(0u64) < Position::eof(1u64));
    }

    #[test]
    fn test_position_as_eof() {
        let eof_position = Position::Beginning.as_eof();

        assert!(eof_position.is_eof());
        assert!(eof_position.as_u64().is_none());

        let eof_position = Position::offset(0u64).as_eof();

        assert!(eof_position.is_eof());
        assert_eq!(eof_position.as_u64().unwrap(), 0u64);
    }

    #[test]
    fn test_position_to_eof() {
        let mut position = Position::Beginning;
        position.to_eof();
        assert!(matches!(position, Position::Eof(None)));

        let mut position = Position::offset(0u64);
        position.to_eof();
        assert!(matches!(position, Position::Eof(Some(offset)) if offset.as_u64().unwrap() == 0));
    }

    #[test]
    fn test_position_json_serde_roundtrip() {
        let serialized = serde_json::to_string(&Position::Beginning).unwrap();
        assert_eq!(serialized, r#""""#);
        let deserialized: Position = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, Position::Beginning);

        let serialized = serde_json::to_string(&Position::offset(0u64)).unwrap();
        assert_eq!(serialized, r#""00000000000000000000""#);
        let deserialized: Position = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, Position::offset(0u64));

        let serialized = serde_json::to_string(&Position::Eof(None)).unwrap();
        assert_eq!(serialized, r#""~""#);
        let deserialized: Position = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, Position::Eof(None));

        let serialized = serde_json::to_string(&Position::eof(0u64)).unwrap();
        assert_eq!(serialized, r#""~00000000000000000000""#);
        let deserialized: Position = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, Position::eof(0u64));
    }

    #[test]
    fn test_position_prost_serde_roundtrip() {
        let encoded = Position::Beginning.encode_to_vec();
        assert_eq!(
            Position::decode(Bytes::from(encoded)).unwrap(),
            Position::Beginning
        );
        let encoded = Position::Beginning.encode_length_delimited_to_vec();
        assert_eq!(
            Position::decode_length_delimited(Bytes::from(encoded)).unwrap(),
            Position::Beginning
        );

        let encoded = Position::offset(0u64).encode_to_vec();
        assert_eq!(
            Position::decode(Bytes::from(encoded)).unwrap(),
            Position::offset(0u64)
        );
        let encoded = Position::offset(0u64).encode_length_delimited_to_vec();
        assert_eq!(
            Position::decode_length_delimited(Bytes::from(encoded)).unwrap(),
            Position::offset(0u64)
        );

        let encoded = Position::Eof(None).encode_to_vec();
        assert_eq!(
            Position::decode(Bytes::from(encoded)).unwrap(),
            Position::Eof(None)
        );
        let encoded = Position::Eof(None).encode_length_delimited_to_vec();
        assert_eq!(
            Position::decode_length_delimited(Bytes::from(encoded)).unwrap(),
            Position::Eof(None)
        );

        let encoded = Position::eof(0u64).encode_to_vec();
        assert_eq!(
            Position::decode(Bytes::from(encoded)).unwrap(),
            Position::eof(0u64)
        );
        let encoded = Position::eof(0u64).encode_length_delimited_to_vec();
        assert_eq!(
            Position::decode_length_delimited(Bytes::from(encoded)).unwrap(),
            Position::eof(0u64)
        );
    }
}
