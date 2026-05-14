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

mod span_id;
mod trace_id;

use std::cmp::Ordering;
use std::fmt;
use std::io::{self, Read};

use prost::Message;
pub use sort_by_value::SortValue;
pub use span_id::{SpanId, TryFromSpanIdError};
pub use trace_id::{TraceId, TryFromTraceIdError};

include!("../codegen/quickwit/quickwit.search.rs");

pub const SEARCH_FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!("../codegen/quickwit/search_descriptor.bin");

impl SearchRequest {
    pub fn time_range(&self) -> impl std::ops::RangeBounds<i64> {
        use std::ops::Bound;
        (
            self.start_timestamp
                .map_or(Bound::Unbounded, Bound::Included),
            self.end_timestamp.map_or(Bound::Unbounded, Bound::Excluded),
        )
    }
}

impl SplitIdAndFooterOffsets {
    pub fn time_range(&self) -> impl std::ops::RangeBounds<i64> {
        use std::ops::Bound;
        (
            self.timestamp_start
                .map_or(Bound::Unbounded, Bound::Included),
            self.timestamp_end.map_or(Bound::Unbounded, Bound::Included),
        )
    }
}

impl fmt::Display for SplitSearchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, split_id: {})", self.error, self.split_id)
    }
}

impl Eq for SortByValue {}

impl From<SortValue> for SortByValue {
    fn from(sort_value: SortValue) -> Self {
        SortByValue {
            sort_value: Some(sort_value),
        }
    }
}

impl std::hash::Hash for SortByValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.sort_value.hash(state);
    }
}

impl SortByValue {
    pub fn into_json(self) -> serde_json::Value {
        use serde_json::Value::*;
        match self.sort_value {
            Some(SortValue::U64(num)) => Number(num.into()),
            Some(SortValue::I64(num)) => Number(num.into()),
            Some(SortValue::F64(num)) => {
                if let Some(num) = serde_json::Number::from_f64(num) {
                    Number(num)
                } else {
                    // TODO is there a better way to handle infinite/nan?
                    Null
                }
            }
            Some(SortValue::Boolean(b)) => Bool(b),
            None => Null,
        }
    }

    pub fn try_from_json(value: serde_json::Value) -> Option<Self> {
        use serde_json::Value::*;
        let sort_value = match value {
            Null => None,
            Bool(b) => Some(SortValue::Boolean(b)),
            Number(number) => {
                if let Some(number) = number.as_u64() {
                    Some(SortValue::U64(number))
                } else if let Some(number) = number.as_i64() {
                    Some(SortValue::I64(number))
                } else if let Some(number) = number.as_f64() {
                    Some(SortValue::F64(number))
                } else {
                    // this should never happen as we don't emit such number ourselves
                    return None;
                }
            }
            // Strings that can be converted to a number are accepted.
            // Some clients (like JS clients) can't easily handle large integers
            // without losing precision, so we accept them as strings.
            String(value) => {
                if let Ok(number) = value.parse::<i64>() {
                    Some(SortValue::I64(number))
                } else if let Ok(number) = value.parse::<u64>() {
                    Some(SortValue::U64(number))
                } else {
                    return None;
                }
            }
            Array(_) | Object(_) => return None,
        };
        Some(SortByValue { sort_value })
    }
}

// !!! Disclaimer !!!
//
// Prost imposes the PartialEq derived implementation.
// This is terrible because this means Eq, PartialEq are not really in line with Ord's
// implementation. if in presence of NaN.
impl Eq for SortValue {}

impl Ord for SortValue {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        // We make sure to end up with a total order.
        match (*self, *other) {
            // Same types.
            (SortValue::U64(left), SortValue::U64(right)) => left.cmp(&right),
            (SortValue::I64(left), SortValue::I64(right)) => left.cmp(&right),
            (SortValue::Boolean(left), SortValue::Boolean(right)) => left.cmp(&right),
            // We half the logic by making sure we keep
            // the "stronger" type on the left.
            (SortValue::U64(left), SortValue::I64(right)) => {
                if left > i64::MAX as u64 {
                    return Ordering::Greater;
                }
                (left as i64).cmp(&right)
            }
            (SortValue::F64(left), SortValue::F64(right)) => left.total_cmp(&right),
            (SortValue::F64(left), SortValue::U64(right)) => left.total_cmp(&(right as f64)),
            (SortValue::F64(left), SortValue::I64(right)) => left.total_cmp(&(right as f64)),
            (SortValue::Boolean(left), right) => SortValue::U64(left as u64).cmp(&right),
            (left, right) => right.cmp(&left).reverse(),
        }
    }
}

impl PartialOrd for SortValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for SortValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let this = self.normalize();
        std::mem::discriminant(&this).hash(state);
        match this {
            SortValue::U64(number) => {
                number.hash(state);
            }
            SortValue::I64(number) => {
                number.hash(state);
            }
            SortValue::F64(number) => {
                number.to_bits().hash(state);
            }
            SortValue::Boolean(b) => {
                b.hash(state);
            }
        }
    }
}

impl SortValue {
    /// Where multiple variant could represent the same logical value, convert to a canonical form.
    ///
    /// For number, we prefer to represent them, in order, as i64, then as u64 and finally as f64.
    pub fn normalize(&self) -> Self {
        match self {
            SortValue::I64(_) => *self,
            SortValue::Boolean(_) => *self,
            SortValue::U64(number) => {
                if let Ok(number) = (*number).try_into() {
                    SortValue::I64(number)
                } else {
                    *self
                }
            }
            SortValue::F64(number) => {
                let number = *number;
                if number.ceil() == number {
                    // number is not NaN, and is a natural number
                    if number >= i64::MIN as f64 && number <= i64::MAX as f64 {
                        return SortValue::I64(number as i64);
                    } else if number.is_sign_positive() && number <= u64::MAX as f64 {
                        return SortValue::U64(number as u64);
                    }
                }
                *self
            }
        }
    }
}

impl PartialHit {
    /// Helper to get access to the 1st sort value
    pub fn sort_value(&self) -> Option<SortValue> {
        if let Some(sort_value) = self.sort_value {
            sort_value.sort_value
        } else {
            None
        }
    }
}

/// On-disk format version for serialized [`ListFieldsMetadata`]. Bumped whenever the wire format
/// produced by [`ListFieldsMetadata::serialize`] changes in a way readers can't tolerate.
const SPLIT_FIELDS_FORMAT_VERSION: u8 = 2;

/// Zstd compression level used when writing split fields.
const SPLIT_FIELDS_COMPRESSION_LEVEL: i32 = 3;

impl ListFieldsMetadata {
    /// Serializes the entries: one version byte followed by the zstd-compressed protobuf
    /// encoding of `Self`.
    pub fn serialize(&self) -> Vec<u8> {
        let payload = self.encode_to_vec();
        let mut out = vec![SPLIT_FIELDS_FORMAT_VERSION];
        zstd::stream::copy_encode(&payload[..], &mut out, SPLIT_FIELDS_COMPRESSION_LEVEL)
            .expect("zstd encoding into `Vec<u8>` should not fail");
        out
    }

    /// Reads the format produced by [`Self::serialize`].
    pub fn deserialize<R: Read>(mut reader: R) -> io::Result<Self> {
        let mut version_byte = [0u8; 1];
        reader.read_exact(&mut version_byte)?;

        if version_byte[0] != SPLIT_FIELDS_FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported split fields format version: {}",
                    version_byte[0]
                ),
            ));
        }
        let mut zstd_decoder = zstd::stream::read::Decoder::new(reader)?;
        let mut decompressed = Vec::new();
        zstd_decoder.read_to_end(&mut decompressed)?;

        Self::decode(&decompressed[..])
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
    }
}

impl ListFieldsEntry {
    pub fn cmp_by_name_and_type(&self, other: &Self) -> Ordering {
        self.field_name
            .cmp(&other.field_name)
            .then_with(|| self.field_type.cmp(&other.field_type))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(field_name: &str) -> ListFieldsEntry {
        ListFieldsEntry {
            field_name: field_name.to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: true,
            aggregatable: true,
            index_ids: vec!["index-1".to_string()],
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
        }
    }

    #[test]
    fn list_fields_entries_roundtrip() {
        let entries = ListFieldsMetadata {
            entries: vec![entry("a"), entry("b"), entry("c")],
        };
        let buf = entries.serialize();
        let decoded = ListFieldsMetadata::deserialize(&buf[..]).unwrap();
        assert_eq!(decoded, entries);
    }

    #[test]
    fn list_fields_entries_empty_roundtrip() {
        let entries = ListFieldsMetadata {
            entries: Vec::new(),
        };
        let buf = entries.serialize();
        // Just the version byte plus an (essentially empty) zstd frame.
        assert_eq!(buf[0], SPLIT_FIELDS_FORMAT_VERSION);
        let decoded = ListFieldsMetadata::deserialize(&buf[..]).unwrap();
        assert_eq!(decoded, entries);
    }

    #[test]
    fn list_fields_entries_wire_compatible_with_encode() {
        // `serialize` must produce the same compressed payload as a one-shot `encode_to_vec`,
        // so existing readers / on-disk snapshots stay compatible.
        let entries = ListFieldsMetadata {
            entries: vec![entry("a"), entry("b")],
        };
        let actual = entries.serialize();

        let one_shot_encoded = entries.encode_to_vec();
        let mut expected = vec![SPLIT_FIELDS_FORMAT_VERSION];
        let compressed =
            zstd::stream::encode_all(&one_shot_encoded[..], SPLIT_FIELDS_COMPRESSION_LEVEL)
                .unwrap();
        expected.extend_from_slice(&compressed);

        assert_eq!(actual, expected);
    }

    #[test]
    fn list_fields_entries_rejects_unknown_version() {
        let buf = [0xFFu8];
        let err = ListFieldsMetadata::deserialize(&buf[..]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
