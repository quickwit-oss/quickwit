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

use std::cmp::Ordering;
use std::fmt;
use std::io::{self, Read};

use prost::Message;
use quickwit_common::numeric_types::num_proj::ProjectedNumber;
use quickwit_common::numeric_types::{num_cmp, num_proj};
pub use sort_by_value::SortValue;

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
            Some(SortValue::Str(s)) => String(s),
            Some(SortValue::Datetime(dt)) => Number(dt.into()),
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
            String(value) => Some(SortValue::Str(value)),
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
        match (self, other) {
            // Same types.
            (SortValue::U64(left), SortValue::U64(right)) => left.cmp(right),
            (SortValue::I64(left), SortValue::I64(right)) => left.cmp(right),
            (SortValue::Boolean(left), SortValue::Boolean(right)) => left.cmp(right),
            (SortValue::Str(left), SortValue::Str(right)) => left.cmp(right),
            (SortValue::F64(left), SortValue::F64(right)) => left.total_cmp(right),
            (SortValue::Datetime(left), SortValue::Datetime(right)) => left.cmp(right),
            // Different numeric types but can still be compared.
            (SortValue::U64(left), SortValue::F64(right)) => {
                num_cmp::cmp_u64_f64(*left, *right).expect("unexpected float comparison")
            }
            (SortValue::F64(left), SortValue::U64(right)) => num_cmp::cmp_u64_f64(*right, *left)
                .expect("unexpected float comparison")
                .reverse(),
            (SortValue::I64(left), SortValue::F64(right)) => {
                num_cmp::cmp_i64_f64(*left, *right).expect("unexpected float comparison")
            }
            (SortValue::F64(left), SortValue::I64(right)) => num_cmp::cmp_i64_f64(*right, *left)
                .expect("unexpected float comparison")
                .reverse(),
            (SortValue::I64(left), SortValue::U64(right)) => num_cmp::cmp_i64_u64(*left, *right),
            (SortValue::U64(left), SortValue::I64(right)) => {
                num_cmp::cmp_i64_u64(*right, *left).reverse()
            }
            // Incompatible types, they are sorted one after another.
            (left, right) => left.type_sort_key().cmp(&right.type_sort_key()),
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
        match &this {
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
            SortValue::Str(s) => {
                s.hash(state);
            }
            SortValue::Datetime(dt) => {
                dt.hash(state);
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
            SortValue::I64(_) => self.clone(),
            SortValue::Boolean(_) => self.clone(),
            SortValue::Str(_) => self.clone(),
            SortValue::U64(number) => match num_proj::u64_to_i64(*number) {
                ProjectedNumber::Exact(number) => SortValue::I64(number),
                _ => self.clone(),
            },
            SortValue::F64(float) => match num_proj::f64_to_i64(*float) {
                ProjectedNumber::Exact(number) => SortValue::I64(number),
                ProjectedNumber::AfterLast => {
                    if let ProjectedNumber::Exact(number) = num_proj::f64_to_u64(*float) {
                        SortValue::U64(number)
                    } else {
                        self.clone()
                    }
                }
                _ => self.clone(),
            },
            SortValue::Datetime(_) => self.clone(),
        }
    }

    pub fn type_sort_key(&self) -> TypeSortKey {
        match self {
            SortValue::U64(_) => TypeSortKey::Numeric,
            SortValue::I64(_) => TypeSortKey::Numeric,
            SortValue::F64(_) => TypeSortKey::Numeric,
            SortValue::Boolean(_) => TypeSortKey::Boolean,
            SortValue::Str(_) => TypeSortKey::Str,
            SortValue::Datetime(_) => TypeSortKey::DateTime,
        }
    }
}

impl PartialHit {
    /// Helper to get access to the 1st sort value
    pub fn sort_value(&self) -> Option<SortValue> {
        if let Some(sort_value) = &self.sort_value {
            sort_value.sort_value.clone()
        } else {
            None
        }
    }
}

/// Defines the order between types when sorting on a field with multiple types.
/// Expected order:
/// - Asc: numeric -> string -> boolean -> datetime
/// - Desc: datetime -> boolean -> string -> numeric
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TypeSortKey {
    Numeric,
    Str,
    Boolean,
    DateTime,
}

/// Serializes the Split fields.
///
/// `fields_metadata` has to be sorted.
pub fn serialize_split_fields(list_fields: ListFields) -> Vec<u8> {
    let payload = list_fields.encode_to_vec();
    let compression_level = 3;
    let payload_compressed = zstd::stream::encode_all(&mut &payload[..], compression_level)
        .expect("zstd encoding failed");
    let mut out = Vec::new();
    // Write Header -- Format Version 2
    let format_version = 2u8;
    out.push(format_version);
    // Write Payload
    out.extend_from_slice(&payload_compressed);
    out
}

/// Reads a fixed number of bytes into an array and returns the array.
fn read_exact_array<const N: usize>(reader: &mut impl Read) -> io::Result<[u8; N]> {
    let mut buffer = [0u8; N];
    reader.read_exact(&mut buffer)?;
    Ok(buffer)
}

/// Reads the Split fields from a zstd compressed stream of bytes
pub fn deserialize_split_fields<R: Read>(mut reader: R) -> io::Result<ListFields> {
    let format_version = read_exact_array::<1>(&mut reader)?[0];
    if format_version != 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Unsupported split field format version: {format_version}"),
        ));
    }
    let reader = zstd::Decoder::new(reader)?;
    read_split_fields_from_zstd(reader)
}

/// Reads the Split fields from a stream of bytes
#[allow(clippy::unbuffered_bytes)]
fn read_split_fields_from_zstd<R: Read>(reader: R) -> io::Result<ListFields> {
    let all_bytes: Vec<_> = reader.bytes().collect::<io::Result<_>>()?;
    let serialized_list_fields: ListFields = prost::Message::decode(&all_bytes[..])?;

    Ok(serialized_list_fields)
}
