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

//! A short-lived adapter: Tantivy copies directly into its owned compact document.
use serde_json_borrow::{KeyStrType, Value as JsonValue};
use tantivy::schema::document::{ReferenceValue, ReferenceValueLeaf, Value};
use tantivy::time::OffsetDateTime;
use tantivy::time::format_description::well_known::Rfc3339;

#[derive(Clone, Copy, Debug)]
pub(super) struct BorrowedValue<'a>(pub &'a JsonValue<'a>);

pub(super) struct ArrayIter<'a>(std::slice::Iter<'a, JsonValue<'a>>);
impl<'a> Iterator for ArrayIter<'a> {
    type Item = BorrowedValue<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(BorrowedValue)
    }
}
pub(super) struct ObjectIter<'a>(std::slice::Iter<'a, (KeyStrType<'a>, JsonValue<'a>)>);
impl<'a> Iterator for ObjectIter<'a> {
    type Item = (&'a str, BorrowedValue<'a>);
    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|(key, value)| (key.as_ref(), BorrowedValue(value)))
    }
}

impl<'a> Value<'a> for BorrowedValue<'a> {
    type ArrayIter = ArrayIter<'a>;
    type ObjectIter = ObjectIter<'a>;
    fn as_value(&self) -> ReferenceValue<'a, Self> {
        match self.0 {
            JsonValue::Null => ReferenceValueLeaf::Null.into(),
            JsonValue::Bool(value) => ReferenceValueLeaf::Bool(*value).into(),
            JsonValue::Number(number) => {
                // Match Tantivy's serde_json conversion: i64 before u64, then f64.
                if let Some(value) = number.as_i64() {
                    ReferenceValueLeaf::I64(value).into()
                } else if let Some(value) = number.as_u64() {
                    ReferenceValueLeaf::U64(value).into()
                } else {
                    ReferenceValueLeaf::F64(number.as_f64().expect("finite JSON number")).into()
                }
            }
            JsonValue::Str(text) => {
                // Same gate and parser as Tantivy's OwnedValue::from(serde_json::Value).
                // In particular, do not trim whitespace or use Quickwit's configured date parser.
                if matches!(text.as_bytes().first(), Some(byte) if byte.is_ascii_digit())
                    && let Ok(date) = OffsetDateTime::parse(text, &Rfc3339)
                {
                    return ReferenceValueLeaf::Date(tantivy::DateTime::from_utc(
                        date.to_offset(tantivy::time::UtcOffset::UTC),
                    ))
                    .into();
                }
                ReferenceValueLeaf::Str(text).into()
            }
            JsonValue::Array(values) => ReferenceValue::Array(ArrayIter(values.iter())),
            JsonValue::Object(object) => ReferenceValue::Object(ObjectIter(object.as_vec().iter())),
        }
    }
}
