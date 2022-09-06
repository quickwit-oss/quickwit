// Copyright (C) 2022 Quickwit, Inc.
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

use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use tantivy::fastfield::Column;
use tantivy::schema::{Field, Type};
use tantivy::{DateTime, DocId, SegmentReader, TantivyError};

#[derive(Clone)]
enum GenericFastFieldReader {
    I64(Arc<dyn Column<i64>>),
    Date(Arc<dyn Column<DateTime>>),
}

impl GenericFastFieldReader {
    fn min_value(&self) -> i64 {
        match self {
            GenericFastFieldReader::I64(fast_reader) => fast_reader.min_value(),
            GenericFastFieldReader::Date(fast_reader) => {
                fast_reader.min_value().into_timestamp_secs()
            }
        }
    }

    fn max_value(&self) -> i64 {
        match self {
            GenericFastFieldReader::I64(fast_reader) => fast_reader.max_value(),
            GenericFastFieldReader::Date(fast_reader) => {
                fast_reader.max_value().into_timestamp_secs()
            }
        }
    }

    fn get(&self, doc_id: DocId) -> i64 {
        match self {
            GenericFastFieldReader::I64(fast_reader) => fast_reader.get_val(doc_id as u64),
            GenericFastFieldReader::Date(fast_reader) => {
                fast_reader.get_val(doc_id as u64).into_timestamp_secs()
            }
        }
    }
}

/// A filter that only retains docs within a time range.
#[derive(Clone)]
pub struct TimestampFilter {
    /// The time range represented as (lower_bound, upper_bound).
    time_range: (Bound<i64>, Bound<i64>),
    /// The timestamp fast field reader.
    timestamp_field_reader: GenericFastFieldReader,
}

impl TimestampFilter {
    pub fn is_within_range(&self, doc_id: DocId) -> bool {
        let timestamp_value = self.timestamp_field_reader.get(doc_id);
        self.time_range.contains(&timestamp_value)
    }
}

#[derive(Clone, Debug)]
pub struct TimestampFilterBuilder {
    pub timestamp_field_name: String,
    timestamp_field: Field,
    start_timestamp_opt: Option<i64>,
    end_timestamp_opt: Option<i64>,
}

impl TimestampFilterBuilder {
    pub fn new(
        timestamp_field_name_opt: Option<String>,
        timestamp_field_opt: Option<Field>,
        start_timestamp_opt: Option<i64>,
        end_timestamp_opt: Option<i64>,
    ) -> Option<TimestampFilterBuilder> {
        let timestamp_field_name = timestamp_field_name_opt?;
        let timestamp_field = timestamp_field_opt?;
        if start_timestamp_opt.is_none() && end_timestamp_opt.is_none() {
            return None;
        }
        Some(TimestampFilterBuilder {
            timestamp_field_name,
            timestamp_field,
            start_timestamp_opt,
            end_timestamp_opt,
        })
    }

    pub fn build(
        &self,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Option<TimestampFilter>> {
        let field_entry = segment_reader
            .schema()
            .get_field_entry(self.timestamp_field);

        let field_schema_type = field_entry.field_type().value_type();
        let timestamp_field_reader = match field_entry.field_type().value_type() {
            Type::I64 => {
                GenericFastFieldReader::I64(segment_reader.fast_fields().i64(self.timestamp_field)?)
            }
            Type::Date => GenericFastFieldReader::Date(
                segment_reader.fast_fields().date(self.timestamp_field)?,
            ),
            _ => {
                return Err(TantivyError::SchemaError(format!(
                    "Failed to build timestamp filter for field `{:?}`: expected I64 or Date \
                     type, got `{:?}`.",
                    field_entry.name(),
                    field_schema_type
                )))
            }
        };

        let segment_range = (
            timestamp_field_reader.min_value(),
            timestamp_field_reader.max_value(),
        );
        let timestamp_range = (
            self.start_timestamp_opt.unwrap_or(i64::MIN),
            self.end_timestamp_opt.unwrap_or(i64::MAX),
        );
        if is_segment_always_within_timestamp_range(segment_range, timestamp_range) {
            return Ok(None);
        }

        let lower_bound = self
            .start_timestamp_opt
            .map(Bound::Included)
            .unwrap_or(Bound::Unbounded);
        let upper_bound = self
            .end_timestamp_opt
            .map(Bound::Excluded)
            .unwrap_or(Bound::Unbounded);

        Ok(Some(TimestampFilter {
            time_range: (lower_bound, upper_bound),
            timestamp_field_reader,
        }))
    }
}

/// Determine if all docs of a segment always satisfy the requested timestamp range.
///
/// Note:
/// - segment_range: is an inclusive range on both ends `[min, max]`.
/// - timestamp_range: is a half open range `[min, max[`.
fn is_segment_always_within_timestamp_range(
    segment_range: (i64, i64),
    timestamp_range: (i64, i64),
) -> bool {
    segment_range.0 >= timestamp_range.0 && segment_range.1 < timestamp_range.1
}

#[cfg(test)]
mod tests {
    use super::is_segment_always_within_timestamp_range;

    #[test]
    fn test_is_segment_always_within_timestamp_range() {
        assert_eq!(
            is_segment_always_within_timestamp_range((20, 30), (i64::MIN, i64::MAX)),
            true
        );

        assert_eq!(
            is_segment_always_within_timestamp_range((20, 30), (20, 35)),
            true
        );

        assert_eq!(
            is_segment_always_within_timestamp_range((20, 30), (20, 25)),
            false
        );

        assert_eq!(
            is_segment_always_within_timestamp_range((20, 30), (20, 30)),
            false
        );

        assert_eq!(
            is_segment_always_within_timestamp_range((i64::MIN, i64::MAX), (i64::MIN, i64::MAX)),
            false
        );
    }
}
