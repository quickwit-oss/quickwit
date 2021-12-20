// Copyright (C) 2021 Quickwit, Inc.
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

use tantivy::fastfield::{DynamicFastFieldReader, FastFieldReader};
use tantivy::schema::{Field, Type};
use tantivy::{DocId, SegmentReader, TantivyError};

/// A filter that only retains docs within a time range.
#[derive(Clone)]
pub struct TimestampFilter {
    /// The time range represented as (lower_bound, upper_bound).
    time_range: (Bound<i64>, Bound<i64>),
    /// The timestamp fast field reader.
    timestamp_field_reader: DynamicFastFieldReader<i64>,
}

impl TimestampFilter {
    pub fn new(
        field: Field,
        start_timestamp_opt: Option<i64>,
        end_timestamp_opt: Option<i64>,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Option<Self>> {
        let field_entry = segment_reader.schema().get_field_entry(field);

        let expected_type = Type::I64;
        let field_schema_type = field_entry.field_type().value_type();
        if expected_type != field_schema_type {
            return Err(TantivyError::SchemaError(format!(
                "Field {:?} is of type {:?}!={:?}",
                field_entry.name(),
                expected_type,
                field_schema_type
            )));
        }

        let timestamp_field_reader = segment_reader.fast_fields().i64(field)?;
        let segment_range = (
            timestamp_field_reader.min_value(),
            timestamp_field_reader.max_value(),
        );
        let timestamp_range = (
            start_timestamp_opt.unwrap_or(i64::MIN),
            end_timestamp_opt.unwrap_or(i64::MAX),
        );
        if is_segment_always_within_timestamp_range(segment_range, timestamp_range) {
            return Ok(None);
        }

        let lower_bound = start_timestamp_opt
            .map(Bound::Included)
            .unwrap_or(Bound::Unbounded);
        let upper_bound = end_timestamp_opt
            .map(Bound::Excluded)
            .unwrap_or(Bound::Unbounded);

        Ok(Some(TimestampFilter {
            time_range: (lower_bound, upper_bound),
            timestamp_field_reader,
        }))
    }

    pub fn is_within_range(&self, doc_id: DocId) -> bool {
        let timestamp_value = self.timestamp_field_reader.get(doc_id);
        self.time_range.contains(&timestamp_value)
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
