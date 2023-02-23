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

use std::ops::{Bound, RangeBounds, RangeInclusive};
use std::sync::Arc;

use fastfield_codecs::Column;
use tantivy::{DateTime, DocId, SegmentReader};

/// A filter that only retains docs within a time range.
#[derive(Clone)]
pub struct TimestampFilter {
    /// The time range represented as (lower_bound, upper_bound).
    // TODO replace this with a RangeInclusive<DateTime> if it improves perf?
    time_range: (Bound<DateTime>, Bound<DateTime>),
    /// The timestamp fast field reader.
    time_column: Arc<dyn Column<DateTime>>,
}

impl TimestampFilter {
    pub fn is_within_range(&self, doc_id: DocId) -> bool {
        let timestamp_value: DateTime = self.time_column.get_val(doc_id);
        self.time_range.contains(&timestamp_value)
    }
}

/// Creates a timestamp field depending on the user request.
///
/// The start/end timestamp are in seconds and are interpreted as
/// a semi-open interval [start, end).
pub fn create_timestamp_filter_builder(
    timestamp_field_opt: Option<&str>,
    start_timestamp_secs: Option<i64>,
    end_timestamp_secs: Option<i64>,
) -> Option<TimestampFilterBuilder> {
    let timestamp_field = timestamp_field_opt?;
    if start_timestamp_secs.is_none() && end_timestamp_secs.is_none() {
        return None;
    }
    let start_timestamp_bound: Bound<DateTime> = start_timestamp_secs
        .map(|timestamp_secs| Bound::Included(DateTime::from_timestamp_secs(timestamp_secs)))
        .unwrap_or(Bound::Unbounded);
    let end_timestamp_bound: Bound<DateTime> = end_timestamp_secs
        .map(|timestamp_secs| Bound::Excluded(DateTime::from_timestamp_secs(timestamp_secs)))
        .unwrap_or(Bound::Unbounded);
    Some(TimestampFilterBuilder::new(
        timestamp_field.to_string(),
        start_timestamp_bound,
        end_timestamp_bound,
    ))
}

#[derive(Clone, Debug)]
pub struct TimestampFilterBuilder {
    pub timestamp_field_name: String,
    start_timestamp: Bound<DateTime>,
    end_timestamp: Bound<DateTime>,
}

impl TimestampFilterBuilder {
    pub fn new(
        timestamp_field_name: String,
        start_timestamp: Bound<DateTime>,
        end_timestamp: Bound<DateTime>,
    ) -> TimestampFilterBuilder {
        TimestampFilterBuilder {
            timestamp_field_name,
            start_timestamp,
            end_timestamp,
        }
    }

    pub fn build(
        &self,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Option<TimestampFilter>> {
        let timestamp_field_reader = segment_reader
            .fast_fields()
            .date(&self.timestamp_field_name)?;
        let segment_range: RangeInclusive<DateTime> =
            timestamp_field_reader.min_value()..=timestamp_field_reader.max_value();
        let time_range = (self.start_timestamp, self.end_timestamp);
        if is_segment_always_within_timestamp_range(segment_range, time_range) {
            return Ok(None);
        }
        Ok(Some(TimestampFilter {
            time_range,
            time_column: timestamp_field_reader,
        }))
    }
}

/// Determine if all docs of a segment always satisfy the requested timestamp range.
///
/// Note:
/// - segment_range: is an inclusive range on both ends `[min, max]`.
/// - timestamp_range: is a half open range `[min, max[`.
fn is_segment_always_within_timestamp_range(
    segment_range: RangeInclusive<DateTime>,
    timestamp_range: impl RangeBounds<DateTime>,
) -> bool {
    timestamp_range.contains(segment_range.start()) && timestamp_range.contains(segment_range.end())
}

#[cfg(test)]
mod tests {
    use tantivy::DateTime;

    use super::is_segment_always_within_timestamp_range;

    const TEST_START: DateTime = DateTime::from_timestamp_secs(1_662_529_435);
    const TEST_MIDDLE: DateTime = DateTime::from_timestamp_secs(1_662_629_435);
    const TEST_END: DateTime = DateTime::from_timestamp_secs(1_662_639_435);

    #[test]
    fn test_is_segment_always_within_timestamp_range() {
        assert_eq!(
            is_segment_always_within_timestamp_range(TEST_START..=TEST_END, ..),
            true
        );

        assert_eq!(
            is_segment_always_within_timestamp_range(
                TEST_START..=TEST_MIDDLE,
                TEST_START..TEST_END
            ),
            true
        );

        assert_eq!(
            is_segment_always_within_timestamp_range(
                TEST_START..=TEST_END,
                TEST_START..TEST_MIDDLE
            ),
            false
        );

        assert_eq!(
            is_segment_always_within_timestamp_range(TEST_START..=TEST_END, TEST_START..TEST_END),
            false
        );
    }
}
