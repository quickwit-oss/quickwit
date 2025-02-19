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

use std::ops::{Bound, RangeBounds, RangeInclusive};

use tantivy::columnar::Cardinality;
use tantivy::fastfield::Column;
use tantivy::{DateTime, DocId, SegmentReader};

/// A filter that only retains docs within a time range.
#[derive(Clone)]
pub struct TimestampFilter {
    /// The time range represented as (lower_bound, upper_bound).
    // TODO replace this with a RangeInclusive<DateTime> if it improves perf?
    time_range: (Bound<DateTime>, Bound<DateTime>),
    timestamp_column: Column<DateTime>,
}

impl TimestampFilter {
    #[inline]
    pub fn contains_timestamp(&self, ts: &DateTime) -> bool {
        self.time_range.contains(ts)
    }

    #[inline]
    /// Fetches the timestamp of a given doc from the column storage and checks if it is within the
    /// time range.
    pub fn contains_doc_timestamp(&self, doc_id: DocId) -> bool {
        if let Some(ts) = self.timestamp_column.first(doc_id) {
            self.contains_timestamp(&ts)
        } else {
            false
        }
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

    /// None means that all documents are matching the timestamp range.
    pub fn build(
        &self,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Option<TimestampFilter>> {
        let timestamp_column_opt: Option<Column<DateTime>> =
            segment_reader
                .fast_fields()
                .column_opt::<DateTime>(&self.timestamp_field_name)?;
        let timestamp_column = timestamp_column_opt
            .unwrap_or_else(|| Column::build_empty_column(segment_reader.max_doc()));
        let time_range = (self.start_timestamp, self.end_timestamp);
        if time_range == (Bound::Unbounded, Bound::Unbounded) {
            return Ok(None);
        }
        if timestamp_column.index.get_cardinality() == Cardinality::Full {
            let segment_range: RangeInclusive<DateTime> =
                timestamp_column.min_value()..=timestamp_column.max_value();
            if is_segment_always_within_timestamp_range(segment_range, time_range) {
                return Ok(None);
            }
        }
        Ok(Some(TimestampFilter {
            time_range,
            timestamp_column,
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
