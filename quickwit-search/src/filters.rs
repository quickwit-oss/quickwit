// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::ops::{Bound, RangeBounds};

use tantivy::fastfield::{DynamicFastFieldReader, FastFieldReader};
use tantivy::schema::Type;
use tantivy::DocId;
use tantivy::{schema::Field, SegmentReader, TantivyError};

/// A filter that only retains docs within a time range.
#[derive(Clone)]
pub struct TimestampFilter {
    /// The field holding the document timestamp value.
    field: Field,
    /// The time range respresented as (lower_bound, upper_bound).
    time_range: (Bound<i64>, Bound<i64>),
    /// The timestamp fast field reader.
    // TODO should be i64 when (https://github.com/tantivy-search/tantivy/issues/1084) is resolved.
    timestamp_field_reader: DynamicFastFieldReader<u64>,
}

impl TimestampFilter {
    pub fn new(
        field: Field,
        start_timestamp_opt: Option<i64>,
        end_timestamp_opt: Option<i64>,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Self> {
        let field_entry = segment_reader.schema().get_field_entry(field);
        if !field_entry.is_fast() {
            return Err(TantivyError::SchemaError(format!(
                "Field {:?} is not a fast field.",
                field_entry.name()
            )));
        }

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

        let timestamp_field_reader = segment_reader.fast_fields().u64(field)?;
        let lower_bound = start_timestamp_opt
            .map(Bound::Included)
            .unwrap_or(Bound::Unbounded);
        let upper_bound = end_timestamp_opt
            .map(Bound::Excluded)
            .unwrap_or(Bound::Unbounded);

        Ok(TimestampFilter {
            field,
            time_range: (lower_bound, upper_bound),
            timestamp_field_reader,
        })
    }

    pub fn is_within_range(&self, doc_id: DocId) -> bool {
        let timestamp_value = self.timestamp_field_reader.get(doc_id) as i64;
        self.time_range.contains(&timestamp_value)
    }
}
