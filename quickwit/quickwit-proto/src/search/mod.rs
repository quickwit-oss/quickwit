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

use std::cmp::Ordering;
use std::fmt;

pub use sort_by_value::SortValue;

include!("../codegen/quickwit/quickwit.search.rs");

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

// !!! Disclaimer !!!
//
// Prost imposes the PartialEq derived implementation.
// This is terrible because this means Eq, PartialEq are not really in line with Ord's
// implementation. if in presence of NaN.
impl Eq for SortByValue {}
impl Copy for SortByValue {}
impl From<SortValue> for SortByValue {
    fn from(sort_value: SortValue) -> Self {
        SortByValue {
            sort_value: Some(sort_value),
        }
    }
}

impl Copy for SortValue {}
impl Eq for SortValue {}

impl Ord for SortValue {
    fn cmp(&self, other: &Self) -> Ordering {
        // We make sure to end up with a total order.
        match (*self, *other) {
            // Same types.
            (SortValue::U64(left), SortValue::U64(right)) => left.cmp(&right),
            (SortValue::I64(left), SortValue::I64(right)) => left.cmp(&right),
            (SortValue::F64(left), SortValue::F64(right)) => {
                if left.is_nan() {
                    if right.is_nan() {
                        Ordering::Equal
                    } else {
                        Ordering::Less
                    }
                } else if right.is_nan() {
                    Ordering::Greater
                } else {
                    left.partial_cmp(&right).unwrap_or(Ordering::Less)
                }
            }
            (SortValue::Boolean(left), SortValue::Boolean(right)) => left.cmp(&right),
            // We half the logic by making sure we keep
            // the "stronger" type on the left.
            (SortValue::U64(left), SortValue::I64(right)) => {
                if left > i64::MAX as u64 {
                    return Ordering::Greater;
                }
                (left as i64).cmp(&right)
            }
            (SortValue::F64(left), _) if left.is_nan() => Ordering::Less,
            (SortValue::F64(left), SortValue::U64(right)) => {
                left.partial_cmp(&(right as f64)).unwrap_or(Ordering::Less)
            }
            (SortValue::F64(left), SortValue::I64(right)) => {
                left.partial_cmp(&(right as f64)).unwrap_or(Ordering::Less)
            }
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
