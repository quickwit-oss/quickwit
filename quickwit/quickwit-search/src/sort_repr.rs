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

use std::fmt::Debug;
use std::ops::Not;

use quickwit_proto::search::SortOrder;
use tantivy::DocId;

use crate::top_k_computer::MinValue;

/// A u64 that can be elided to unit type to save memory.
pub(crate) trait ElidableU64: Ord + Copy + Debug + MinValue {
    fn value(self) -> u64;
    fn from_u64(value: u64) -> Self;
    fn is_elided() -> bool;
}

impl MinValue for u64 {
    fn min_value() -> Self {
        0
    }
}

impl MinValue for () {
    fn min_value() -> Self {}
}

impl ElidableU64 for u64 {
    fn from_u64(value: u64) -> Self {
        value
    }
    fn value(self) -> u64 {
        self
    }
    fn is_elided() -> bool {
        false
    }
}

impl ElidableU64 for () {
    fn from_u64(_value: u64) -> Self {}
    fn value(self) -> u64 {
        0
    }
    fn is_elided() -> bool {
        true
    }
}

/// Encoded representation of the value, the index of its accessor in the list
/// of fast field columns and the sort order.
///
/// The first u8 encodes the index of the accessor and a sentinel value for
/// missing and search after values:
/// - 0 is a sentinel for skip all
/// - 1 is a sentinel for missing (always last in the sort order)
/// - other odd values encode the index of the accessor in the list of fast field columns (3 for
///   index 0, 5 for index 1, etc.)
/// - even values are sentinels for search after values that keep/skip all documents for a given
///   column (2 to skip all columns but keep missing, 4 only keeps column 0, 6 keeps column 0 and 1,
///   etc.)
///
/// The following u64 encodes the value itself or its bitwise negation to
/// reverse the sort order when building an ascending sort (keeping in mind that
/// this is fed to a top-k calculator).
#[derive(Clone, Copy)]
pub(crate) struct InternalValueRepr<V: ElidableU64>(u8, V);

/// Inverts the sort order by reversing the bits.
///
/// Using the bitwise negation is a cheap way to reverse the order while
/// maintaining the type (and memory footprint). It is also reversible
/// (`not(not(value)) == value`) which makes it simply decodable.
///
/// This wrapper is just an alias to make the code more readable. Using `!value`
/// or `value.not()` inline yields the same result.
#[inline]
fn reverse<T: Not<Output = T>>(value: T) -> T {
    value.not()
}

impl<V: ElidableU64> InternalValueRepr<V> {
    #[inline]
    pub fn new(value: u64, accessor_idx: u8, order: SortOrder) -> Self {
        // For Asc, smaller values should win: invert so smaller maps to larger repr
        match order {
            SortOrder::Asc => Self(reverse(accessor_idx * 2 + 3), V::from_u64(reverse(value))),
            SortOrder::Desc => Self(accessor_idx * 2 + 3, V::from_u64(value)),
        }
    }
    /// A sentinel value that can be instantiated as search after boundary to indicate
    /// that all documents should be kept.
    pub fn new_keep_column(accessor_idx: u8, order: SortOrder) -> Self {
        match order {
            SortOrder::Asc => Self(reverse(accessor_idx * 2 + 2), V::from_u64(0)),
            SortOrder::Desc => Self(accessor_idx * 2 + 4, V::from_u64(0)),
        }
    }
    #[inline]
    pub fn new_missing() -> Self {
        // Missing always last in topk, so use the smallest possible value
        // (besides the skip_all value)
        Self(1, V::from_u64(0))
    }
    /// A sentinel value that can be instantiated as search after boundary to indicate
    /// that all documents should be skipped for the given column.
    pub fn new_skip_column(accessor_idx: u8, order: SortOrder) -> Self {
        match order {
            SortOrder::Asc => Self(reverse(accessor_idx * 2 + 4), V::from_u64(0)),
            SortOrder::Desc => Self(accessor_idx * 2 + 2, V::from_u64(0)),
        }
    }
    /// A sentinel value that can be instantiated as search after boundary to indicate
    /// that all documents should be skipped.
    pub fn new_skip_all_but_missing() -> Self {
        Self(2, V::from_u64(0))
    }
    #[inline]
    pub fn decode(self, order: SortOrder) -> Option<(u8, u64)> {
        if self.0 == 1 {
            return None;
        }
        debug_assert_eq!(
            match order {
                SortOrder::Asc => reverse(self.0),
                SortOrder::Desc => self.0,
            } % 2,
            1,
            "sentinel indexes are not meant to be decoded"
        );
        match order {
            SortOrder::Asc => Some(((reverse(self.0) - 3) / 2, reverse(V::value(self.1)))),
            SortOrder::Desc => Some(((self.0 - 3) / 2, V::value(self.1))),
        }
    }
}

/// Ordered representation of the sort values. It is the concatenation of:
/// - the first two (u8, u64) pairs contain the internal representation of the sort values
/// - the second sort value's internal representation
/// - the doc id, preceeded by a sentinel indicating how it should be used for tie-breaking
///
/// ElidableU64 is used instead of u64 for sort values to reduce the size of the
/// representation when they are not used. The associated sentinels could also
/// be elided, but in practice they don't have an impact on the tuple's size
/// because the doc id and its sentinel (u8, u32) gets padded anyway.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub(crate) struct InternalSortValueRepr<V1: ElidableU64, V2: ElidableU64>(u8, V1, u8, V2, u8, u32);

impl<V1: ElidableU64, V2: ElidableU64> InternalSortValueRepr<V1, V2> {
    #[inline]
    pub fn new(
        sort_1: InternalValueRepr<V1>,
        sort_2: InternalValueRepr<V2>,
        doc_id: DocId,
        doc_id_sort: SortOrder,
    ) -> Self {
        // For Asc, smaller values should win: invert so smaller maps to larger repr
        match doc_id_sort {
            SortOrder::Asc => Self(sort_1.0, sort_1.1, sort_2.0, sort_2.1, 1, reverse(doc_id)),
            SortOrder::Desc => Self(sort_1.0, sort_1.1, sort_2.0, sort_2.1, 1, doc_id),
        }
    }
    pub fn new_keep_doc_ids(sort_1: InternalValueRepr<V1>, sort_2: InternalValueRepr<V2>) -> Self {
        Self(sort_1.0, sort_1.1, sort_2.0, sort_2.1, 2, 0)
    }
    pub fn new_skip_doc_ids(sort_1: InternalValueRepr<V1>, sort_2: InternalValueRepr<V2>) -> Self {
        Self(sort_1.0, sort_1.1, sort_2.0, sort_2.1, 0, 0)
    }
    #[inline]
    pub fn sort_1(self) -> InternalValueRepr<V1> {
        InternalValueRepr(self.0, self.1)
    }
    #[inline]
    pub fn sort_2(self) -> InternalValueRepr<V2> {
        InternalValueRepr(self.2, self.3)
    }
    #[inline]
    pub fn doc_id(self, order: SortOrder) -> DocId {
        debug_assert_eq!(self.4, 1, "doc id sentinel is not meant to be decoded");
        match order {
            SortOrder::Asc => reverse(self.5),
            SortOrder::Desc => self.5,
        }
    }
    pub fn is_skip_all(&self) -> bool {
        *self <= Self(1, V1::min_value(), 1, V2::min_value(), 1, 0)
    }
}

impl<V1: ElidableU64, V2: ElidableU64> MinValue for InternalSortValueRepr<V1, V2> {
    fn min_value() -> Self {
        Self(0, V1::min_value(), 0, V2::min_value(), 1, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_internal_sort_value_repr_ordering_values() {
        // Primary sort (Desc v1=10) dominates over secondary (Desc v2=100) and doc_id.
        let lhs = InternalSortValueRepr::new(
            InternalValueRepr::<u64>::new(10, 0, SortOrder::Desc),
            InternalValueRepr::<u64>::new(0, 0, SortOrder::Desc),
            0,
            SortOrder::Desc,
        );
        let rhs = InternalSortValueRepr::new(
            InternalValueRepr::<u64>::new(5, 0, SortOrder::Desc),
            InternalValueRepr::<u64>::new(100, 0, SortOrder::Desc),
            999,
            SortOrder::Desc,
        );
        assert!(lhs > rhs, "primary sort must dominate, desc");

        // Same values but Asc, the order is reversed
        let lhs = InternalSortValueRepr::new(
            InternalValueRepr::<u64>::new(10, 0, SortOrder::Asc),
            InternalValueRepr::<u64>::new(0, 0, SortOrder::Desc),
            0,
            SortOrder::Desc,
        );
        let rhs = InternalSortValueRepr::new(
            InternalValueRepr::<u64>::new(5, 0, SortOrder::Asc),
            InternalValueRepr::<u64>::new(100, 0, SortOrder::Desc),
            999,
            SortOrder::Desc,
        );
        assert!(lhs < rhs, "primary sort must dominate, asc");

        // Secondary sort (Desc v2) breaks a tie on the primary field.
        let lhs = InternalSortValueRepr::new(
            InternalValueRepr::<u64>::new(10, 0, SortOrder::Desc),
            InternalValueRepr::<u64>::new(10, 0, SortOrder::Desc),
            0,
            SortOrder::Desc,
        );
        let rhs = InternalSortValueRepr::new(
            InternalValueRepr::<u64>::new(10, 0, SortOrder::Desc),
            InternalValueRepr::<u64>::new(5, 0, SortOrder::Desc),
            0,
            SortOrder::Desc,
        );
        assert!(lhs > rhs, "secondary sort must break primary tie, desc");

        // Same values but Asc, the order is reversed.
        let lhs = InternalSortValueRepr::new(
            InternalValueRepr::<u64>::new(10, 0, SortOrder::Desc),
            InternalValueRepr::<u64>::new(10, 0, SortOrder::Asc),
            0,
            SortOrder::Desc,
        );
        let rhs = InternalSortValueRepr::new(
            InternalValueRepr::<u64>::new(10, 0, SortOrder::Desc),
            InternalValueRepr::<u64>::new(5, 0, SortOrder::Asc),
            0,
            SortOrder::Desc,
        );
        assert!(lhs < rhs, "secondary sort must break primary tie, asc");

        // Doc-id Desc tiebreaker: higher doc_id wins.
        let lhs = InternalSortValueRepr::new(
            InternalValueRepr::<u64>::new(10, 0, SortOrder::Desc),
            InternalValueRepr::<u64>::new_missing(),
            10,
            SortOrder::Desc,
        );
        let rhs = InternalSortValueRepr::new(
            InternalValueRepr::<u64>::new(10, 0, SortOrder::Desc),
            InternalValueRepr::<u64>::new_missing(),
            5,
            SortOrder::Desc,
        );
        assert!(lhs > rhs, "Desc: higher doc_id must win tiebreaker");

        // Doc-id Asc tiebreaker: lower doc_id wins.
        let lhs = InternalSortValueRepr::new(
            InternalValueRepr::<u64>::new(10, 0, SortOrder::Desc),
            InternalValueRepr::<u64>::new_missing(),
            5,
            SortOrder::Asc,
        );
        let rhs = InternalSortValueRepr::new(
            InternalValueRepr::<u64>::new(10, 0, SortOrder::Desc),
            InternalValueRepr::<u64>::new_missing(),
            10,
            SortOrder::Asc,
        );
        assert!(lhs > rhs, "Asc: lower doc_id must win tiebreaker");

        // Missing values are always smaller
        let lhs = InternalSortValueRepr::new(
            InternalValueRepr::<u64>::new_missing(),
            InternalValueRepr::<u64>::new(10, 0, SortOrder::Desc),
            10,
            SortOrder::Desc,
        );
        let rhs = InternalSortValueRepr::new(
            InternalValueRepr::<u64>::new(5, 0, SortOrder::Desc),
            InternalValueRepr::<u64>::new(0, 0, SortOrder::Desc),
            0,
            SortOrder::Desc,
        );
        assert!(lhs < rhs, "missing values are always smaller, desc");

        // Same but Asc, missing is still smaller.
        let lhs = InternalSortValueRepr::new(
            InternalValueRepr::<u64>::new_missing(),
            InternalValueRepr::<u64>::new(10, 0, SortOrder::Desc),
            10,
            SortOrder::Desc,
        );
        let rhs = InternalSortValueRepr::new(
            InternalValueRepr::<u64>::new(5, 0, SortOrder::Asc),
            InternalValueRepr::<u64>::new(0, 0, SortOrder::Desc),
            0,
            SortOrder::Desc,
        );
        assert!(lhs < rhs, "missing values are always smaller, asc");
    }

    #[test]
    fn test_internal_sort_value_repr_ordering_sentinels() {
        // Doc-id sentinel ordering: skip_doc_ids < normal_doc_id < keep_doc_ids.
        let s1 = InternalValueRepr::<u64>::new(10, 0, SortOrder::Desc);
        let s2 = InternalValueRepr::<u64>::new_missing();
        let skip_docs = InternalSortValueRepr::new_skip_doc_ids(s1, s2);
        let keep_docs = InternalSortValueRepr::new_keep_doc_ids(s1, s2);
        let normal_doc_desc = InternalSortValueRepr::new(s1, s2, 0, SortOrder::Desc);
        let normal_doc_asc = InternalSortValueRepr::new(s1, s2, 0, SortOrder::Asc);
        assert!(
            skip_docs < normal_doc_desc,
            "skip_doc_ids must be below normal"
        );
        assert!(
            normal_doc_desc < keep_docs,
            "normal must be below keep_doc_ids"
        );
        assert!(
            skip_docs < normal_doc_asc,
            "skip_doc_ids must be below normal"
        );
        assert!(
            normal_doc_asc < keep_docs,
            "normal must be below keep_doc_ids"
        );
    }

    #[test]
    fn test_internal_sort_value_repr_ordering_types() {
        // Primary accessor ordering dominates all the rest
        let lhs = InternalSortValueRepr::new(
            InternalValueRepr::<u64>::new(5, 1, SortOrder::Desc),
            InternalValueRepr::<u64>::new(0, 0, SortOrder::Desc),
            0,
            SortOrder::Desc,
        );
        let rhs = InternalSortValueRepr::new(
            InternalValueRepr::<u64>::new(15, 0, SortOrder::Desc),
            InternalValueRepr::<u64>::new(100, 0, SortOrder::Desc),
            999,
            SortOrder::Desc,
        );
        assert!(lhs > rhs, "primary type sort must dominate, desc");

        // Same values but Asc, the order is reversed
        let lhs = InternalSortValueRepr::new(
            InternalValueRepr::<u64>::new(5, 1, SortOrder::Asc),
            InternalValueRepr::<u64>::new(0, 0, SortOrder::Desc),
            0,
            SortOrder::Desc,
        );
        let rhs = InternalSortValueRepr::new(
            InternalValueRepr::<u64>::new(15, 0, SortOrder::Asc),
            InternalValueRepr::<u64>::new(100, 0, SortOrder::Desc),
            999,
            SortOrder::Desc,
        );
        assert!(lhs < rhs, "primary type sort must dominate, asc");
    }

    #[test]
    fn test_memory_footprint() {
        // Make sure that the memory representation is efficiently packed. For
        // instance refactoring to:
        // ```
        //   struct InternalSortValueRepr(InternalValueRepr<u64>,InternalValueRepr<u64>,u64)
        // ```
        // would cause InternalSortValueRepr<u64, u64> to jump to 40 bytes.

        assert_eq!(std::mem::size_of::<InternalSortValueRepr<u64, u64>>(), 24);
        assert_eq!(std::mem::size_of::<InternalSortValueRepr<u64, ()>>(), 16);
        assert_eq!(std::mem::size_of::<InternalSortValueRepr<(), ()>>(), 8);
    }
}
