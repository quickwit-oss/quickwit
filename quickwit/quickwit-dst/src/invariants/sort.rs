// Copyright (C) 2024 Quickwit, Inc.
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

//! Shared null-aware comparison for SS-2 (null ordering invariant).
//!
//! This is the single source of truth for how nulls sort relative to non-null
//! values. Used by both the stateright sort_schema model and production code.

use std::cmp::Ordering;

/// Compare two optional values with null ordering per SS-2.
///
/// - Ascending:  nulls sort AFTER non-null (nulls last).
/// - Descending: nulls sort BEFORE non-null (nulls first).
///
/// For two non-null values, the natural ordering is used (reversed for
/// descending). Two nulls compare as equal.
pub fn compare_with_null_ordering<T: Ord>(
    a: Option<&T>,
    b: Option<&T>,
    ascending: bool,
) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => {
            if ascending {
                Ordering::Greater // null after non-null
            } else {
                Ordering::Less // null before non-null
            }
        }
        (Some(_), None) => {
            if ascending {
                Ordering::Less // non-null before null
            } else {
                Ordering::Greater // non-null after null
            }
        }
        (Some(va), Some(vb)) => {
            if ascending {
                va.cmp(vb)
            } else {
                vb.cmp(va)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascending_null_ordering() {
        // null > non-null in ascending
        assert_eq!(
            compare_with_null_ordering(None::<&i32>, Some(&1), true),
            Ordering::Greater
        );
        assert_eq!(
            compare_with_null_ordering(Some(&1), None::<&i32>, true),
            Ordering::Less
        );
        // null == null
        assert_eq!(
            compare_with_null_ordering(None::<&i32>, None::<&i32>, true),
            Ordering::Equal
        );
        // non-null comparison
        assert_eq!(
            compare_with_null_ordering(Some(&1), Some(&2), true),
            Ordering::Less
        );
    }

    #[test]
    fn descending_null_ordering() {
        // null < non-null in descending
        assert_eq!(
            compare_with_null_ordering(None::<&i32>, Some(&1), false),
            Ordering::Less
        );
        assert_eq!(
            compare_with_null_ordering(Some(&1), None::<&i32>, false),
            Ordering::Greater
        );
        // non-null comparison reversed
        assert_eq!(
            compare_with_null_ordering(Some(&1), Some(&2), false),
            Ordering::Greater
        );
    }
}
