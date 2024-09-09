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

use std::collections::LinkedList;
use std::sync::Weak;

pub trait Container<T> {
    fn contains(&self, other: &T) -> bool;
}

/// Data structure for checking if a given value is still present in a set of
/// weakly referenced containers (e.g arrays).
///
/// Each time a lookup is performed, all inactive references are removed.
/// Lookups are O(number of referenced containers).
pub struct RefTracker<C, T> {
    refs: LinkedList<Weak<C>>,
    _marker: std::marker::PhantomData<T>,
}

impl<C, T> Default for RefTracker<C, T> {
    fn default() -> Self {
        RefTracker {
            refs: LinkedList::new(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<C: Container<T>, T> RefTracker<C, T> {
    pub fn add(&mut self, weak: Weak<C>) {
        self.refs.push_back(weak);
    }

    pub fn contains(&mut self, value: &T) -> bool {
        // TODO replace impl with `LinkedList::retain` when stabilized
        let mut new_refs = LinkedList::new();
        let mut value_found = false;
        while let Some(weak) = self.refs.pop_front() {
            if let Some(ingester_ids) = weak.upgrade() {
                if !value_found && ingester_ids.contains(value) {
                    value_found = true;
                }
                new_refs.push_back(weak);
            }
        }
        self.refs = new_refs;
        value_found
    }
}

impl<T: PartialEq> Container<T> for Vec<T> {
    fn contains(&self, other: &T) -> bool {
        self[..].contains(other)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_add_and_contains() {
        let mut ref_tracker = RefTracker::default();
        let container = Arc::new(vec![1, 2, 3]);
        let weak_container = Arc::downgrade(&container);

        ref_tracker.add(weak_container);
        assert!(ref_tracker.contains(&2));
        assert!(!ref_tracker.contains(&4));
    }

    #[test]
    fn test_contains_with_dropped_reference() {
        let mut ref_tracker = RefTracker::default();
        let container = Arc::new(vec![1, 2, 3]);
        let weak_container = Arc::downgrade(&container);

        ref_tracker.add(weak_container);
        drop(container);

        assert!(!ref_tracker.contains(&2));
        assert!(ref_tracker.refs.is_empty());
    }

    #[test]
    fn test_multiple_references() {
        let mut ref_tracker = RefTracker::default();
        let container1 = Arc::new(vec![1, 2, 3]);
        let container2 = Arc::new(vec![4, 5, 6]);
        let weak_container1 = Arc::downgrade(&container1);
        let weak_container2 = Arc::downgrade(&container2);

        ref_tracker.add(weak_container1);
        ref_tracker.add(weak_container2);

        assert!(ref_tracker.contains(&2));
        assert!(ref_tracker.contains(&5));
        assert!(!ref_tracker.contains(&7));
    }

    #[test]
    fn test_empty_tracker() {
        let mut ref_tracker: RefTracker<Vec<_>, i32> = RefTracker::default();
        assert!(!ref_tracker.contains(&1));
    }
}
