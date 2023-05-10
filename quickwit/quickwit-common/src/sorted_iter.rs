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
use std::collections::{btree_map, btree_set};
use std::iter::Peekable;

/// Marks sorted iterators, typically iterators over [`btree_set::BTreeSet`] and
/// [`btree_map::BTreeMap`].
trait Sorted {}

/// Defines helper methods on sorted iterators.
pub trait SortedIterator: Iterator + Sized {
    /// Compares two sorted iterators and returns the diff.
    fn diff<U>(self, other: U) -> DiffIterator<Self, U>
    where U: SortedIterator<Item = Self::Item> {
        DiffIterator {
            left: self.peekable(),
            right: other.peekable(),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Diff<K> {
    Added(K),
    Unchanged(K),
    Removed(K),
}

pub struct DiffIterator<T: Iterator, U: Iterator> {
    left: Peekable<T>,
    right: Peekable<U>,
}

impl<T, U, K> Iterator for DiffIterator<T, U>
where
    T: Iterator<Item = K>,
    U: Iterator<Item = K>,
    K: Ord,
{
    type Item = Diff<K>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.left.peek(), self.right.peek()) {
            (Some(left), Some(right)) => match left.cmp(right) {
                Ordering::Less => {
                    let left = self
                        .left
                        .next()
                        .expect("The left iterator should not be empty.");
                    Some(Diff::Removed(left))
                }
                Ordering::Equal => {
                    let left = self
                        .left
                        .next()
                        .expect("The left iterator should not be empty.");
                    self.right.next();
                    Some(Diff::Unchanged(left))
                }
                Ordering::Greater => {
                    let right = self
                        .right
                        .next()
                        .expect("The right iterator should not be empty.");
                    Some(Diff::Added(right))
                }
            },
            (Some(_), None) => {
                let left = self
                    .left
                    .next()
                    .expect("The left iterator should not be empty.");
                Some(Diff::Removed(left))
            }
            (None, Some(_)) => {
                let right = self
                    .right
                    .next()
                    .expect("The right iterator should not be empty.");
                Some(Diff::Added(right))
            }
            (None, None) => None,
        }
    }
}

impl<T> SortedIterator for T where T: Iterator + Sorted {}

impl<K, V> Sorted for btree_map::IntoKeys<K, V> {}
impl<K, V> Sorted for btree_map::IntoValues<K, V> {}
impl<'a, K, V> Sorted for btree_map::Keys<'a, K, V> {}
impl<'a, K, V> Sorted for btree_map::Values<'a, K, V> {}
impl<K> Sorted for btree_set::IntoIter<K> {}
impl<'a, K> Sorted for btree_set::Iter<'a, K> {}

/// Same as [`SortedIterator`] but for (key, value) pairs sorted by key.
pub trait SortedByKeyIterator<K, V>: Iterator + Sized {
    /// Compares the keys of two sorted key-value iterators and returns the diff.
    fn diff_by_key<U, W>(self, other: U) -> DiffByKeyIterator<Self, U>
    where U: SortedByKeyIterator<K, W> {
        DiffByKeyIterator {
            left: self.peekable(),
            right: other.peekable(),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum KeyDiff<K, V, W> {
    Added(K, W),
    Unchanged(K, V, W),
    Removed(K, V),
}

pub struct DiffByKeyIterator<T: Iterator, U: Iterator> {
    left: Peekable<T>,
    right: Peekable<U>,
}

impl<T, U, K, V, W> Iterator for DiffByKeyIterator<T, U>
where
    T: Iterator<Item = (K, V)>,
    U: Iterator<Item = (K, W)>,
    K: Ord,
{
    type Item = KeyDiff<K, V, W>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.left.peek(), self.right.peek()) {
            (Some((left_key, _)), Some((right_key, _))) => match left_key.cmp(right_key) {
                Ordering::Less => {
                    let (left_key, left_value) = self
                        .left
                        .next()
                        .expect("The left iterator should not be empty.");
                    Some(KeyDiff::Removed(left_key, left_value))
                }
                Ordering::Equal => {
                    let (left_key, left_value) = self
                        .left
                        .next()
                        .expect("The left iterator should not be empty.");
                    let (_, right_value) = self
                        .right
                        .next()
                        .expect("The right iterator should not be empty.");
                    Some(KeyDiff::Unchanged(left_key, left_value, right_value))
                }
                Ordering::Greater => {
                    let (right_key, right_value) = self
                        .right
                        .next()
                        .expect("The right iterator should not be empty.");
                    Some(KeyDiff::Added(right_key, right_value))
                }
            },
            (Some(_), None) => {
                let (left_key, left_value) = self
                    .left
                    .next()
                    .expect("The left iterator should not be empty.");
                Some(KeyDiff::Removed(left_key, left_value))
            }
            (None, Some(_)) => {
                let (right_key, right_value) = self
                    .right
                    .next()
                    .expect("The right iterator should not be empty.");
                Some(KeyDiff::Added(right_key, right_value))
            }
            (None, None) => None,
        }
    }
}

impl<T, K, V> SortedByKeyIterator<K, V> for T where T: Iterator<Item = (K, V)> + Sorted {}

impl<K, V> Sorted for btree_map::IntoIter<K, V> {}
impl<'a, K, V> Sorted for btree_map::Iter<'a, K, V> {}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;

    #[test]
    fn test_diff() {
        {
            let left: BTreeSet<u64> = vec![].into_iter().collect();
            let right: BTreeSet<u64> = vec![].into_iter().collect();
            let diff: Vec<_> = left.iter().diff(right.iter()).collect();
            assert_eq!(diff, vec![]);
        }
        {
            let left: BTreeSet<_> = vec![1].into_iter().collect();
            let right: BTreeSet<_> = vec![].into_iter().collect();
            let diff: Vec<_> = left.iter().diff(right.iter()).collect();
            assert_eq!(diff, vec![Diff::Removed(&1)]);
        }
        {
            let left: BTreeSet<_> = vec![].into_iter().collect();
            let right: BTreeSet<_> = vec![1].into_iter().collect();
            let diff: Vec<_> = left.iter().diff(right.iter()).collect();
            assert_eq!(diff, vec![Diff::Added(&1)]);
        }
        {
            let left: BTreeSet<_> = vec![1].into_iter().collect();
            let right: BTreeSet<_> = vec![1].into_iter().collect();
            let diff: Vec<_> = left.iter().diff(right.iter()).collect();
            assert_eq!(diff, vec![Diff::Unchanged(&1)]);
        }
        {
            let left: BTreeSet<_> = vec![1, 3, 5, 7].into_iter().collect();
            let right: BTreeSet<_> = vec![2, 4, 5, 6].into_iter().collect();
            let diff: Vec<_> = left.iter().diff(right.iter()).collect();
            assert_eq!(
                diff,
                vec![
                    Diff::Removed(&1),
                    Diff::Added(&2),
                    Diff::Removed(&3),
                    Diff::Added(&4),
                    Diff::Unchanged(&5),
                    Diff::Added(&6),
                    Diff::Removed(&7),
                ]
            );
        }
    }

    #[test]
    fn test_diff_by_key() {
        {
            let left: BTreeMap<u64, u64> = vec![].into_iter().collect();
            let right: BTreeMap<u64, u64> = vec![].into_iter().collect();
            let key_diff: Vec<_> = left.iter().diff_by_key(right.iter()).collect();
            assert_eq!(key_diff, vec![]);
        }
        {
            let left: BTreeMap<_, _> = vec![(1, 1)].into_iter().collect();
            let right: BTreeMap<_, &'static str> = vec![].into_iter().collect();
            let key_diff: Vec<_> = left.iter().diff_by_key(right.iter()).collect();
            assert_eq!(key_diff, vec![KeyDiff::Removed(&1, &1)]);
        }
        {
            let left: BTreeMap<_, usize> = vec![].into_iter().collect();
            let right: BTreeMap<_, _> = vec![(1, "a")].into_iter().collect();
            let key_diff: Vec<_> = left.iter().diff_by_key(right.iter()).collect();
            assert_eq!(key_diff, vec![KeyDiff::Added(&1, &"a")]);
        }
        {
            let left: BTreeMap<_, _> = vec![(1, 11)].into_iter().collect();
            let right: BTreeMap<_, _> = vec![(1, "a")].into_iter().collect();
            let key_diff: Vec<_> = left.iter().diff_by_key(right.iter()).collect();
            assert_eq!(key_diff, vec![KeyDiff::Unchanged(&1, &11, &"a")]);
        }
        {
            let left: BTreeMap<_, _> = vec![(1, 1), (3, 3), (5, 5), (7, 7)].into_iter().collect();
            let right: BTreeMap<_, _> = vec![(2, "b"), (4, "d"), (5, "e"), (6, "f")]
                .into_iter()
                .collect();
            let key_diff: Vec<_> = left.iter().diff_by_key(right.iter()).collect();
            assert_eq!(
                key_diff,
                vec![
                    KeyDiff::Removed(&1, &1),
                    KeyDiff::Added(&2, &"b"),
                    KeyDiff::Removed(&3, &3),
                    KeyDiff::Added(&4, &"d"),
                    KeyDiff::Unchanged(&5, &5, &"e"),
                    KeyDiff::Added(&6, &"f"),
                    KeyDiff::Removed(&7, &7),
                ]
            );
        }
    }
}
