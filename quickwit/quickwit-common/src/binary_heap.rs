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

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::iter::FusedIterator;

// TODO: Remove this once `BinaryHeap::into_iter_sorted` is stabilized.

#[must_use = "iterators are lazy and do nothing unless consumed"]
#[derive(Clone, Debug)]
pub struct IntoIterSorted<T> {
    inner: BinaryHeap<T>,
}

impl<T> IntoIterSorted<T> {
    pub fn new(instance: BinaryHeap<T>) -> Self {
        Self { inner: instance }
    }
}

impl<T: Ord> Iterator for IntoIterSorted<T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        self.inner.pop()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.inner.len();
        (exact, Some(exact))
    }
}

impl<T: Ord> ExactSizeIterator for IntoIterSorted<T> {}

impl<T: Ord> FusedIterator for IntoIterSorted<T> {}

/// Consumes an iterator entirely and return the top-K best element according to a scoring key.
/// Behavior under the presence of ties is unspecified.
pub fn top_k<T, SortKeyFn, O>(
    mut items: impl Iterator<Item = T>,
    k: usize,
    sort_key_fn: SortKeyFn,
) -> Vec<T>
where
    SortKeyFn: Fn(&T) -> O,
    O: Ord,
{
    if k == 0 {
        return Vec::new();
    }
    let mut heap: BinaryHeap<Reverse<OrderItemPair<O, T>>> = BinaryHeap::with_capacity(k);
    for _ in 0..k {
        if let Some(item) = items.next() {
            let order: O = sort_key_fn(&item);
            heap.push(Reverse(OrderItemPair { order, item }));
        } else {
            break;
        }
    }
    if heap.len() == k {
        for item in items {
            let mut head = heap.peek_mut().unwrap();
            let order = sort_key_fn(&item);
            if head.0.order < order {
                *head = Reverse(OrderItemPair { order, item });
            }
        }
    }
    let resulting_top_k: Vec<T> = heap
        .into_sorted_vec()
        .into_iter()
        .map(|order_item| order_item.0.item)
        .collect();
    resulting_top_k
}

#[derive(Clone)]
struct OrderItemPair<O: Ord, T> {
    order: O,
    item: T,
}

impl<O: Ord, T> Ord for OrderItemPair<O, T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.order.cmp(&other.order)
    }
}

impl<O: Ord, T> PartialOrd for OrderItemPair<O, T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.order.cmp(&other.order))
    }
}

impl<O: Ord, T> PartialEq for OrderItemPair<O, T> {
    fn eq(&self, other: &Self) -> bool {
        self.order.cmp(&other.order) == Ordering::Equal
    }
}

impl<O: Ord, T> Eq for OrderItemPair<O, T> {}

pub trait SortKeyMapper<Value> {
    type Key;
    fn get_sort_key(&self, value: &Value) -> Self::Key;
}

/// Progressively compute top-k.
#[derive(Clone)]
pub struct TopK<T, O: Ord, S> {
    heap: BinaryHeap<Reverse<OrderItemPair<O, T>>>,
    pub sort_key_mapper: S,
    k: usize,
}

impl<T, O, S> TopK<T, O, S>
where
    O: Ord,
    S: SortKeyMapper<T, Key = O>,
{
    /// Create a new top-k computer.
    pub fn new(k: usize, sort_key_mapper: S) -> Self {
        TopK {
            heap: BinaryHeap::with_capacity(k),
            sort_key_mapper,
            k,
        }
    }

    /// Whether there are k element ready already.
    pub fn at_capacity(&self) -> bool {
        self.heap.len() >= self.k
    }

    pub fn max_len(&self) -> usize {
        self.k
    }

    /// Try to add new entries, if they are better than the current worst.
    pub fn add_entries(&mut self, mut items: impl Iterator<Item = T>) {
        if self.k == 0 {
            return;
        }
        while !self.at_capacity() {
            if let Some(item) = items.next() {
                let order: O = self.sort_key_mapper.get_sort_key(&item);
                self.heap.push(Reverse(OrderItemPair { order, item }));
            } else {
                return;
            }
        }

        for item in items {
            let mut head = self.heap.peek_mut().unwrap();
            let order = self.sort_key_mapper.get_sort_key(&item);
            if head.0.order < order {
                *head = Reverse(OrderItemPair { order, item });
            }
        }
    }

    pub fn add_entry(&mut self, item: T) {
        self.add_entries(std::iter::once(item))
    }

    /// Get a reference to the worst entry.
    pub fn peek_worst(&self) -> Option<&T> {
        self.heap.peek().map(|entry| &entry.0.item)
    }

    /// Get a Vec of sorted entries.
    pub fn finalize(self) -> Vec<T> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|order_item| order_item.0.item)
            .collect()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_top_k() {
        let top_k = super::top_k(vec![1u32, 2, 3].into_iter(), 2, |n| *n);
        assert_eq!(&top_k, &[3, 2]);
        let top_k = super::top_k(vec![1u32, 2, 3].into_iter(), 2, |n| Reverse(*n));
        assert_eq!(&top_k, &[1, 2]);
        let top_k = super::top_k(vec![1u32, 2, 2].into_iter(), 4, |n| *n);
        assert_eq!(&top_k, &[2u32, 2, 1]);
        let top_k = super::top_k(vec![1u32, 2, 2].into_iter(), 4, |n| *n);
        assert_eq!(&top_k, &[2u32, 2, 1]);
        let top_k: Vec<u32> = super::top_k(vec![].into_iter(), 4, |n| *n);
        assert!(top_k.is_empty());
    }

    #[test]
    fn test_incremental_top_k() {
        struct Mapper(bool);
        impl SortKeyMapper<u32> for Mapper {
            type Key = u32;
            fn get_sort_key(&self, value: &u32) -> u32 {
                if self.0 {
                    u32::MAX - value
                } else {
                    *value
                }
            }
        }
        let mut top_k = TopK::new(2, Mapper(false));
        top_k.add_entries([1u32, 2, 3].into_iter());
        assert!(top_k.at_capacity());
        assert_eq!(top_k.peek_worst(), Some(&2));
        assert_eq!(&top_k.finalize(), &[3, 2]);

        let mut top_k = TopK::new(2, Mapper(false));
        top_k.add_entries([1u32].into_iter());
        assert!(!top_k.at_capacity());
        assert_eq!(top_k.peek_worst(), Some(&1));
        top_k.add_entries([3].into_iter());
        assert!(top_k.at_capacity());
        assert_eq!(top_k.peek_worst(), Some(&1));
        top_k.add_entries([2].into_iter());
        assert!(top_k.at_capacity());
        assert_eq!(top_k.peek_worst(), Some(&2));
        assert_eq!(&top_k.finalize(), &[3, 2]);

        let mut top_k = TopK::new(2, Mapper(true));
        top_k.add_entries([1u32, 2, 3].into_iter());
        assert!(top_k.at_capacity());
        assert_eq!(top_k.peek_worst(), Some(&2));
        assert_eq!(&top_k.finalize(), &[1, 2]);

        let mut top_k = TopK::new(2, Mapper(true));
        top_k.add_entries([1u32].into_iter());
        assert!(!top_k.at_capacity());
        assert_eq!(top_k.peek_worst(), Some(&1));
        top_k.add_entries([3].into_iter());
        assert!(top_k.at_capacity());
        assert_eq!(top_k.peek_worst(), Some(&3));
        top_k.add_entries([2].into_iter());
        assert!(top_k.at_capacity());
        assert_eq!(top_k.peek_worst(), Some(&2));
        assert_eq!(&top_k.finalize(), &[1, 2]);

        let mut top_k = TopK::new(4, Mapper(false));
        top_k.add_entries([2u32, 1, 2].into_iter());
        assert!(!top_k.at_capacity());
        assert_eq!(top_k.peek_worst(), Some(&1));
        assert_eq!(&top_k.finalize(), &[2, 2, 1]);

        let mut top_k = TopK::new(4, Mapper(false));
        top_k.add_entries([2u32].into_iter());
        assert!(!top_k.at_capacity());
        assert_eq!(top_k.peek_worst(), Some(&2));
        top_k.add_entries([1].into_iter());
        assert!(!top_k.at_capacity());
        assert_eq!(top_k.peek_worst(), Some(&1));
        top_k.add_entries([2].into_iter());
        assert!(!top_k.at_capacity());
        assert_eq!(top_k.peek_worst(), Some(&1));
        assert_eq!(&top_k.finalize(), &[2, 2, 1]);

        let mut top_k = TopK::<u32, u32, _>::new(4, Mapper(false));
        top_k.add_entries([].into_iter());
        assert!(top_k.finalize().is_empty());

        let mut top_k = TopK::new(0, Mapper(false));
        top_k.add_entries([1u32, 2, 3].into_iter());
        assert!(top_k.at_capacity());
        assert_eq!(top_k.peek_worst(), None);
        assert!(top_k.finalize().is_empty());
    }
}
