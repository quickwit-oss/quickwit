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
}
