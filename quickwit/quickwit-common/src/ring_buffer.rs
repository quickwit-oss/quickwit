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

use std::fmt::{Debug, Formatter};

/// Fixed-size buffer that keeps the last N elements pushed into it.
///
/// `head` is the write cursor. It advances by one on each push and wraps
/// back to 0 when it reaches N, overwriting the oldest element.
///
/// ```text
/// RingBuffer<u32, 4> after pushing 1, 2, 3, 4, 5, 6:
///
///   buffer = [5, 6, 3, 4]    head = 2    len = 4
///                 ^
///                 next write goes here
///
///   logical view (oldest → newest): [3, 4, 5, 6]
/// ```
pub struct RingBuffer<T: Copy + Default, const N: usize> {
    buffer: [T; N],
    head: usize,
    len: usize,
}

impl<T: Copy + Default, const N: usize> Default for RingBuffer<T, N> {
    fn default() -> Self {
        Self {
            buffer: [T::default(); N],
            head: 0,
            len: 0,
        }
    }
}

impl<T: Copy + Default + Debug, const N: usize> Debug for RingBuffer<T, N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<T: Copy + Default, const N: usize> RingBuffer<T, N> {
    pub fn push_back(&mut self, value: T) {
        self.buffer[self.head] = value;
        self.head = (self.head + 1) % N;
        if self.len < N {
            self.len += 1;
        }
    }

    pub fn last(&self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        Some(self.buffer[(self.head + N - 1) % N])
    }

    pub fn front(&self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        Some(self.buffer[(self.head + N - self.len) % N])
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> + '_ {
        let start = (self.head + N - self.len) % N;
        (0..self.len).map(move |i| &self.buffer[(start + i) % N])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        let rb = RingBuffer::<u32, 4>::default();
        assert!(rb.is_empty());
        assert_eq!(rb.len(), 0);
        assert_eq!(rb.last(), None);
        assert_eq!(rb.front(), None);
        assert_eq!(rb.iter().count(), 0);
    }

    #[test]
    fn test_single_push() {
        let mut rb = RingBuffer::<u32, 4>::default();
        rb.push_back(10);
        assert_eq!(rb.len(), 1);
        assert!(!rb.is_empty());
        assert_eq!(rb.last(), Some(10));
        assert_eq!(rb.front(), Some(10));
        assert_eq!(rb.iter().copied().collect::<Vec<_>>(), vec![10]);
    }

    #[test]
    fn test_partial_fill() {
        let mut rb = RingBuffer::<u32, 4>::default();
        rb.push_back(1);
        rb.push_back(2);
        rb.push_back(3);
        assert_eq!(rb.len(), 3);
        assert_eq!(rb.last(), Some(3));
        assert_eq!(rb.front(), Some(1));
        assert_eq!(rb.iter().copied().collect::<Vec<_>>(), vec![1, 2, 3]);
    }

    #[test]
    fn test_exactly_full() {
        let mut rb = RingBuffer::<u32, 4>::default();
        for i in 1..=4 {
            rb.push_back(i);
        }
        assert_eq!(rb.len(), 4);
        assert_eq!(rb.last(), Some(4));
        assert_eq!(rb.front(), Some(1));
        assert_eq!(rb.iter().copied().collect::<Vec<_>>(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_wrap_around() {
        let mut rb = RingBuffer::<u32, 4>::default();
        for i in 1..=6 {
            rb.push_back(i);
        }
        assert_eq!(rb.len(), 4);
        assert_eq!(rb.last(), Some(6));
        assert_eq!(rb.front(), Some(3));
        assert_eq!(rb.iter().copied().collect::<Vec<_>>(), vec![3, 4, 5, 6]);
    }

    #[test]
    fn test_many_wraps() {
        let mut rb = RingBuffer::<u32, 3>::default();
        for i in 1..=100 {
            rb.push_back(i);
        }
        assert_eq!(rb.len(), 3);
        assert_eq!(rb.last(), Some(100));
        assert_eq!(rb.front(), Some(98));
        assert_eq!(rb.iter().copied().collect::<Vec<_>>(), vec![98, 99, 100]);
    }

    #[test]
    fn test_debug() {
        let mut rb = RingBuffer::<u32, 3>::default();
        rb.push_back(1);
        rb.push_back(2);
        assert_eq!(format!("{:?}", rb), "[1, 2]");
    }
}
