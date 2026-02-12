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

/// A fixed-capacity circular buffer that overwrites the oldest element when full.
///
/// Elements are stored in a flat array of size `N` and rotated on each push.
/// The newest element is always at position `N - 1` (the last slot), and the
/// oldest is at position `N - len`.
pub struct RingBuffer<T: Copy + Default, const N: usize> {
    buffer: [T; N],
    len: usize,
}

impl<T: Copy + Default, const N: usize> Default for RingBuffer<T, N> {
    fn default() -> Self {
        Self {
            buffer: [T::default(); N],
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
    pub fn push(&mut self, value: T) {
        self.len = (self.len + 1).min(N);
        self.buffer.rotate_left(1);
        if let Some(last) = self.buffer.last_mut() {
            *last = value;
        }
    }

    pub fn last(&self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        self.buffer.last().copied()
    }

    pub fn oldest(&self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        Some(self.buffer[N - self.len])
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Iterates from oldest to newest over the recorded elements.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.buffer[N - self.len..].iter()
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
        assert_eq!(rb.oldest(), None);
        assert_eq!(rb.iter().count(), 0);
    }

    #[test]
    fn test_single_push() {
        let mut rb = RingBuffer::<u32, 4>::default();
        rb.push(10);
        assert_eq!(rb.len(), 1);
        assert!(!rb.is_empty());
        assert_eq!(rb.last(), Some(10));
        assert_eq!(rb.oldest(), Some(10));
        assert_eq!(rb.iter().copied().collect::<Vec<_>>(), vec![10]);
    }

    #[test]
    fn test_partial_fill() {
        let mut rb = RingBuffer::<u32, 4>::default();
        rb.push(1);
        rb.push(2);
        rb.push(3);
        assert_eq!(rb.len(), 3);
        assert_eq!(rb.last(), Some(3));
        assert_eq!(rb.oldest(), Some(1));
        assert_eq!(rb.iter().copied().collect::<Vec<_>>(), vec![1, 2, 3]);
    }

    #[test]
    fn test_exactly_full() {
        let mut rb = RingBuffer::<u32, 4>::default();
        for i in 1..=4 {
            rb.push(i);
        }
        assert_eq!(rb.len(), 4);
        assert_eq!(rb.last(), Some(4));
        assert_eq!(rb.oldest(), Some(1));
        assert_eq!(rb.iter().copied().collect::<Vec<_>>(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_wrap_around() {
        let mut rb = RingBuffer::<u32, 4>::default();
        for i in 1..=6 {
            rb.push(i);
        }
        // Buffer should contain [3, 4, 5, 6], oldest overwritten.
        assert_eq!(rb.len(), 4);
        assert_eq!(rb.last(), Some(6));
        assert_eq!(rb.oldest(), Some(3));
        assert_eq!(rb.iter().copied().collect::<Vec<_>>(), vec![3, 4, 5, 6]);
    }

    #[test]
    fn test_many_wraps() {
        let mut rb = RingBuffer::<u32, 3>::default();
        for i in 1..=100 {
            rb.push(i);
        }
        assert_eq!(rb.len(), 3);
        assert_eq!(rb.last(), Some(100));
        assert_eq!(rb.oldest(), Some(98));
        assert_eq!(rb.iter().copied().collect::<Vec<_>>(), vec![98, 99, 100]);
    }

    #[test]
    fn test_debug() {
        let mut rb = RingBuffer::<u32, 3>::default();
        rb.push(1);
        rb.push(2);
        assert_eq!(format!("{:?}", rb), "[1, 2]");
    }
}
