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

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("Failed to reserve requested memory capacity. Current capacity: {0}")]
pub struct ReserveCapacityError(usize);

#[derive(Clone)]
pub struct MemoryCapacity {
    inner: Arc<InnerMemoryCapacity>,
}

impl fmt::Debug for MemoryCapacity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryCapacity")
            .field("capacity", &self.capacity())
            .field("max_capacity", &self.max_capacity())
            .finish()
    }
}

impl MemoryCapacity {
    /// Creates a new [`MemoryCapacity`] object with a capacity of `max_capacity` bytes.
    ///
    /// # Panics
    ///
    /// This constructor panics if `max_capacity` is 0.
    pub fn new(max_capacity: usize) -> Self {
        assert!(
            max_capacity > 0,
            "The memory capacity is required to be > 0."
        );

        Self {
            inner: Arc::new(InnerMemoryCapacity {
                max_capacity,
                capacity: AtomicUsize::new(max_capacity),
            }),
        }
    }

    /// Attempts to reserve `num_bytes` of capacity. Returns an error if there is not enough
    /// capacity available.
    pub fn reserve_capacity(&self, num_bytes: usize) -> Result<(), ReserveCapacityError> {
        loop {
            let current_capacity = self.inner.capacity.load(Ordering::Acquire);

            if current_capacity < num_bytes {
                return Err(ReserveCapacityError(current_capacity));
            }
            let new_capacity = current_capacity - num_bytes;

            if self
                .inner
                .capacity
                .compare_exchange(
                    current_capacity,
                    new_capacity,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    /// Resets the capacity to `new_capacity`.
    pub fn reset_capacity(&self, new_capacity: usize) {
        self.inner.capacity.store(new_capacity, Ordering::Release);
    }

    pub fn max_capacity(&self) -> usize {
        self.inner.max_capacity
    }

    /// Returns the current capacity.
    pub fn capacity(&self) -> usize {
        self.inner
            .capacity
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns the ratio of used capacity to maximum capacity.
    pub fn usage_ratio(&self) -> f64 {
        1.0 - (self.capacity() as f64 / self.max_capacity() as f64)
    }
}

struct InnerMemoryCapacity {
    /// The maximum number of bytes that can be stored in memory.
    max_capacity: usize,
    /// The current number of bytes stored in memory.
    capacity: AtomicUsize,
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::thread;

    use super::*;

    #[tokio::test]
    async fn test_memory_capacity() {
        let memory_capacity = MemoryCapacity::new(10);
        assert_eq!(memory_capacity.max_capacity(), 10);
        assert_eq!(memory_capacity.capacity(), 10);
        assert_eq!(memory_capacity.usage_ratio(), 0.0);

        memory_capacity.reserve_capacity(6).unwrap();
        assert_eq!(memory_capacity.max_capacity(), 10);
        assert_eq!(memory_capacity.capacity(), 4);
        assert_eq!(memory_capacity.usage_ratio(), 0.6);

        memory_capacity.reserve_capacity(3).unwrap();
        assert_eq!(memory_capacity.max_capacity(), 10);
        assert_eq!(memory_capacity.capacity(), 1);
        assert_eq!(memory_capacity.usage_ratio(), 0.9);

        memory_capacity.reserve_capacity(1).unwrap();
        assert_eq!(memory_capacity.max_capacity(), 10);
        assert_eq!(memory_capacity.capacity(), 0);
        assert_eq!(memory_capacity.usage_ratio(), 1.0);

        memory_capacity.reserve_capacity(1).unwrap_err();

        let mut handles = Vec::with_capacity(100);
        let barrier = Arc::new(Barrier::new(100));
        let memory_capacity = MemoryCapacity::new(100);

        for _ in 0..100 {
            let barrier = barrier.clone();
            let memory_capacity = memory_capacity.clone();

            handles.push(thread::spawn(move || {
                barrier.wait();
                memory_capacity.reserve_capacity(1).unwrap();
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(memory_capacity.capacity(), 0)
    }
}
