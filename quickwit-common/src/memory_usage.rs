// Copyright (C) 2021 Quickwit, Inc.
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

use std::sync::{Arc, Mutex};

use tokio::sync::Notify;
use tracing::debug;

/// Helps track a memory usage.
#[derive(Clone, Default)]
pub struct MemoryUsage {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    // We cannot use an async mutex here this is fine.
    // Lock holders release only do some very simple computation and release it very fast.
    total_num_bytes: Mutex<u64>,
    notify: Notify,
}

impl MemoryUsage {
    fn release_num_bytes(&self, num_bytes: u64) {
        let mut total_num_bytes_lock = self.inner.total_num_bytes.lock().unwrap();
        *total_num_bytes_lock -= num_bytes;
        self.inner.notify.notify_waiters();
    }

    /// Registers some memory usage and returns a memory guard.
    /// Upon drop, the memory usage will be substracted automatically.
    pub fn use_memory(&self, num_bytes: u64) -> MemoryUsageGuard {
        let mut total_num_bytes_lock = self.inner.total_num_bytes.lock().unwrap();
        *total_num_bytes_lock += num_bytes;
        MemoryUsageGuard {
            memory_usage: self.clone(),
            num_bytes,
        }
    }

    /// Returns the total amount of bytes currently registered.
    pub fn total_num_bytes(&self) -> u64 {
        *self.inner.total_num_bytes.lock().unwrap()
    }

    /// Attempts to register a `num_bytes`, but only if the
    /// current overall amount of memory does not exceed `limit`.
    ///
    /// If it is not possible right way, this function will wait for some memory
    /// to be released.
    ///
    /// This is certainly a weird contract. :)
    /// Check out its callsite for an explanation.
    pub async fn use_memory_with_limit(&self, num_bytes: u64, limit: u64) -> MemoryUsageGuard {
        loop {
            let notify_future = {
                // The purpose of the mutex is to make sure
                // notify_waiters() is not called right in between after
                // we decide to wait and the moment, we effectively create the `Notified`
                // object.
                let mut total_num_bytes_guard = self.inner.total_num_bytes.lock().unwrap();
                let total_num_bytes = *total_num_bytes_guard;
                if total_num_bytes <= limit {
                    *total_num_bytes_guard += num_bytes;
                    return MemoryUsageGuard {
                        memory_usage: self.clone(),
                        num_bytes,
                    };
                }
                self.inner.notify.notified()
            };
            debug!("waiting due to memory limit");
            notify_future.await;
        }
    }
}

/// Upon drop, this guard will substract the memory usage it was tracking.
pub struct MemoryUsageGuard {
    memory_usage: MemoryUsage,
    num_bytes: u64,
}

impl MemoryUsageGuard {
    /// Returns the number of bytes held by this memory usage guard.
    pub fn num_bytes(&self) -> u64 {
        self.num_bytes
    }
}

impl Drop for MemoryUsageGuard {
    fn drop(&mut self) {
        self.memory_usage.release_num_bytes(self.num_bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryUsage;

    #[test]
    fn test_memory_usage_default_0() {
        let memory_usage = MemoryUsage::default();
        assert_eq!(memory_usage.total_num_bytes(), 0);
    }

    #[test]
    fn test_memory_usage_default_simple() {
        let memory_usage = MemoryUsage::default();
        let guard = memory_usage.use_memory(76);
        assert_eq!(memory_usage.total_num_bytes(), 76);
        drop(guard);
        assert_eq!(memory_usage.total_num_bytes(), 0);
    }

    #[test]
    fn test_clone_shares() {
        let memory_usage = MemoryUsage::default();
        let guard = memory_usage.use_memory(1);
        let memory_usage_clone = memory_usage.clone();
        assert_eq!(memory_usage.total_num_bytes(), 1);
        assert_eq!(memory_usage_clone.total_num_bytes(), 1);
        let guard2 = memory_usage_clone.use_memory(2);
        assert_eq!(memory_usage.total_num_bytes(), 3);
        assert_eq!(memory_usage_clone.total_num_bytes(), 3);
        drop(guard);
        assert_eq!(memory_usage.total_num_bytes(), 2);
        assert_eq!(memory_usage_clone.total_num_bytes(), 2);
        drop(guard2);
        assert_eq!(memory_usage.total_num_bytes(), 0);
        assert_eq!(memory_usage_clone.total_num_bytes(), 0);
    }

    #[test]
    fn test_memory_usage_drop_in_any_order() {
        let memory_usage = MemoryUsage::default();
        let guard_1 = memory_usage.use_memory(1);
        assert_eq!(memory_usage.total_num_bytes(), 1);
        let guard_2 = memory_usage.use_memory(2);
        assert_eq!(memory_usage.total_num_bytes(), 1 + 2);
        let guard_4 = memory_usage.use_memory(4);
        assert_eq!(memory_usage.total_num_bytes(), 1 + 2 + 4);
        drop(guard_2);
        assert_eq!(memory_usage.total_num_bytes(), 1 + 4);
        drop(guard_1);
        assert_eq!(memory_usage.total_num_bytes(), 4);
        drop(guard_4);
        assert_eq!(memory_usage.total_num_bytes(), 0);
    }

    #[tokio::test]
    async fn test_use_memory_with_limit_no_waiting() {
        let memory_usage = MemoryUsage::default();
        let _guard_100k = memory_usage.use_memory(100_000);
        let guard_1m = memory_usage.use_memory_with_limit(1_000_000, 500_000).await;
        assert_eq!(memory_usage.total_num_bytes(), 1_100_000);
        drop(guard_1m);
        assert_eq!(memory_usage.total_num_bytes(), 100_000);
    }

    #[tokio::test]
    async fn test_use_memory_with_limit_wait() {
        let memory_usage = MemoryUsage::default();
        let memory_usage_clone = memory_usage.clone();
        let _guard = memory_usage.use_memory(100_000);
        let join = tokio::task::spawn(async move {
            memory_usage_clone
                .use_memory_with_limit(1_000_000, 50_000)
                .await
        });
        tokio::task::yield_now().await;
        assert_eq!(memory_usage.total_num_bytes(), 100_000);
        drop(_guard);
        let _second_guard = join.await.unwrap();
        assert_eq!(memory_usage.total_num_bytes(), 1_000_000);
    }
}
