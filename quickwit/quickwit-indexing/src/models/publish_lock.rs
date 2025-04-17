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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::{Mutex, MutexGuard};

// Publisher locks have two clients: publishers and sources.
//
// Publishers must acquire the lock and ensure that the lock is alive before publishing.
//
// When a partition reassignment occurs, sources must (i) acquire, then (ii) kill, and finally (iii)
// release the lock before propagating a new lock via message passing to the downstream consumers.
#[derive(Clone, Default)]
pub struct PublishLock {
    inner: Arc<PublishLockInner>,
}

impl PartialEq for PublishLock {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self.inner.as_ref(), other.inner.as_ref())
    }
}

impl Debug for PublishLock {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        fmt.debug_struct("PublishLock")
            .field("is_alive", &self.is_alive())
            .finish()
    }
}

struct PublishLockInner {
    alive: AtomicBool,
    mutex: Mutex<()>,
}

impl Default for PublishLockInner {
    fn default() -> Self {
        Self {
            alive: AtomicBool::new(true),
            mutex: Mutex::default(),
        }
    }
}

impl PublishLock {
    pub fn dead() -> Self {
        PublishLock {
            inner: Arc::new(PublishLockInner {
                alive: AtomicBool::new(false),
                mutex: Mutex::default(),
            }),
        }
    }
    pub async fn acquire(&self) -> Option<MutexGuard<'_, ()>> {
        let guard = self.inner.mutex.lock().await;
        if self.is_dead() {
            return None;
        }
        Some(guard)
    }

    pub fn is_alive(&self) -> bool {
        self.inner.alive.load(Ordering::Relaxed)
    }

    pub fn is_dead(&self) -> bool {
        !self.is_alive()
    }

    pub async fn kill(&self) {
        let _guard = self.inner.mutex.lock().await;
        self.inner.alive.store(false, Ordering::Relaxed);
    }
}

#[derive(Debug, PartialEq)]
pub struct NewPublishLock(pub PublishLock);

#[cfg(test)]
mod tests {

    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn test_publish_lock() {
        let lock = PublishLock::default();
        assert!(lock.is_alive());

        let guard = lock.acquire().await.unwrap();
        assert!(
            timeout(Duration::from_millis(50), lock.kill())
                .await
                .is_err()
        );
        drop(guard);

        lock.kill().await;
        assert!(lock.is_dead());
        assert!(lock.acquire().await.is_none());
    }

    #[test]
    fn test_publish_lock_dead() {
        let publish_lock = PublishLock::dead();
        assert!(publish_lock.is_dead());
    }
}
