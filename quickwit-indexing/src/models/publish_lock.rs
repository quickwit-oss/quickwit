// Copyright (C) 2022 Quickwit, Inc.
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

use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

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

#[derive(Debug)]
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
        assert!(timeout(Duration::from_millis(50), lock.kill())
            .await
            .is_err());
        drop(guard);

        lock.kill().await;
        assert!(lock.is_dead());
        assert!(lock.acquire().await.is_none());
    }
}
