// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::sync::Arc;

/// Semaphore for which the guard has a 'static lifetime.
/// This is just a wrapper over the normal tokio Semaphore.
///
/// This is a trick to get semaphore for which the SemaphoreGuard has a 'static lifetime.
pub(crate) struct Semaphore {
    inner: Arc<tokio::sync::Semaphore>,
}

impl Semaphore {
    /// Creates a semaphore allowing `num_permits`.
    pub fn new(num_permits: usize) -> Semaphore {
        let inner_semaphore = tokio::sync::Semaphore::new(num_permits);
        Semaphore {
            inner: Arc::new(inner_semaphore),
        }
    }

    /// Tries to acquire a new permit.
    /// If no permit is available, wait asynchronously for a permit to be released.
    pub async fn acquire(&self) -> SemaphoreGuard {
        self.inner
            .acquire()
            .await
            .expect("This should never fail. The semaphore does not allow close().")
            .forget();
        SemaphoreGuard {
            permits: self.inner.clone(),
        }
    }
}

/// The semaphore guard defines -by its lifetime- the amount of time
/// the permit given by the semaphore is held.
///
/// Upon drop, the semaphore is increased.
pub struct SemaphoreGuard {
    permits: Arc<tokio::sync::Semaphore>,
}

impl Drop for SemaphoreGuard {
    fn drop(&mut self) {
        self.permits.add_permits(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn test_semaphore() {
        let finished_task = Arc::new(AtomicUsize::default());
        const NUM_CONCURRENT_TASKS: usize = 3;
        let semaphore = Semaphore::new(NUM_CONCURRENT_TASKS);
        for i in 1..1_000 {
            let guard = semaphore.acquire().await;
            // Thanks to the semaphore we have the guarantee that at most NUM_CONCURRENT_TASKS tasks are running
            // at the same time.
            // In other words, upon acquisition of guard, we know that at least (i - 3) tasks have terminated.
            assert!(finished_task.load(Ordering::SeqCst) + NUM_CONCURRENT_TASKS >= i);
            let finished_task_clone = finished_task.clone();
            tokio::task::spawn(async move {
                finished_task_clone.fetch_add(1, Ordering::SeqCst);
                mem::drop(guard);
            });
        }
    }
}
