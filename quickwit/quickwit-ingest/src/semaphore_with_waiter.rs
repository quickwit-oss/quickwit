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

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// `SemaphoreWithLimitedWaiters` is an extension of semaphore
/// that limits the number of waiters.
///
/// If more than n-waiters then acquire returns an error.
#[derive(Clone)]
pub struct SemaphoreWithMaxWaiters {
    permits: Arc<Semaphore>,
    // Implementation detail:
    // We do not use the async semaphore mechanics.
    // The waiter count could have implemented using a simple atomic counter.
    // We use a semaphore to avoid the need to reimplement the looping compare-and-swap dance.
    waiter_count: Arc<Semaphore>,
}

impl SemaphoreWithMaxWaiters {
    /// Creates a new `SemaphoreWithLimitedWaiters`.
    pub fn new(num_permits: usize, max_num_waiters: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(num_permits)),
            waiter_count: Arc::new(Semaphore::new(max_num_waiters)),
        }
    }

    /// Acquires a permit.
    pub async fn acquire(&self) -> Result<OwnedSemaphorePermit, ()> {
        match Semaphore::try_acquire_owned(self.permits.clone()) {
            Ok(permit) => {
                return Ok(permit);
            }
            Err(TryAcquireError::NoPermits) => {}
            Err(TryAcquireError::Closed) => {
                panic!("semaphore closed (this should never happen)");
            }
        };
        // We bind wait_permit to a variable to extend its lifetime,
        // (so we keep holding the associated permit).
        let _wait_permit = match Semaphore::try_acquire_owned(self.waiter_count.clone()) {
            Ok(wait_permit) => wait_permit,
            Err(TryAcquireError::NoPermits) => {
                return Err(());
            }
            Err(TryAcquireError::Closed) => {
                // The `permits` semaphore should never be closed because we don't expose the
                // `close` API and never call `close` internally.
                panic!("semaphore closed");
            }
        };
        let permit = Semaphore::acquire_owned(self.permits.clone())
            .await
            .expect("semaphore closed "); // (See justification above)
        Ok(permit)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    #[tokio::test]
    async fn test_semaphore_with_waiters() {
        let semaphore_with_waiters = super::SemaphoreWithMaxWaiters::new(1, 1);
        let permit = semaphore_with_waiters.acquire().await.unwrap();
        let semaphore_with_waiters_clone = semaphore_with_waiters.clone();
        let join_handle =
            tokio::task::spawn(async move { semaphore_with_waiters_clone.acquire().await });
        assert!(!join_handle.is_finished());
        tokio::time::sleep(Duration::from_millis(500)).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(semaphore_with_waiters.acquire().await.is_err());
        assert!(!join_handle.is_finished());
        drop(permit);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(join_handle.await.is_ok());
        assert!(semaphore_with_waiters.acquire().await.is_ok());
    }
}
