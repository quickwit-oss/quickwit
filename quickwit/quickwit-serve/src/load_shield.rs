use std::time::Duration;

use tokio::sync::{Semaphore, SemaphorePermit};
use warp::reject;

pub struct LoadShield {
    concurrency_semaphore: Semaphore,
    load_shed_semaphore: Semaphore,
}

#[derive(Debug)]
struct TooManyRequests;
impl reject::Reject for TooManyRequests {}


#[derive(Debug)]
struct Timeout;
impl reject::Reject for Timeout {}

pub struct LoadShieldPermit {
    _concurrency_permit: SemaphorePermit<'static>,
    _load_shed_permit: SemaphorePermit<'static>,
}

impl LoadShield {
    pub const fn new(max_concurrency: usize, max_pending: usize) -> Self {
        LoadShield {
            concurrency_semaphore: Semaphore::const_new(max_concurrency),
            load_shed_semaphore: Semaphore::const_new(max_pending),
        }
    }

    pub async fn acquire_permit(&'static self) -> Result<LoadShieldPermit, warp::Rejection> {
        let Ok(load_shed_permit) = self.load_shed_semaphore.try_acquire() else {
            // Wait a little to deal before load shedding. The point is to lower the load associated
            // with super aggressive clients.
            tokio::time::sleep(Duration::from_millis(100)).await;
            return Err(warp::reject::custom(TooManyRequests))
        };
        let Ok(concurrency_permit) = self.concurrency_semaphore.acquire().await else {
            // todo internal error
            return Err(warp::reject::custom(TooManyRequests))
        };
        Ok(LoadShieldPermit { _concurrency_permit: concurrency_permit, _load_shed_permit: load_shed_permit })
    }
}
