// Copyright (C) 2024 Quickwit, Inc.
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

use quickwit_common::metrics::GaugeGuard;
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
    _ongoing_gauge_guard: GaugeGuard<'static>,
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
            // tokio::time::sleep(Duration::from_millis(100)).await;
            return Err(warp::reject::custom(TooManyRequests))
        };
        let mut pending_gauge_guard = GaugeGuard::from_gauge(&crate::metrics::SERVE_METRICS.ingest_pending_requests);
        pending_gauge_guard.add(1);
        let Ok(concurrency_permit) = self.concurrency_semaphore.acquire().await else {
            // todo internal error
                        return Err(warp::reject::custom(TooManyRequests))
        };
        drop(pending_gauge_guard);
        let mut ongoing_gauge_guard = GaugeGuard::from_gauge(&crate::metrics::SERVE_METRICS.ingest_ongoing_requests);
        ongoing_gauge_guard.add(1);
        Ok(LoadShieldPermit { _concurrency_permit: concurrency_permit, _load_shed_permit: load_shed_permit, _ongoing_gauge_guard: ongoing_gauge_guard })
    }
}
