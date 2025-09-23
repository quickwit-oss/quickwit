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

use std::time::Duration;

use quickwit_common::metrics::{GaugeGuard, IntGauge};
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::rest::TooManyRequests;

pub struct LoadShield {
    in_flight_semaphore_opt: Option<Semaphore>, // This one is doing the load shedding.
    concurrency_semaphore_opt: Option<Semaphore>,
    ongoing_gauge: IntGauge,
    pending_gauge: IntGauge,
}

pub struct LoadShieldPermit {
    _concurrency_permit_opt: Option<SemaphorePermit<'static>>,
    _in_flight_permit_opt: Option<SemaphorePermit<'static>>,
    _ongoing_gauge_guard: GaugeGuard<'static>,
}

impl LoadShield {
    pub fn new(endpoint_group: &'static str) -> LoadShield {
        let endpoint_group_uppercase = endpoint_group.to_ascii_uppercase();
        let max_in_flight_env_key = format!("QW_{endpoint_group_uppercase}_MAX_IN_FLIGHT");
        let max_concurrency_env_key = format!("QW_{endpoint_group_uppercase}_MAX_CONCURRENCY");
        let max_in_flight_opt: Option<usize> =
            quickwit_common::get_from_env_opt(&max_in_flight_env_key, false);
        let max_concurrency_opt: Option<usize> =
            quickwit_common::get_from_env_opt(&max_concurrency_env_key, false);
        let in_flight_semaphore_opt = max_in_flight_opt.map(Semaphore::new);
        let concurrency_semaphore_opt = max_concurrency_opt.map(Semaphore::new);
        let pending_gauge = crate::metrics::SERVE_METRICS
            .pending_requests
            .with_label_values([endpoint_group]);
        let ongoing_gauge = crate::metrics::SERVE_METRICS
            .ongoing_requests
            .with_label_values([endpoint_group]);
        LoadShield {
            in_flight_semaphore_opt,
            concurrency_semaphore_opt,
            ongoing_gauge,
            pending_gauge,
        }
    }

    async fn acquire_in_flight_permit(
        &'static self,
    ) -> Result<Option<SemaphorePermit<'static>>, warp::Rejection> {
        let Some(in_flight_semaphore) = &self.in_flight_semaphore_opt else {
            return Ok(None);
        };
        let Ok(in_flight_permit) = in_flight_semaphore.try_acquire() else {
            // Wait a little to deal before load shedding. The point is to lower the load associated
            // with super aggressive clients.
            tokio::time::sleep(Duration::from_millis(100)).await;
            return Err(warp::reject::custom(TooManyRequests));
        };
        Ok(Some(in_flight_permit))
    }

    async fn acquire_concurrency_permit(&'static self) -> Option<SemaphorePermit<'static>> {
        let concurrency_semaphore = self.concurrency_semaphore_opt.as_ref()?;
        Some(concurrency_semaphore.acquire().await.unwrap())
    }

    pub async fn acquire_permit(&'static self) -> Result<LoadShieldPermit, warp::Rejection> {
        let mut pending_gauge_guard = GaugeGuard::from_gauge(&self.pending_gauge);
        pending_gauge_guard.add(1);
        let in_flight_permit_opt = self.acquire_in_flight_permit().await?;
        let concurrency_permit_opt = self.acquire_concurrency_permit().await;
        drop(pending_gauge_guard);
        let mut ongoing_gauge_guard = GaugeGuard::from_gauge(&self.ongoing_gauge);
        ongoing_gauge_guard.add(1);
        Ok(LoadShieldPermit {
            _in_flight_permit_opt: in_flight_permit_opt,
            _concurrency_permit_opt: concurrency_permit_opt,
            _ongoing_gauge_guard: ongoing_gauge_guard,
        })
    }
}
