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

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use quickwit_common::metrics::GaugeGuard;
use tokio::sync::oneshot;

/// `SearchPermitProvider` is a distributor of permits to perform single split
/// search operation.
///
/// Requests are served in order.
#[derive(Clone)]
pub struct SearchPermitProvider {
    inner_arc: Arc<Mutex<InnerSearchPermitProvider>>,
}

impl SearchPermitProvider {
    pub fn new(num_permits: usize) -> SearchPermitProvider {
        SearchPermitProvider {
            inner_arc: Arc::new(Mutex::new(InnerSearchPermitProvider {
                num_permits_available: num_permits,
                permits_requests: VecDeque::new(),
            })),
        }
    }

    /// Returns a future permit in the form of a oneshot Receiver channel.
    ///
    /// At this point the permit is not acquired yet.
    #[must_use]
    pub fn get_permit(&self) -> oneshot::Receiver<SearchPermit> {
        let mut permits_lock = self.inner_arc.lock().unwrap();
        permits_lock.get_permit(&self.inner_arc)
    }

    /// Returns a list of future permits in the form of oneshot Receiver channels.
    ///
    /// The permits returned are guaranteed to be resolved in order.
    /// In addition, the permits are guaranteed to be resolved before permits returned by
    /// subsequent calls to this function (or `get_permit`).
    #[must_use]
    pub fn get_permits(&self, num_permits: usize) -> Vec<oneshot::Receiver<SearchPermit>> {
        let mut permits_lock = self.inner_arc.lock().unwrap();
        permits_lock.get_permits(num_permits, &self.inner_arc)
    }
}

struct InnerSearchPermitProvider {
    num_permits_available: usize,
    permits_requests: VecDeque<oneshot::Sender<SearchPermit>>,
}

impl InnerSearchPermitProvider {
    fn get_permit(
        &mut self,
        inner_arc: &Arc<Mutex<InnerSearchPermitProvider>>,
    ) -> oneshot::Receiver<SearchPermit> {
        let (tx, rx) = oneshot::channel();
        self.permits_requests.push_back(tx);
        self.assign_available_permits(inner_arc);
        rx
    }

    fn get_permits(
        &mut self,
        num_permits: usize,
        inner_arc: &Arc<Mutex<InnerSearchPermitProvider>>,
    ) -> Vec<oneshot::Receiver<SearchPermit>> {
        let mut permits = Vec::with_capacity(num_permits);
        for _ in 0..num_permits {
            let (tx, rx) = oneshot::channel();
            self.permits_requests.push_back(tx);
            permits.push(rx);
        }
        self.assign_available_permits(inner_arc);
        permits
    }

    fn recycle_permit(&mut self, inner_arc: &Arc<Mutex<InnerSearchPermitProvider>>) {
        self.num_permits_available += 1;
        self.assign_available_permits(inner_arc);
    }

    fn assign_available_permits(&mut self, inner_arc: &Arc<Mutex<InnerSearchPermitProvider>>) {
        while self.num_permits_available > 0 {
            let Some(sender) = self.permits_requests.pop_front() else {
                break;
            };
            let mut ongoing_gauge_guard = GaugeGuard::from_gauge(
                &crate::SEARCH_METRICS.leaf_search_single_split_tasks_ongoing,
            );
            ongoing_gauge_guard.add(1);
            let send_res = sender.send(SearchPermit {
                _ongoing_gauge_guard: ongoing_gauge_guard,
                inner_arc: inner_arc.clone(),
                recycle_on_drop: true,
            });
            match send_res {
                Ok(()) => {
                    self.num_permits_available -= 1;
                }
                Err(search_permit) => {
                    search_permit.drop_without_recycling_permit();
                }
            }
        }
        crate::SEARCH_METRICS
            .leaf_search_single_split_tasks_pending
            .set(self.permits_requests.len() as i64);
    }
}

pub struct SearchPermit {
    _ongoing_gauge_guard: GaugeGuard<'static>,
    inner_arc: Arc<Mutex<InnerSearchPermitProvider>>,
    recycle_on_drop: bool,
}

impl SearchPermit {
    fn drop_without_recycling_permit(mut self) {
        self.recycle_on_drop = false;
        drop(self);
    }
}

impl Drop for SearchPermit {
    fn drop(&mut self) {
        if !self.recycle_on_drop {
            return;
        }
        let mut inner_guard = self.inner_arc.lock().unwrap();
        inner_guard.recycle_permit(&self.inner_arc.clone());
    }
}

#[cfg(test)]
mod tests {
    use tokio::task::JoinSet;

    use super::*;

    #[tokio::test]
    async fn test_search_permits_get_permits_future() {
        // We test here that `get_permits_futures` does not interleave
        let search_permits = SearchPermitProvider::new(1);
        let mut all_futures = Vec::new();
        let first_batch_of_permits = search_permits.get_permits(10);
        assert_eq!(first_batch_of_permits.len(), 10);
        all_futures.extend(
            first_batch_of_permits
                .into_iter()
                .enumerate()
                .map(move |(i, fut)| ((1, i), fut)),
        );

        let second_batch_of_permits = search_permits.get_permits(10);
        assert_eq!(second_batch_of_permits.len(), 10);
        all_futures.extend(
            second_batch_of_permits
                .into_iter()
                .enumerate()
                .map(move |(i, fut)| ((2, i), fut)),
        );

        use rand::seq::SliceRandom;
        // not super useful, considering what join set does, but still a tiny bit more sound.
        all_futures.shuffle(&mut rand::thread_rng());

        let mut join_set = JoinSet::new();
        for (res, fut) in all_futures {
            join_set.spawn(async move {
                let permit = fut.await;
                (res, permit)
            });
        }
        let mut ordered_result: Vec<(usize, usize)> = Vec::with_capacity(20);
        while let Some(Ok(((batch_id, order), _permit))) = join_set.join_next().await {
            ordered_result.push((batch_id, order));
        }

        assert_eq!(ordered_result.len(), 20);
        for (i, res) in ordered_result[0..10].iter().enumerate() {
            assert_eq!(res, &(1, i));
        }
        for (i, res) in ordered_result[10..20].iter().enumerate() {
            assert_eq!(res, &(2, i));
        }
    }

    #[tokio::test]
    async fn test_search_permits_receiver_race_condition() {
        // Here we test that we don't have a problem if the Receiver is dropped.
        // In particular, we want to check that there is not a race condition where drop attempts to
        // lock the mutex.
        let search_permits = SearchPermitProvider::new(1);
        let permit_rx = search_permits.get_permit();
        let permit_rx2 = search_permits.get_permit();
        drop(permit_rx2);
        drop(permit_rx);
        let _permit_rx = search_permits.get_permit();
    }
}
