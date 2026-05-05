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

// See https://prometheus.io/docs/practices/naming/

use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Instant;

use pin_project::{pin_project, pinned_drop};
use quickwit_metrics::{counter, histogram, label_values};
use quickwit_proto::search::LeafSearchResponse;

use crate::SearchError;
use crate::metrics::{
    LEAF_SEARCH_REQUEST_DURATION_SECONDS, LEAF_SEARCH_REQUESTS_TOTAL, LEAF_SEARCH_TARGETED_SPLITS,
    ROOT_SEARCH_REQUEST_DURATION_SECONDS, ROOT_SEARCH_REQUESTS_TOTAL, ROOT_SEARCH_TARGETED_SPLITS,
    STATUS_LABELS,
};

// root

pub enum RootSearchMetricsStep {
    Plan,
    Exec { num_targeted_splits: usize },
}

/// Wrapper around the plan and search futures to track metrics.
#[pin_project(PinnedDrop)]
pub struct RootSearchMetricsFuture<F> {
    #[pin]
    pub tracked: F,
    pub start: Instant,
    pub step: RootSearchMetricsStep,
    pub is_success: Option<bool>,
}

#[pinned_drop]
impl<F> PinnedDrop for RootSearchMetricsFuture<F> {
    fn drop(self: Pin<&mut Self>) {
        let (num_targeted_splits, status) = match (&self.step, self.is_success) {
            // is is a partial success, actual success is recorded during the search step
            (RootSearchMetricsStep::Plan, Some(true)) => return,
            (RootSearchMetricsStep::Plan, Some(false)) => (0, "plan-error"),
            (RootSearchMetricsStep::Plan, None) => (0, "plan-cancelled"),
            (
                RootSearchMetricsStep::Exec {
                    num_targeted_splits,
                },
                Some(true),
            ) => (*num_targeted_splits, "success"),
            (
                RootSearchMetricsStep::Exec {
                    num_targeted_splits,
                },
                Some(false),
            ) => (*num_targeted_splits, "error"),
            (
                RootSearchMetricsStep::Exec {
                    num_targeted_splits,
                },
                None,
            ) => (*num_targeted_splits, "cancelled"),
        };

        let labels = label_values!(STATUS_LABELS, [status]);
        counter!(
            parent: ROOT_SEARCH_REQUESTS_TOTAL,
            labels: labels,
        )
        .increment(1);
        histogram!(
            parent: ROOT_SEARCH_REQUEST_DURATION_SECONDS,
            labels: labels,
        )
        .record(self.start.elapsed().as_secs_f64());
        histogram!(
            parent: ROOT_SEARCH_TARGETED_SPLITS,
            labels: labels,
        )
        .record(num_targeted_splits as f64);
    }
}

impl<F, R, E> Future for RootSearchMetricsFuture<F>
where F: Future<Output = Result<R, E>>
{
    type Output = Result<R, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let response = ready!(this.tracked.poll(cx));
        *this.is_success = Some(response.is_ok());
        Poll::Ready(Ok(response?))
    }
}

// leaf

/// Wrapper around the search future to track metrics.
#[pin_project(PinnedDrop)]
pub struct LeafSearchMetricsFuture<F>
where F: Future<Output = Result<LeafSearchResponse, SearchError>>
{
    #[pin]
    pub tracked: F,
    pub start: Instant,
    pub targeted_splits: usize,
    pub status: Option<&'static str>,
}

#[pinned_drop]
impl<F> PinnedDrop for LeafSearchMetricsFuture<F>
where F: Future<Output = Result<LeafSearchResponse, SearchError>>
{
    fn drop(self: Pin<&mut Self>) {
        let status = self.status.unwrap_or("cancelled");
        let labels = label_values!(STATUS_LABELS, [status]);
        counter!(
            parent: LEAF_SEARCH_REQUESTS_TOTAL,
            labels: labels,
        )
        .increment(1);
        histogram!(
            parent: LEAF_SEARCH_REQUEST_DURATION_SECONDS,
            labels: labels,
        )
        .record(self.start.elapsed().as_secs_f64());
        histogram!(
            parent: LEAF_SEARCH_TARGETED_SPLITS,
            labels: labels,
        )
        .record(self.targeted_splits as f64);
    }
}

impl<F> Future for LeafSearchMetricsFuture<F>
where F: Future<Output = Result<LeafSearchResponse, SearchError>>
{
    type Output = Result<LeafSearchResponse, SearchError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let response = ready!(this.tracked.poll(cx));
        *this.status = if response.is_ok() {
            Some("success")
        } else {
            Some("error")
        };
        Poll::Ready(Ok(response?))
    }
}
