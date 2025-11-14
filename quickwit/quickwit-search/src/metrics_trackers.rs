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
use quickwit_proto::search::{LeafSearchResponse, SearchResponse};

use crate::SearchError;
use crate::metrics::SEARCH_METRICS;

// planning

/// Wrapper around the plan future to tracks error/cancellation metrics.
/// Planning phase success isn't explicitely recorded as it can be deduced from
/// the search phase metrics.
#[pin_project(PinnedDrop)]
pub struct SearchPlanMetricsFuture<F> {
    #[pin]
    pub tracked: F,
    pub start: Instant,
    pub is_success: Option<bool>,
}

#[pinned_drop]
impl<F> PinnedDrop for SearchPlanMetricsFuture<F> {
    fn drop(self: Pin<&mut Self>) {
        let status = match self.is_success {
            // this is a partial success, actual status will be recorded during the search step
            Some(true) => return,
            Some(false) => "plan-error",
            None => "plan-cancelled",
        };

        let label_values = [status];
        SEARCH_METRICS
            .root_search_requests_total
            .with_label_values(label_values)
            .inc();
        SEARCH_METRICS
            .root_search_request_duration_seconds
            .with_label_values(label_values)
            .observe(self.start.elapsed().as_secs_f64());
    }
}

impl<F, R> Future for SearchPlanMetricsFuture<F>
where F: Future<Output = crate::Result<R>>
{
    type Output = crate::Result<R>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let response = ready!(this.tracked.poll(cx));
        if let Err(err) = &response {
            tracing::error!(?err, "root search planning failed");
        }
        *this.is_success = Some(response.is_ok());
        Poll::Ready(Ok(response?))
    }
}

// root search

/// Wrapper around the root search futures to track metrics.
#[pin_project(PinnedDrop)]
pub struct RootSearchMetricsFuture<F> {
    #[pin]
    pub tracked: F,
    pub start: Instant,
    pub num_targeted_splits: usize,
    pub status: Option<&'static str>,
}

#[pinned_drop]
impl<F> PinnedDrop for RootSearchMetricsFuture<F> {
    fn drop(self: Pin<&mut Self>) {
        let status = self.status.unwrap_or("cancelled");
        let label_values = [status];
        SEARCH_METRICS
            .root_search_requests_total
            .with_label_values(label_values)
            .inc();
        SEARCH_METRICS
            .root_search_request_duration_seconds
            .with_label_values(label_values)
            .observe(self.start.elapsed().as_secs_f64());
        SEARCH_METRICS
            .root_search_targeted_splits
            .with_label_values(label_values)
            .observe(self.num_targeted_splits as f64);
    }
}

impl<F> Future for RootSearchMetricsFuture<F>
where F: Future<Output = crate::Result<SearchResponse>>
{
    type Output = crate::Result<SearchResponse>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let response = ready!(this.tracked.poll(cx));
        if let Err(err) = &response {
            tracing::error!(?err, "root search failed");
        }
        if let Ok(resp) = &response {
            if resp.failed_splits.is_empty() {
                *this.status = Some("success");
            } else {
                *this.status = Some("partial-success");
            }
        } else {
            *this.status = Some("error");
        }
        Poll::Ready(Ok(response?))
    }
}

// leaf search

/// Wrapper around the search future to track metrics.
#[pin_project(PinnedDrop)]
pub struct LeafSearchMetricsFuture<F> {
    #[pin]
    pub tracked: F,
    pub start: Instant,
    pub targeted_splits: usize,
    pub status: Option<&'static str>,
}

#[pinned_drop]
impl<F> PinnedDrop for LeafSearchMetricsFuture<F> {
    fn drop(self: Pin<&mut Self>) {
        let label_values = [self.status.unwrap_or("cancelled")];
        SEARCH_METRICS
            .leaf_search_requests_total
            .with_label_values(label_values)
            .inc();
        SEARCH_METRICS
            .leaf_search_request_duration_seconds
            .with_label_values(label_values)
            .observe(self.start.elapsed().as_secs_f64());
        SEARCH_METRICS
            .leaf_search_targeted_splits
            .with_label_values(label_values)
            .observe(self.targeted_splits as f64);
    }
}

impl<F> Future for LeafSearchMetricsFuture<F>
where F: Future<Output = Result<LeafSearchResponse, SearchError>>
{
    type Output = Result<LeafSearchResponse, SearchError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let response = ready!(this.tracked.poll(cx));
        *this.status = match &response {
            Ok(resp) if !resp.failed_splits.is_empty() => Some("partial-success"),
            Ok(_) => Some("success"),
            Err(_) => Some("error"),
        };
        Poll::Ready(Ok(response?))
    }
}
