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

use std::marker::PhantomData;

use hyper::{Response, Request};
use once_cell::sync::Lazy;
use quickwit_common::metrics::{new_counter_vec, IntCounterVec, HistogramVec, new_histogram_vec, Histogram};
use tower_http::trace::{OnFailure, OnResponse, OnRequest};

pub struct RestMetrics {
    pub http_requests_total: IntCounterVec<2>,
    pub http_requests_duration: HistogramVec<2>,
}

impl Default for RestMetrics {
    fn default() -> Self {
        RestMetrics {
            http_requests_total: new_counter_vec(
                "http_requests_total",
                "Total number of HTTP requests received",
                "quickwit",
                ["method", "path"],
            ),
            http_requests_duration: new_histogram_vec(
                "http_requests_duration",
                "Duration of HTTP requests",
                "quickwit",
                ["method", "path"],
            )
        }
    }
}


pub struct MetricsRecorder<B> {
    histogram: Option<Histogram>,
    _phantom: PhantomData<B>,
}

impl<B> MetricsRecorder<B> {
    pub fn new() -> Self {
        Self {
            histogram: None,
            _phantom: PhantomData,
        }
    }
}

impl<B> Clone for MetricsRecorder<B>
{
    fn clone(&self) -> Self {
        Self {
            histogram: self.histogram.clone(),
            _phantom: self._phantom.clone(),
        }
    }
}

impl<B, FailureClass> OnFailure<FailureClass> for MetricsRecorder<B>
{
    fn on_failure(
        &mut self,
        _failure_classification: FailureClass,
        _latency: std::time::Duration,
        _span: &tracing::Span,
    ) {
        // TODO
    }
}

impl<B, RB> OnResponse<RB> for MetricsRecorder<B>
{
    fn on_response(
        self,
        _response: &Response<RB>,
        latency: std::time::Duration,
        _span: &tracing::Span,
    ) {
        self.histogram.expect("msg").observe(latency.as_secs_f64());
    }
}


impl<B, RB> OnRequest<RB> for MetricsRecorder<B>
{
    fn on_request(&mut self, request: &Request<RB>, _span: &tracing::Span) {
        SERVE_METRICS.http_requests_total
            .with_label_values([request.method().as_str(), request.uri().path()])
            .inc();
        self.histogram = Some(SERVE_METRICS.http_requests_duration
            .with_label_values([request.method().as_str(), request.uri().path()]));
    }
}



/// Serve counters exposes a bunch a set of metrics about the request received to quickwit.
pub static SERVE_METRICS: Lazy<RestMetrics> = Lazy::new(RestMetrics::default);
