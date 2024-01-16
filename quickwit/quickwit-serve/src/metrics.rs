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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hyper::{Request, Response};
use once_cell::sync::Lazy;
use quickwit_common::metrics::{
    new_counter, new_counter_vec, new_histogram_vec, Histogram, HistogramVec, IntCounter,
    IntCounterVec,
};
use tower_http::classify::{ServerErrorsAsFailures, SharedClassifier};
use tower_http::trace::{
    DefaultMakeSpan, DefaultOnBodyChunk, DefaultOnEos, OnFailure, OnRequest, OnResponse, TraceLayer,
};

pub struct RestMetrics {
    pub http_requests_total: IntCounterVec<2>,
    pub http_requests_error: IntCounter,
    pub http_requests_duration_secs: HistogramVec<2>,
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
            http_requests_error: new_counter(
                "http_requests_error",
                "Number of HTTP requests with errors",
                "quickwit",
            ),
            http_requests_duration_secs: new_histogram_vec(
                "http_requests_duration_secs",
                "Number of seconds required to run the HTTP request",
                "quickwit",
                ["method", "path"],
            ),
        }
    }
}

pub type RestMetricsTraceLayer<B> = TraceLayer<
    SharedClassifier<ServerErrorsAsFailures>,
    DefaultMakeSpan,
    RestMetricsRecorder<B>,
    RestMetricsRecorder<B>,
    DefaultOnBodyChunk,
    DefaultOnEos,
    RestMetricsRecorder<B>,
>;

/// Holds the state required for recording metrics on a given request.
pub struct RestMetricsRecorder<B> {
    pub histogram: Arc<Mutex<Option<Histogram>>>,
    _phantom: PhantomData<B>,
}

impl<B> Clone for RestMetricsRecorder<B> {
    fn clone(&self) -> Self {
        Self {
            histogram: self.histogram.clone(),
            _phantom: self._phantom,
        }
    }
}

impl<B> RestMetricsRecorder<B> {
    pub fn new() -> Self {
        Self {
            histogram: Arc::new(Mutex::new(None)),
            _phantom: PhantomData,
        }
    }

    fn record_latency(self, latency: Duration) {
        if let Some(histogram) = self
            .histogram
            .lock()
            .expect("Failed to unlock histogram")
            .clone()
        {
            histogram.observe(latency.as_secs_f64())
        }
    }
}

impl<B, FailureClass> OnFailure<FailureClass> for RestMetricsRecorder<B> {
    fn on_failure(
        &mut self,
        _failure_classification: FailureClass,
        latency: std::time::Duration,
        _span: &tracing::Span,
    ) {
        SERVE_METRICS.http_requests_error.inc();
        self.clone().record_latency(latency);
    }
}

impl<B, RB> OnResponse<RB> for RestMetricsRecorder<B> {
    fn on_response(
        self,
        _response: &Response<RB>,
        latency: std::time::Duration,
        _span: &tracing::Span,
    ) {
        self.clone().record_latency(latency);
    }
}

impl<B, RB> OnRequest<RB> for RestMetricsRecorder<B> {
    fn on_request(&mut self, request: &Request<RB>, _span: &tracing::Span) {
        let uri = request.uri().path();
        if uri.starts_with("/api") {
            SERVE_METRICS
                .http_requests_total
                .with_label_values([request.method().as_str(), uri])
                .inc();
            *self.histogram.lock().unwrap() = Some(
                SERVE_METRICS
                    .http_requests_duration_secs
                    .with_label_values([request.method().as_str(), uri]),
            );
        }
    }
}

pub fn make_rest_metrics_layer<B>() -> RestMetricsTraceLayer<B> {
    let metrics_recorder = RestMetricsRecorder::new();
    TraceLayer::new_for_http()
        .on_request(metrics_recorder.clone())
        .on_response(metrics_recorder.clone())
        .on_failure(metrics_recorder.clone())
}

/// Serve counters exposes a bunch a set of metrics about the request received to quickwit.
pub static SERVE_METRICS: Lazy<RestMetrics> = Lazy::new(RestMetrics::default);
