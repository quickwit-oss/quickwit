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

use hyper::{Request, Response};
use once_cell::sync::Lazy;
use quickwit_common::metrics::{
    new_counter_vec, new_gauge_vec, new_histogram_vec, HistogramVec, IntCounterVec, IntGaugeVec,
};
use tower_http::classify::{ServerErrorsAsFailures, SharedClassifier};
use tower_http::trace::{
    DefaultMakeSpan, DefaultOnBodyChunk, DefaultOnEos, OnFailure, OnRequest, OnResponse, TraceLayer,
};

pub struct RestMetrics {
    pub http_requests_total: IntCounterVec<3>,
    pub http_requests_pending: IntGaugeVec<2>,
    pub http_requests_duration_seconds: HistogramVec<3>,
}

impl Default for RestMetrics {
    //! - `http_requests_total` (labels: endpoint, method, status): the total number of HTTP
    //!   requests handled (counter)
    //! - `http_requests_duration_seconds` (labels: endpoint, method, status): the request duration
    //!   for all HTTP requests handled (histogram)
    //! - `http_requests_pending` (labels: endpoint, method): the number of currently in-flight
    //!   requests (gauge)
    fn default() -> Self {
        RestMetrics {
            http_requests_total: new_counter_vec(
                "http_requests_total",
                "Total total number of HTTP requests handled (counter)",
                "quickwit",
                ["method", "path", "status"],
            ),
            http_requests_pending: new_gauge_vec(
                "http_requests_pending",
                "Number of currently in-flight requests (gauge)",
                "quickwit",
                ["method", "path"],
            ),
            http_requests_duration_seconds: new_histogram_vec(
                "http_requests_duration_seconds",
                "Request duration for all HTTP requests handled (histogram)",
                "quickwit",
                ["method", "path", "status"],
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
    pub labels: Arc<Mutex<Vec<String>>>,
    _phantom: PhantomData<B>,
}

impl<B> Clone for RestMetricsRecorder<B> {
    fn clone(&self) -> Self {
        Self {
            labels: self.labels.clone(),
            _phantom: self._phantom,
        }
    }
}

impl<B> RestMetricsRecorder<B> {
    pub fn new() -> Self {
        Self {
            labels: Arc::new(Mutex::new(vec![])),
            _phantom: PhantomData,
        }
    }
}

impl<B, FailureClass> OnFailure<FailureClass> for RestMetricsRecorder<B> {
    fn on_failure(
        &mut self,
        _failure_classification: FailureClass,
        _latency: std::time::Duration,
        _span: &tracing::Span,
    ) {
        let labels = self.labels.lock().expect("Failed to unlock labels").clone();

        SERVE_METRICS
            .http_requests_pending
            .with_label_values([labels[0].as_str(), labels[1].as_str()])
            .inc();
    }
}

impl<B, RB> OnResponse<RB> for RestMetricsRecorder<B> {
    fn on_response(
        self,
        response: &Response<RB>,
        latency: std::time::Duration,
        _span: &tracing::Span,
    ) {
        let labels = self.labels.lock().expect("Failed to unlock labels").clone();

        let method = labels[0].as_str();
        let path = labels[1].as_str();
        SERVE_METRICS
            .http_requests_pending
            .with_label_values([method, path])
            .dec();

        SERVE_METRICS
            .http_requests_duration_seconds
            .with_label_values([method, path, response.status().as_str()])
            .observe(latency.as_secs_f64());

        SERVE_METRICS
            .http_requests_total
            .with_label_values([method, path, response.status().as_str()])
            .inc();
    }
}

impl<B, RB> OnRequest<RB> for RestMetricsRecorder<B> {
    fn on_request(&mut self, request: &Request<RB>, _span: &tracing::Span) {
        *self.labels.lock().unwrap() = vec![
            request.method().to_string(),
            request.uri().path().to_string(),
        ]
    }
}

pub fn make_rest_metrics_layer<B>() -> RestMetricsTraceLayer<B> {
    let metrics_recorder = RestMetricsRecorder::new();
    TraceLayer::new_for_http()
        .on_request(metrics_recorder.clone())
        .on_response(metrics_recorder.clone())
        .on_failure(metrics_recorder.clone())
}

/// Serve counters exposes a bunch a set of metrics about the request received to Quickwit.
pub static SERVE_METRICS: Lazy<RestMetrics> = Lazy::new(RestMetrics::default);
