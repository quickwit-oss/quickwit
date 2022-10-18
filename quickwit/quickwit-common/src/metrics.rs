// Copyright (C) 2022 Quickwit, Inc.
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

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Future;
use pin_project_lite::pin_project;
use prometheus::{Encoder, HistogramOpts, Opts, TextEncoder};
pub use prometheus::{Histogram, HistogramTimer, HistogramVec, IntCounter, IntGauge};

pub fn new_counter(name: &str, description: &str, namespace: &str) -> IntCounter {
    let counter_opts = Opts::new(name, description).namespace(namespace);
    let counter = IntCounter::with_opts(counter_opts).expect("Failed to create counter");
    prometheus::register(Box::new(counter.clone())).expect("Failed to register counter");
    counter
}

pub fn new_histogram(name: &str, description: &str, namespace: &str) -> Histogram {
    let histogram_opts = HistogramOpts::new(name, description).namespace(namespace);
    let histogram = Histogram::with_opts(histogram_opts).expect("Failed to create counter");
    prometheus::register(Box::new(histogram.clone())).expect("Failed to register counter");
    histogram
}

pub fn new_histogram_vec(
    name: &str,
    description: &str,
    namespace: &str,
    label_names: &[&str],
) -> HistogramVec {
    let histogram_opts = HistogramOpts::new(name, description).namespace(namespace);
    let histogram_vec =
        HistogramVec::new(histogram_opts, label_names).expect("metric can be created");
    prometheus::register(Box::new(histogram_vec.clone())).expect("Failed to register counter");
    histogram_vec
}

pub fn new_gauge(name: &str, description: &str, namespace: &str) -> IntGauge {
    let gauge_opts = Opts::new(name, description).namespace(namespace);
    let gauge = IntGauge::with_opts(gauge_opts).expect("Failed to create gauge");
    prometheus::register(Box::new(gauge.clone())).expect("Failed to register gauge");
    gauge
}

pub fn metrics_handler() -> impl warp::Reply {
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    let _ = encoder.encode(&metric_families, &mut buffer); // TODO avoid ignoring the error.
    String::from_utf8_lossy(&buffer).to_string()
}

pub fn create_gauge_guard(gauge: &'static IntGauge) -> GaugeGuard {
    gauge.inc();
    GaugeGuard(gauge)
}

pub struct GaugeGuard(&'static IntGauge);

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.0.dec();
    }
}
pub trait InstrumentHistogramMetric: Sized {
    fn measure_time(self, metric: &HistogramVec, labels: &[&str]) -> InstrumentedMetric<Self> {
        let histogram_timer = metric.with_label_values(labels).start_timer();
        InstrumentedMetric {
            inner: self,
            histogram_timer: Some(histogram_timer),
        }
    }
}

pin_project! {
    pub struct InstrumentedMetric<T> {
        #[pin]
        inner: T,
        #[pin]
        histogram_timer: Option<HistogramTimer>,
    }
}

impl<T: Future> Future for InstrumentedMetric<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let output = match this.inner.poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(output) => output,
        };
        if let Some(histogram_timer) = this.histogram_timer.take() {
            histogram_timer.observe_duration();
        };
        Poll::Ready(output)
    }
}

impl<T: Future + Sized> InstrumentHistogramMetric for T {}
