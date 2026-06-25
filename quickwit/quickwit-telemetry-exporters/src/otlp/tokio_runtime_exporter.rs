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

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use opentelemetry::logs::Severity;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::logs::{LogBatch, LogExporter};
use opentelemetry_sdk::metrics::Temporality;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::trace::{SpanData, SpanExporter};

pub(super) struct TokioRuntimeExporter<E> {
    inner: E,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl<E> TokioRuntimeExporter<E> {
    pub(super) fn new(inner: E) -> anyhow::Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(1)
            .thread_name("opentelemetry_exporter_runtime")
            .build()
            .context("failed to create OpenTelemetry exporter Tokio runtime")?;
        Ok(Self {
            inner,
            runtime: Arc::new(runtime),
        })
    }
}

impl<E> fmt::Debug for TokioRuntimeExporter<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokioRuntimeExporter")
            .finish_non_exhaustive()
    }
}

impl<E> SpanExporter for TokioRuntimeExporter<E>
where
    E: SpanExporter,
{
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        self.runtime.block_on(self.inner.export(batch))
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

impl<E> LogExporter for TokioRuntimeExporter<E>
where
    E: LogExporter,
{
    async fn export(&self, batch: LogBatch<'_>) -> OTelSdkResult {
        self.runtime.block_on(self.inner.export(batch))
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn event_enabled(&self, level: Severity, target: &str, name: Option<&str>) -> bool {
        self.inner.event_enabled(level, target, name)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

impl<E> PushMetricExporter for TokioRuntimeExporter<E>
where
    E: PushMetricExporter,
{
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        self.runtime.block_on(self.inner.export(metrics))
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn temporality(&self) -> Temporality {
        self.inner.temporality()
    }
}
