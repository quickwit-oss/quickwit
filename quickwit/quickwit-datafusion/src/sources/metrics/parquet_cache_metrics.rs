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

//! Per-query cache metrics for metrics parquet scans.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result as DFResult;
use datafusion::parquet;
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource_parquet::ParquetFileReaderFactory;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricType,
};
use futures::FutureExt;
use futures::future::BoxFuture;
use quickwit_storage::{
    StorageCacheMetrics, StorageCacheMetricsSnapshot, with_storage_cache_metrics,
};
use tracing::warn;

pub(super) fn instrument_parquet_file_reader_factory(
    inner: Arc<dyn ParquetFileReaderFactory>,
) -> Arc<dyn ParquetFileReaderFactory> {
    Arc::new(InstrumentedParquetFileReaderFactory { inner })
}

pub fn instrument_parquet_range_cache_metrics(
    plan: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    match Arc::clone(&plan).transform_up(|plan| {
        if let Some(rewritten) = instrument_parquet_scan(&plan) {
            Ok(Transformed::yes(rewritten))
        } else {
            Ok(Transformed::no(plan))
        }
    }) {
        Ok(transformed) => transformed.data,
        Err(error) => {
            warn!(%error, "failed to install parquet cache metrics on worker plan");
            plan
        }
    }
}

fn instrument_parquet_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    let data_source_exec = plan.as_any().downcast_ref::<DataSourceExec>()?;
    let (file_scan_config, parquet_source) =
        data_source_exec.downcast_to_file_source::<ParquetSource>()?;
    let reader_factory = parquet_source.parquet_file_reader_factory()?.clone();

    let parquet_source = parquet_source
        .clone()
        .with_parquet_file_reader_factory(instrument_parquet_file_reader_factory(reader_factory));
    let file_scan_config = FileScanConfigBuilder::from(file_scan_config.clone())
        .with_source(Arc::new(parquet_source))
        .build();
    let rewritten: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(file_scan_config);
    Some(rewritten)
}

#[derive(Debug)]
struct InstrumentedParquetFileReaderFactory {
    inner: Arc<dyn ParquetFileReaderFactory>,
}

impl ParquetFileReaderFactory for InstrumentedParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        partitioned_file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> DFResult<Box<dyn AsyncFileReader + Send>> {
        let cache_metrics = Arc::new(StorageCacheMetrics::default());
        let counters = StorageCacheMetricCounters::new(partition_index, metrics);
        let reader = self.inner.create_reader(
            partition_index,
            partitioned_file,
            metadata_size_hint,
            metrics,
        )?;
        Ok(Box::new(StorageCacheObservedReader {
            inner: reader,
            cache_metrics,
            counters,
        }))
    }
}

#[derive(Clone)]
struct StorageCacheMetricCounters {
    range_hit_bytes: Count,
    range_miss_bytes: Count,
    range_hit_count: Count,
    range_miss_count: Count,
    footer_hit_count: Count,
    footer_miss_count: Count,
}

impl StorageCacheMetricCounters {
    fn new(partition_index: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let builder = || MetricBuilder::new(metrics).with_type(MetricType::SUMMARY);
        Self {
            range_hit_bytes: builder().counter("parquet_range_cache_hit_bytes", partition_index),
            range_miss_bytes: builder().counter("parquet_range_cache_miss_bytes", partition_index),
            range_hit_count: builder().counter("parquet_range_cache_hit_count", partition_index),
            range_miss_count: builder().counter("parquet_range_cache_miss_count", partition_index),
            footer_hit_count: builder().counter("parquet_footer_cache_hit_count", partition_index),
            footer_miss_count: builder()
                .counter("parquet_footer_cache_miss_count", partition_index),
        }
    }

    fn record_range_delta(&self, delta: StorageCacheMetricsSnapshot) {
        self.range_hit_bytes.add(delta.hit_bytes);
        self.range_miss_bytes.add(delta.miss_bytes);
        self.range_hit_count.add(delta.hit_count);
        self.range_miss_count.add(delta.miss_count);
    }

    fn record_footer_observation(&self, delta: StorageCacheMetricsSnapshot) {
        if delta.hit_count == 0 && delta.miss_count == 0 {
            self.footer_hit_count.add(1);
        } else {
            self.footer_miss_count.add(1);
        }
    }
}

struct StorageCacheObservedReader {
    inner: Box<dyn AsyncFileReader + Send>,
    cache_metrics: Arc<StorageCacheMetrics>,
    counters: StorageCacheMetricCounters,
}

fn observe_storage_cache_activity<'a, T>(
    cache_metrics: Arc<StorageCacheMetrics>,
    counters: StorageCacheMetricCounters,
    before: StorageCacheMetricsSnapshot,
    future: BoxFuture<'a, parquet::errors::Result<T>>,
) -> BoxFuture<'a, parquet::errors::Result<T>>
where
    T: Send + 'a,
{
    async move {
        let result = with_storage_cache_metrics(Arc::clone(&cache_metrics), future).await;
        let delta = cache_metrics.snapshot().saturating_delta_since(before);
        counters.record_range_delta(delta);
        result
    }
    .boxed()
}

fn observe_footer_cache_activity<'a>(
    cache_metrics: Arc<StorageCacheMetrics>,
    counters: StorageCacheMetricCounters,
    before: StorageCacheMetricsSnapshot,
    future: BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>>,
) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
    async move {
        let result = with_storage_cache_metrics(Arc::clone(&cache_metrics), future).await;
        let delta = cache_metrics.snapshot().saturating_delta_since(before);
        counters.record_range_delta(delta);
        if result.is_ok() {
            counters.record_footer_observation(delta);
        }
        result
    }
    .boxed()
}

impl AsyncFileReader for StorageCacheObservedReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let before = self.cache_metrics.snapshot();
        let cache_metrics = Arc::clone(&self.cache_metrics);
        let counters = self.counters.clone();
        let future = self.inner.get_bytes(range);
        observe_storage_cache_activity(cache_metrics, counters, before, future)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>>
    where
        Self: Send,
    {
        let before = self.cache_metrics.snapshot();
        let cache_metrics = Arc::clone(&self.cache_metrics);
        let counters = self.counters.clone();
        let future = self.inner.get_byte_ranges(ranges);
        observe_storage_cache_activity(cache_metrics, counters, before, future)
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let before = self.cache_metrics.snapshot();
        let cache_metrics = Arc::clone(&self.cache_metrics);
        let counters = self.counters.clone();
        let future = self.inner.get_metadata(options);
        observe_footer_cache_activity(cache_metrics, counters, before, future)
    }
}
