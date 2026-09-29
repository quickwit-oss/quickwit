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

//! Report of a Parquet bulk load.

use std::time::Duration;

use anyhow::bail;
use bytesize::ByteSize;
use colored::Colorize;
use humantime::format_duration;
use quickwit_common::uri::Uri;
use quickwit_config::IndexConfig;
use quickwit_indexing::models::IndexingStatistics;
use quickwit_indexing::source::parquet_file::ParquetLoadPlan;
use quickwit_metastore::SplitMetadata;
use thousands::Separable;

pub(super) struct LoadReport<'a> {
    pub num_pipelines: usize,
    pub plan: &'a ParquetLoadPlan,
    pub indexing_statistics: IndexingStatistics,
    pub elapsed: Duration,
    pub splits: Vec<SplitMetadata>,
    pub index_config: &'a IndexConfig,
    pub metastore_uri: &'a Uri,
}

impl LoadReport<'_> {
    pub fn print(&self) {
        let statistics = &self.indexing_statistics;
        let num_docs = statistics.num_docs;
        let ndjson_num_bytes = statistics.total_bytes_processed;
        let splits_num_bytes: u64 = self
            .splits
            .iter()
            .map(|split| split.footer_offsets.end)
            .sum();
        let indexing_settings = &self.index_config.indexing_settings;
        let metastore_type = if self.metastore_uri.protocol().is_database() {
            "PostgreSQL"
        } else {
            "file-backed"
        };
        println!();
        println!("{}", "Parquet bulk load report".bold());
        println!(
            "  Input:        {} rows, {} uncompressed Parquet, {} NDJSON",
            self.plan.num_rows().separate_with_commas(),
            ByteSize(self.plan.num_uncompressed_bytes()),
            ByteSize(ndjson_num_bytes),
        );
        println!(
            "  Docs:         {} processed, {} invalid",
            num_docs.separate_with_commas(),
            statistics.num_invalid_docs.separate_with_commas()
        );
        println!(
            "  Elapsed:      {}, {}",
            format_elapsed(self.elapsed),
            format_throughput(num_docs, ndjson_num_bytes, self.elapsed)
        );
        println!(
            "  Splits:       {} ({})",
            self.splits.len(),
            ByteSize(splits_num_bytes),
        );
        println!(
            "  Settings:     num_pipelines={}, batch_num_rows={}, heap_size={}, \
             split_num_docs_target={}, commit_timeout_secs={}, metastore={metastore_type}",
            self.num_pipelines,
            self.plan.batch_num_rows(),
            indexing_settings.resources.heap_size,
            indexing_settings.split_num_docs_target,
            indexing_settings.commit_timeout_secs,
        );
        println!("  Merging:      disabled for Parquet bulk loads");
    }

    /// Fails if the load is not a valid benchmark run.
    pub fn check(&self) -> anyhow::Result<()> {
        let statistics = &self.indexing_statistics;
        if statistics.num_invalid_docs > 0 {
            bail!(
                "failed to index {} document(s)",
                statistics.num_invalid_docs.separate_with_commas()
            );
        }
        let num_split_docs: usize = self.splits.iter().map(|split| split.num_docs).sum();
        if statistics.num_docs != self.plan.num_rows()
            || num_split_docs as u64 != self.plan.num_rows()
        {
            bail!(
                "indexed {} documents ({} in the splits) but the Parquet file has {} rows",
                statistics.num_docs.separate_with_commas(),
                num_split_docs.separate_with_commas(),
                self.plan.num_rows().separate_with_commas()
            );
        }
        Ok(())
    }
}

fn format_elapsed(elapsed: Duration) -> String {
    format_duration(Duration::from_millis(elapsed.as_millis() as u64)).to_string()
}

fn format_throughput(num_docs: u64, num_bytes: u64, elapsed: Duration) -> String {
    let secs = elapsed.as_secs_f64().max(f64::EPSILON);
    format!(
        "{:.0} docs/s, {:.2} MB/s",
        num_docs as f64 / secs,
        num_bytes as f64 / 1_000_000.0 / secs
    )
}
