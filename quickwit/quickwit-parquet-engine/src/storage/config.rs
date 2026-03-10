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

//! Parquet writer configuration for metrics storage.

use parquet::basic::Compression as ParquetCompression;
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;

use crate::schema::ParquetField;

/// Default row group size: 128K rows for efficient columnar scans.
const DEFAULT_ROW_GROUP_SIZE: usize = 128 * 1024;

/// Default data page size: 1MB for good compression/random access balance.
const DEFAULT_DATA_PAGE_SIZE: usize = 1024 * 1024;

/// Default write batch size: 64K rows per batch.
const DEFAULT_WRITE_BATCH_SIZE: usize = 64 * 1024;

/// Default zstd compression level: 3 balances speed and ratio.
const DEFAULT_ZSTD_LEVEL: i32 = 3;

/// Default bloom filter false positive probability (5%).
const BLOOM_FILTER_FPP: f64 = 0.05;

/// Default NDV estimate for tag columns.
const BLOOM_FILTER_NDV_TAGS: u64 = 10_000;

/// Default NDV estimate for metric_name column (higher cardinality).
const BLOOM_FILTER_NDV_METRIC_NAME: u64 = 100_000;

/// Compression algorithm for Parquet files.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Compression {
    /// Zstd compression (default) - best compression ratio.
    #[default]
    Zstd,
    /// Snappy compression - faster but lower ratio.
    Snappy,
    /// No compression.
    Uncompressed,
}

/// Configuration for Parquet writer.
#[derive(Debug, Clone)]
pub struct ParquetWriterConfig {
    /// Compression algorithm to use.
    pub compression: Compression,
    /// Compression level for zstd (1-22). Ignored for other algorithms.
    pub compression_level: Option<i32>,
    /// Number of rows per row group.
    pub row_group_size: usize,
    /// Target size in bytes for data pages.
    pub data_page_size: usize,
    /// Number of rows per write batch.
    pub write_batch_size: usize,
}

impl Default for ParquetWriterConfig {
    fn default() -> Self {
        Self {
            compression: Compression::default(),
            compression_level: Some(DEFAULT_ZSTD_LEVEL),
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            data_page_size: DEFAULT_DATA_PAGE_SIZE,
            write_batch_size: DEFAULT_WRITE_BATCH_SIZE,
        }
    }
}

impl ParquetWriterConfig {
    /// Create a new ParquetWriterConfig with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the compression algorithm.
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Set the compression level for zstd.
    pub fn with_compression_level(mut self, level: i32) -> Self {
        self.compression_level = Some(level);
        self
    }

    /// Set the row group size.
    pub fn with_row_group_size(mut self, size: usize) -> Self {
        self.row_group_size = size;
        self
    }

    /// Set the data page size.
    pub fn with_data_page_size(mut self, size: usize) -> Self {
        self.data_page_size = size;
        self
    }

    /// Set the write batch size.
    pub fn with_write_batch_size(mut self, size: usize) -> Self {
        self.write_batch_size = size;
        self
    }

    /// Convert to Parquet WriterProperties.
    pub fn to_writer_properties(&self) -> WriterProperties {
        let mut builder = WriterProperties::builder()
            .set_max_row_group_size(self.row_group_size)
            .set_data_page_size_limit(self.data_page_size)
            .set_write_batch_size(self.write_batch_size)
            // Enable column index for efficient pruning on sorted data (64 bytes default)
            .set_column_index_truncate_length(Some(64))
            // Set sorting columns metadata for readers to use during pruning
            .set_sorting_columns(Some(Self::sorting_columns()))
            // Enable row group level statistics (min/max/null_count) for query pruning
            // This allows DataFusion to skip row groups based on timestamp ranges
            .set_statistics_enabled(EnabledStatistics::Chunk);

        builder = match self.compression {
            Compression::Zstd => {
                let level = self.compression_level.unwrap_or(DEFAULT_ZSTD_LEVEL);
                builder.set_compression(ParquetCompression::ZSTD(
                    parquet::basic::ZstdLevel::try_new(level).unwrap_or_default(),
                ))
            }
            Compression::Snappy => builder.set_compression(ParquetCompression::SNAPPY),
            Compression::Uncompressed => builder.set_compression(ParquetCompression::UNCOMPRESSED),
        };

        // Apply RLE_DICTIONARY encoding and bloom filters for dictionary columns
        builder = Self::configure_dictionary_columns(builder);

        builder.build()
    }

    /// Configure dictionary encoding and bloom filters for high-cardinality columns.
    ///
    /// Dictionary-encoded columns benefit from:
    /// - Dictionary encoding: Enabled by default, uses RLE for dictionary indices
    /// - Bloom filters: Enable efficient equality filtering without scanning
    ///
    /// Note: Dictionary encoding is ON by default in Parquet. When enabled, the dictionary
    /// indices are automatically encoded using RLE (run-length encoding), which efficiently
    /// compresses runs of repeated values. This is ideal for sorted data where consecutive
    /// rows often share the same dictionary index.
    fn configure_dictionary_columns(
        mut builder: WriterPropertiesBuilder,
    ) -> WriterPropertiesBuilder {
        // Dictionary-encoded columns - ensure dictionary encoding is explicitly enabled
        // (default is true, but being explicit documents intent)
        let dictionary_columns = [
            ParquetField::MetricName,
            ParquetField::TagService,
            ParquetField::TagEnv,
            ParquetField::TagDatacenter,
            ParquetField::TagRegion,
            ParquetField::TagHost,
            ParquetField::ServiceName,
        ];

        // Columns that benefit from bloom filters (used in WHERE clauses)
        // Note: We enable bloom filters on filtering columns, not timestamp_secs or value
        let bloom_filter_columns = [
            (ParquetField::MetricName, BLOOM_FILTER_NDV_METRIC_NAME),
            (ParquetField::TagService, BLOOM_FILTER_NDV_TAGS),
            (ParquetField::TagEnv, BLOOM_FILTER_NDV_TAGS),
            (ParquetField::TagDatacenter, BLOOM_FILTER_NDV_TAGS),
            (ParquetField::TagHost, BLOOM_FILTER_NDV_TAGS),
            (ParquetField::ServiceName, BLOOM_FILTER_NDV_TAGS),
        ];

        // Ensure dictionary encoding is enabled on dictionary columns
        // (dictionary encoding uses RLE for indices automatically)
        for field in dictionary_columns {
            let col_path = ColumnPath::new(vec![field.name().to_string()]);
            builder = builder.set_column_dictionary_enabled(col_path, true);
        }

        // Enable bloom filters on filtering columns
        for (field, ndv) in bloom_filter_columns {
            let col_path = ColumnPath::new(vec![field.name().to_string()]);
            builder = builder
                .set_column_bloom_filter_enabled(col_path.clone(), true)
                .set_column_bloom_filter_fpp(col_path.clone(), BLOOM_FILTER_FPP)
                .set_column_bloom_filter_ndv(col_path, ndv);
        }

        builder
    }

    /// Get the sorting columns for parquet metadata.
    /// Order: metric_name, tag_service, tag_env, tag_datacenter, tag_region, tag_host,
    /// timestamp_secs.
    fn sorting_columns() -> Vec<SortingColumn> {
        ParquetField::sort_order()
            .iter()
            .map(|field| SortingColumn {
                column_idx: field.column_index() as i32,
                descending: false,
                nulls_first: true,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ParquetWriterConfig::default();
        assert_eq!(config.compression, Compression::Zstd);
        assert_eq!(config.compression_level, Some(3));
        assert_eq!(config.row_group_size, 128 * 1024);
        assert_eq!(config.data_page_size, 1024 * 1024);
        assert_eq!(config.write_batch_size, 64 * 1024);
    }

    #[test]
    fn test_builder_pattern() {
        let config = ParquetWriterConfig::new()
            .with_compression(Compression::Snappy)
            .with_row_group_size(256 * 1024);

        assert_eq!(config.compression, Compression::Snappy);
        assert_eq!(config.row_group_size, 256 * 1024);
    }

    #[test]
    fn test_to_writer_properties_zstd() {
        let config = ParquetWriterConfig::default();
        let props = config.to_writer_properties();
        // WriterProperties doesn't expose compression directly, but we can verify it builds
        assert!(props.max_row_group_size() == 128 * 1024);
    }

    #[test]
    fn test_to_writer_properties_snappy() {
        let config = ParquetWriterConfig::new().with_compression(Compression::Snappy);
        let props = config.to_writer_properties();
        assert!(props.max_row_group_size() == 128 * 1024);
    }

    #[test]
    fn test_to_writer_properties_uncompressed() {
        let config = ParquetWriterConfig::new().with_compression(Compression::Uncompressed);
        let props = config.to_writer_properties();
        assert!(props.max_row_group_size() == 128 * 1024);
    }

    #[test]
    fn test_bloom_filter_configuration() {
        let config = ParquetWriterConfig::default();
        let props = config.to_writer_properties();

        // Verify bloom filter is enabled on metric_name column
        let metric_name_path = ColumnPath::new(vec!["metric_name".to_string()]);
        let bloom_props = props.bloom_filter_properties(&metric_name_path);
        assert!(
            bloom_props.is_some(),
            "Bloom filter should be enabled for metric_name"
        );

        let bloom_props = bloom_props.unwrap();
        assert!(
            (bloom_props.fpp - BLOOM_FILTER_FPP).abs() < 0.001,
            "Bloom filter FPP should be {}",
            BLOOM_FILTER_FPP
        );
        assert_eq!(
            bloom_props.ndv, BLOOM_FILTER_NDV_METRIC_NAME,
            "Bloom filter NDV for metric_name should be {}",
            BLOOM_FILTER_NDV_METRIC_NAME
        );

        // Verify bloom filter is enabled on tag columns
        let tag_service_path = ColumnPath::new(vec!["tag_service".to_string()]);
        let bloom_props = props.bloom_filter_properties(&tag_service_path);
        assert!(
            bloom_props.is_some(),
            "Bloom filter should be enabled for tag_service"
        );

        let bloom_props = bloom_props.unwrap();
        assert_eq!(
            bloom_props.ndv, BLOOM_FILTER_NDV_TAGS,
            "Bloom filter NDV for tag columns should be {}",
            BLOOM_FILTER_NDV_TAGS
        );

        // Verify bloom filter is NOT enabled on timestamp_secs (not a filtering column)
        let timestamp_path = ColumnPath::new(vec!["timestamp_secs".to_string()]);
        let bloom_props = props.bloom_filter_properties(&timestamp_path);
        assert!(
            bloom_props.is_none(),
            "Bloom filter should NOT be enabled for timestamp_secs"
        );

        // Verify bloom filter is NOT enabled on value column
        let value_path = ColumnPath::new(vec!["value".to_string()]);
        let bloom_props = props.bloom_filter_properties(&value_path);
        assert!(
            bloom_props.is_none(),
            "Bloom filter should NOT be enabled for value"
        );
    }

    #[test]
    fn test_statistics_enabled() {
        let config = ParquetWriterConfig::default();
        let props = config.to_writer_properties();

        // Verify statistics are enabled at Chunk (row group) level
        let metric_name_path = ColumnPath::new(vec!["metric_name".to_string()]);
        assert_eq!(
            props.statistics_enabled(&metric_name_path),
            EnabledStatistics::Chunk,
            "Statistics should be enabled at Chunk level"
        );

        // Verify for timestamp column as well (important for time range pruning)
        let timestamp_path = ColumnPath::new(vec!["timestamp_secs".to_string()]);
        assert_eq!(
            props.statistics_enabled(&timestamp_path),
            EnabledStatistics::Chunk,
            "Statistics should be enabled at Chunk level for timestamp"
        );
    }

    #[test]
    fn test_dictionary_encoding_enabled() {
        let config = ParquetWriterConfig::default();
        let props = config.to_writer_properties();

        // Verify dictionary encoding is enabled for dictionary columns
        let dictionary_columns = [
            "metric_name",
            "tag_service",
            "tag_env",
            "tag_datacenter",
            "tag_region",
            "tag_host",
            "service_name",
        ];

        for col_name in dictionary_columns {
            let col_path = ColumnPath::new(vec![col_name.to_string()]);
            assert!(
                props.dictionary_enabled(&col_path),
                "Dictionary encoding should be enabled for {}",
                col_name
            );
        }
    }

    #[test]
    fn test_sorting_columns_order() {
        let sorting_cols = ParquetWriterConfig::sorting_columns();

        // Verify we have the expected number of sorting columns
        assert_eq!(
            sorting_cols.len(),
            7,
            "Should have 7 sorting columns: metric_name, 5 tags, timestamp"
        );

        // Verify sort order matches expected: metric_name, tag_service, tag_env,
        // tag_datacenter, tag_region, tag_host, timestamp_secs
        let expected_order = [
            ParquetField::MetricName,
            ParquetField::TagService,
            ParquetField::TagEnv,
            ParquetField::TagDatacenter,
            ParquetField::TagRegion,
            ParquetField::TagHost,
            ParquetField::TimestampSecs,
        ];

        for (i, expected_field) in expected_order.iter().enumerate() {
            assert_eq!(
                sorting_cols[i].column_idx,
                expected_field.column_index() as i32,
                "Sorting column {} should be {}",
                i,
                expected_field.name()
            );
            assert!(!sorting_cols[i].descending, "Sorting should be ascending");
            assert!(sorting_cols[i].nulls_first, "Nulls should be first");
        }
    }
}
