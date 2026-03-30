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

use arrow::datatypes::{DataType, Schema as ArrowSchema};
use parquet::basic::Compression as ParquetCompression;
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;

use crate::schema::SORT_ORDER;

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

    /// Convert to Parquet WriterProperties using the given Arrow schema to configure
    /// per-column settings like dictionary encoding and bloom filters.
    pub fn to_writer_properties(&self, schema: &ArrowSchema) -> WriterProperties {
        let mut builder = WriterProperties::builder()
            .set_max_row_group_size(self.row_group_size)
            .set_data_page_size_limit(self.data_page_size)
            .set_write_batch_size(self.write_batch_size)
            // Enable column index for efficient pruning on sorted data (64 bytes default)
            .set_column_index_truncate_length(Some(64))
            // Set sorting columns metadata for readers to use during pruning
            .set_sorting_columns(Some(Self::sorting_columns(schema)))
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

        // Apply dictionary encoding and bloom filters based on schema column types
        builder = Self::configure_columns(builder, schema);

        builder.build()
    }

    /// Configure dictionary encoding and bloom filters based on the Arrow schema.
    ///
    /// - Dictionary encoding is enabled on all Dictionary(Int32, Utf8) columns.
    /// - Bloom filters are enabled on metric_name and sort order tag columns.
    fn configure_columns(
        mut builder: WriterPropertiesBuilder,
        schema: &ArrowSchema,
    ) -> WriterPropertiesBuilder {
        for field in schema.fields() {
            let col_path = ColumnPath::new(vec![field.name().to_string()]);

            // Enable dictionary encoding on all Dictionary(_, _) columns
            if matches!(field.data_type(), DataType::Dictionary(_, _)) {
                builder = builder.set_column_dictionary_enabled(col_path.clone(), true);
            }

            // Enable bloom filters on dictionary-typed metric_name and sort order tag columns.
            // Exclude non-dictionary columns, like timestamp_secs.
            let is_bloom_column = matches!(field.data_type(), DataType::Dictionary(_, _))
                && (field.name() == "metric_name" || SORT_ORDER.contains(&field.name().as_str()));
            if is_bloom_column {
                let ndv = if field.name() == "metric_name" {
                    BLOOM_FILTER_NDV_METRIC_NAME
                } else {
                    BLOOM_FILTER_NDV_TAGS
                };
                builder = builder
                    .set_column_bloom_filter_enabled(col_path.clone(), true)
                    .set_column_bloom_filter_fpp(col_path.clone(), BLOOM_FILTER_FPP)
                    .set_column_bloom_filter_ndv(col_path, ndv);
            }
        }
        builder
    }

    /// Get the sorting columns for parquet metadata, computed from the schema
    /// and SORT_ORDER. Only columns present in the schema are included.
    fn sorting_columns(schema: &ArrowSchema) -> Vec<SortingColumn> {
        SORT_ORDER
            .iter()
            .filter_map(|name| schema.index_of(name).ok())
            .map(|idx| SortingColumn {
                column_idx: idx as i32,
                descending: false,
                nulls_first: false,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::Field;

    use super::*;

    /// Create a test schema with required fields + some tag columns.
    fn create_test_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new(
                "metric_name",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("metric_type", DataType::UInt8, false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
            Field::new(
                "service",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new(
                "env",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new(
                "host",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
        ])
    }

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
        let schema = create_test_schema();
        let props = config.to_writer_properties(&schema);
        assert!(props.max_row_group_size() == 128 * 1024);
    }

    #[test]
    fn test_to_writer_properties_snappy() {
        let config = ParquetWriterConfig::new().with_compression(Compression::Snappy);
        let schema = create_test_schema();
        let props = config.to_writer_properties(&schema);
        assert!(props.max_row_group_size() == 128 * 1024);
    }

    #[test]
    fn test_to_writer_properties_uncompressed() {
        let config = ParquetWriterConfig::new().with_compression(Compression::Uncompressed);
        let schema = create_test_schema();
        let props = config.to_writer_properties(&schema);
        assert!(props.max_row_group_size() == 128 * 1024);
    }

    #[test]
    fn test_bloom_filter_configuration() {
        let config = ParquetWriterConfig::default();
        let schema = create_test_schema();
        let props = config.to_writer_properties(&schema);

        // Verify bloom filter is enabled on metric_name column
        let metric_name_path = ColumnPath::new(vec!["metric_name".to_string()]);
        let bloom_props = props.bloom_filter_properties(&metric_name_path);
        assert!(
            bloom_props.is_some(),
            "bloom filter should be enabled for metric_name"
        );

        let bloom_props = bloom_props.unwrap();
        assert!(
            (bloom_props.fpp - BLOOM_FILTER_FPP).abs() < 0.001,
            "bloom filter FPP should be {}",
            BLOOM_FILTER_FPP
        );
        assert_eq!(
            bloom_props.ndv, BLOOM_FILTER_NDV_METRIC_NAME,
            "bloom filter NDV for metric_name should be {}",
            BLOOM_FILTER_NDV_METRIC_NAME
        );

        // Verify bloom filter is enabled on service tag column (in SORT_ORDER)
        let service_path = ColumnPath::new(vec!["service".to_string()]);
        let bloom_props = props.bloom_filter_properties(&service_path);
        assert!(
            bloom_props.is_some(),
            "bloom filter should be enabled for service"
        );

        let bloom_props = bloom_props.unwrap();
        assert_eq!(
            bloom_props.ndv, BLOOM_FILTER_NDV_TAGS,
            "bloom filter NDV for tag columns should be {}",
            BLOOM_FILTER_NDV_TAGS
        );

        // Verify bloom filter is NOT enabled on value column
        let value_path = ColumnPath::new(vec!["value".to_string()]);
        let bloom_props = props.bloom_filter_properties(&value_path);
        assert!(
            bloom_props.is_none(),
            "bloom filter should NOT be enabled for value"
        );
    }

    #[test]
    fn test_statistics_enabled() {
        let config = ParquetWriterConfig::default();
        let schema = create_test_schema();
        let props = config.to_writer_properties(&schema);

        // Verify statistics are enabled at Chunk (row group) level
        let metric_name_path = ColumnPath::new(vec!["metric_name".to_string()]);
        assert_eq!(
            props.statistics_enabled(&metric_name_path),
            EnabledStatistics::Chunk,
            "statistics should be enabled at Chunk level"
        );

        // Verify for timestamp column as well (important for time range pruning)
        let timestamp_path = ColumnPath::new(vec!["timestamp_secs".to_string()]);
        assert_eq!(
            props.statistics_enabled(&timestamp_path),
            EnabledStatistics::Chunk,
            "statistics should be enabled at Chunk level for timestamp"
        );
    }

    #[test]
    fn test_dictionary_encoding_enabled() {
        let config = ParquetWriterConfig::default();
        let schema = create_test_schema();
        let props = config.to_writer_properties(&schema);

        // Verify dictionary encoding is enabled for dictionary-typed columns
        let dictionary_columns = ["metric_name", "service", "env", "host"];

        for col_name in dictionary_columns {
            let col_path = ColumnPath::new(vec![col_name.to_string()]);
            assert!(
                props.dictionary_enabled(&col_path),
                "dictionary encoding should be enabled for {}",
                col_name
            );
        }
    }

    #[test]
    fn test_sorting_columns_order() {
        let schema = create_test_schema();
        let sorting_cols = ParquetWriterConfig::sorting_columns(&schema);

        // The test schema has metric_name (idx 0), timestamp_secs (idx 2),
        // service (idx 4), env (idx 5), host (idx 6).
        // SORT_ORDER is: metric_name, service, env, datacenter, region, host, timestamp_secs
        // Only present columns are included, so: metric_name, service, env, host, timestamp_secs
        assert_eq!(
            sorting_cols.len(),
            5,
            "should have 5 sorting columns from the test schema"
        );

        // Verify all are ascending with nulls first
        for col in &sorting_cols {
            assert!(!col.descending, "sorting should be ascending");
            assert!(!col.nulls_first, "nulls should be last");
        }

        // Verify order matches SORT_ORDER filtered by schema presence:
        // metric_name (idx 0), service (idx 4), env (idx 5), host (idx 6), timestamp_secs (idx 2)
        assert_eq!(sorting_cols[0].column_idx, 0); // metric_name
        assert_eq!(sorting_cols[1].column_idx, 4); // service
        assert_eq!(sorting_cols[2].column_idx, 5); // env
        assert_eq!(sorting_cols[3].column_idx, 6); // host
        assert_eq!(sorting_cols[4].column_idx, 2); // timestamp_secs
    }
}
