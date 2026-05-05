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
use parquet::file::metadata::{KeyValue, SortingColumn};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;

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
    /// Maximum rows per data page (`0` = unbounded — let `data_page_size`
    /// drive the rollover). Useful for tests that need to force the
    /// metric_name column into multiple pages even when it compresses
    /// to a handful of bytes.
    pub data_page_row_count_limit: usize,
    /// Number of rows per write batch.
    pub write_batch_size: usize,
    /// Whether to emit page-level statistics (Parquet Column Index +
    /// Offset Index) in the footer. Default `true`.
    ///
    /// When `false`, only chunk-level (row-group) statistics are emitted,
    /// which gives one min/max per (RG, column) and no per-page metadata.
    /// This loses page-level pruning in queries — fine for multi-RG files
    /// where RG-level pruning is already useful, but **strongly
    /// discouraged for single-RG files**, where it collapses pruning to
    /// one min/max per file. Reduces footer size by tens of KB on small
    /// files (a few hundred KB on very wide schemas).
    pub page_statistics_enabled: bool,
}

impl Default for ParquetWriterConfig {
    fn default() -> Self {
        Self {
            compression: Compression::default(),
            compression_level: Some(DEFAULT_ZSTD_LEVEL),
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            data_page_size: DEFAULT_DATA_PAGE_SIZE,
            data_page_row_count_limit: 0,
            write_batch_size: DEFAULT_WRITE_BATCH_SIZE,
            page_statistics_enabled: true,
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

    /// Set the per-page row count limit. `0` means unbounded (rely on
    /// `data_page_size` for rollover). See
    /// [`ParquetWriterConfig::data_page_row_count_limit`].
    pub fn with_data_page_row_count_limit(mut self, limit: usize) -> Self {
        self.data_page_row_count_limit = limit;
        self
    }

    /// Set the write batch size.
    pub fn with_write_batch_size(mut self, size: usize) -> Self {
        self.write_batch_size = size;
        self
    }

    /// Enable or disable page-level statistics (column index + offset
    /// index). Default `true`. See
    /// [`ParquetWriterConfig::page_statistics_enabled`] for the trade-off.
    pub fn with_page_statistics(mut self, enabled: bool) -> Self {
        self.page_statistics_enabled = enabled;
        self
    }

    /// Convert to Parquet WriterProperties using the given Arrow schema to configure
    /// per-column settings like dictionary encoding and bloom filters, with an empty
    /// sort order and no metadata.
    ///
    /// Prefer `to_writer_properties_with_metadata()` in production — this method
    /// is mainly for tests that don't care about sort order.
    pub fn to_writer_properties(&self, schema: &ArrowSchema) -> WriterProperties {
        self.to_writer_properties_with_metadata(schema, Vec::new(), None, &[])
    }

    /// Convert to Parquet WriterProperties with sorting columns and optional key_value_metadata.
    ///
    /// `sorting_cols` is produced by `ParquetWriter::sorting_columns()` from the
    /// resolved table_config sort fields.
    ///
    /// When `kv_metadata` is provided, the entries are embedded in the Parquet file's
    /// key_value_metadata, making files self-describing (META-07).
    pub fn to_writer_properties_with_metadata(
        &self,
        schema: &ArrowSchema,
        sorting_cols: Vec<SortingColumn>,
        kv_metadata: Option<Vec<KeyValue>>,
        sort_field_names: &[String],
    ) -> WriterProperties {
        // Page-level statistics let queries prune individual data pages via
        // Parquet's Column Index + Offset Index in the footer. This is the
        // prerequisite for the streaming column-major merge engine, where
        // outputs may be a single large row group: without per-page stats,
        // pruning would collapse to one min/max per file. Truncation length
        // (64 bytes) bounds the per-page metadata footprint for high-
        // cardinality string columns.
        //
        // The choice is controlled by `page_statistics_enabled`. Disabling
        // it falls back to `Chunk`-level stats only — saves a few percent
        // of footer space at the cost of page-level pruning.
        let stats_level = if self.page_statistics_enabled {
            EnabledStatistics::Page
        } else {
            EnabledStatistics::Chunk
        };
        let mut builder = WriterProperties::builder()
            .set_max_row_group_row_count(Some(self.row_group_size))
            .set_data_page_size_limit(self.data_page_size)
            .set_write_batch_size(self.write_batch_size)
            .set_column_index_truncate_length(Some(64))
            .set_sorting_columns(Some(sorting_cols))
            .set_statistics_enabled(stats_level);

        if self.data_page_row_count_limit > 0 {
            builder = builder.set_data_page_row_count_limit(self.data_page_row_count_limit);
        }

        if let Some(kvs) = kv_metadata
            && !kvs.is_empty()
        {
            builder = builder.set_key_value_metadata(Some(kvs));
        }

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
        builder = Self::configure_columns(builder, schema, sort_field_names);

        builder.build()
    }

    /// Configure dictionary encoding and bloom filters based on the Arrow schema.
    ///
    /// - Dictionary encoding is enabled on all Dictionary(Int32, Utf8) columns.
    /// - Bloom filters are enabled on metric_name and sort order tag columns.
    fn configure_columns(
        mut builder: WriterPropertiesBuilder,
        schema: &ArrowSchema,
        sort_field_names: &[String],
    ) -> WriterPropertiesBuilder {
        for field in schema.fields() {
            let col_path = ColumnPath::new(vec![field.name().to_string()]);

            // Enable dictionary encoding on all Dictionary(_, _) columns
            if matches!(field.data_type(), DataType::Dictionary(_, _)) {
                builder = builder.set_column_dictionary_enabled(col_path.clone(), true);
            }

            // Enable bloom filters on metric_name and sort order tag columns.
            // Only dictionary-typed columns are eligible (excludes timestamp_secs etc.).
            let is_bloom_column = matches!(field.data_type(), DataType::Dictionary(_, _))
                && (field.name() == "metric_name"
                    || sort_field_names.iter().any(|s| s == field.name()));
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
}

#[cfg(test)]
mod tests {
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
        assert!(props.max_row_group_row_count() == Some(128 * 1024));
    }

    #[test]
    fn test_to_writer_properties_snappy() {
        let config = ParquetWriterConfig::new().with_compression(Compression::Snappy);
        let schema = create_test_schema();
        let props = config.to_writer_properties(&schema);
        assert!(props.max_row_group_row_count() == Some(128 * 1024));
    }

    #[test]
    fn test_to_writer_properties_uncompressed() {
        let config = ParquetWriterConfig::new().with_compression(Compression::Uncompressed);
        let schema = create_test_schema();
        let props = config.to_writer_properties(&schema);
        assert!(props.max_row_group_row_count() == Some(128 * 1024));
    }

    #[test]
    fn test_bloom_filter_configuration() {
        let config = ParquetWriterConfig::default();
        let schema = create_test_schema();
        let sort_fields = vec!["service".to_string()];
        let props =
            config.to_writer_properties_with_metadata(&schema, Vec::new(), None, &sort_fields);

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

        // Page-level stats are required for column index / offset index to
        // land in the footer; downstream pruning relies on that data.
        let metric_name_path = ColumnPath::new(vec!["metric_name".to_string()]);
        assert_eq!(
            props.statistics_enabled(&metric_name_path),
            EnabledStatistics::Page,
            "statistics should be enabled at Page level"
        );

        let timestamp_path = ColumnPath::new(vec!["timestamp_secs".to_string()]);
        assert_eq!(
            props.statistics_enabled(&timestamp_path),
            EnabledStatistics::Page,
            "statistics should be enabled at Page level for timestamp"
        );
    }

    #[test]
    fn test_page_statistics_default_enabled() {
        let config = ParquetWriterConfig::default();
        assert!(config.page_statistics_enabled);
    }

    #[test]
    fn test_page_statistics_disable_falls_back_to_chunk() {
        let config = ParquetWriterConfig::new().with_page_statistics(false);
        assert!(!config.page_statistics_enabled);

        let schema = create_test_schema();
        let props = config.to_writer_properties(&schema);
        let metric_name_path = ColumnPath::new(vec!["metric_name".to_string()]);
        assert_eq!(
            props.statistics_enabled(&metric_name_path),
            EnabledStatistics::Chunk,
            "disabling page stats should fall back to chunk-level stats"
        );
    }

    #[test]
    fn test_page_statistics_disabled_writer_omits_indexes() {
        // With the knob off, written files have no Column Index / Offset
        // Index in the footer. The inspector should report
        // `has_column_index = false` for every column.
        use std::sync::Arc;

        use arrow::array::{
            DictionaryArray, Float64Array, Int64Array, RecordBatch, UInt8Array, UInt64Array,
        };
        use arrow::datatypes::{Field, Int32Type};
        use parquet::arrow::ArrowWriter;
        use tempfile::TempDir;

        use super::*;
        use crate::storage::inspect_parquet_page_stats;

        let dir = TempDir::new().unwrap();
        let metric_name_array: DictionaryArray<Int32Type> = (0..16)
            .map(|i| Some(if i < 8 { "cpu" } else { "mem" }))
            .collect();
        let timestamp_array = UInt64Array::from((0..16u64).collect::<Vec<_>>());
        let value_array = Float64Array::from((0..16).map(|i| i as f64).collect::<Vec<_>>());
        let tsid_array = Int64Array::from(vec![42i64; 16]);
        let metric_type_array = UInt8Array::from(vec![0u8; 16]);

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "metric_name",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("metric_type", DataType::UInt8, false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("timeseries_id", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(metric_name_array),
                Arc::new(metric_type_array),
                Arc::new(timestamp_array),
                Arc::new(value_array),
                Arc::new(tsid_array),
            ],
        )
        .unwrap();

        let path = dir.path().join("no_page_index.parquet");
        let config = ParquetWriterConfig::new().with_page_statistics(false);
        let props = config.to_writer_properties(&schema);
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let report = inspect_parquet_page_stats(&path, 100).unwrap();
        for col in &report.row_groups[0].columns {
            assert!(
                !col.has_column_index,
                "column '{}' must NOT have a column index when page stats are disabled",
                col.column_path
            );
        }
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
}
