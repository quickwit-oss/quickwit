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

//! Parquet index configuration.

use std::sync::OnceLock;

use crate::storage::ParquetWriterConfig;
use crate::table_config::TableConfig;

/// Default maximum rows to accumulate before flushing to split.
const DEFAULT_MAX_ROWS: usize = 1_000_000;

/// Default maximum bytes to accumulate before flushing (128MB).
const DEFAULT_MAX_BYTES: usize = 128 * 1024 * 1024;

/// Get max_rows from environment variable or use default.
fn get_max_rows_from_env() -> usize {
    static MAX_ROWS: OnceLock<usize> = OnceLock::new();
    *MAX_ROWS.get_or_init(|| {
        std::env::var("QW_METRICS_MAX_ROWS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_MAX_ROWS)
    })
}

/// Get max_bytes from environment variable or use default.
fn get_max_bytes_from_env() -> usize {
    static MAX_BYTES: OnceLock<usize> = OnceLock::new();
    *MAX_BYTES.get_or_init(|| {
        std::env::var("QW_METRICS_MAX_BYTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_MAX_BYTES)
    })
}

/// Configuration for parquet indexing.
///
/// Controls when batches are flushed to create new splits based on
/// row count and byte size thresholds.
#[derive(Debug, Clone)]
pub struct ParquetIndexingConfig {
    /// Maximum rows to accumulate before flushing to split.
    pub max_rows: usize,
    /// Maximum bytes to accumulate before flushing (approximate).
    pub max_bytes: usize,
    /// Parquet writer configuration for split creation.
    pub writer_config: ParquetWriterConfig,
    /// Table-level configuration (sort fields, window duration, product type).
    pub table_config: TableConfig,
}

impl Default for ParquetIndexingConfig {
    fn default() -> Self {
        Self {
            max_rows: get_max_rows_from_env(),
            max_bytes: get_max_bytes_from_env(),
            writer_config: ParquetWriterConfig::default(),
            table_config: TableConfig::default(),
        }
    }
}

impl ParquetIndexingConfig {
    /// Set the maximum rows to accumulate before flushing.
    pub fn with_max_rows(mut self, rows: usize) -> Self {
        self.max_rows = rows;
        self
    }

    /// Set the maximum bytes to accumulate before flushing.
    pub fn with_max_bytes(mut self, bytes: usize) -> Self {
        self.max_bytes = bytes;
        self
    }

    /// Set the Parquet writer configuration.
    pub fn with_writer_config(mut self, config: ParquetWriterConfig) -> Self {
        self.writer_config = config;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Compression;

    #[test]
    fn test_default_config() {
        let config = ParquetIndexingConfig::default();
        assert_eq!(config.max_rows, 1_000_000);
        assert_eq!(config.max_bytes, 128 * 1024 * 1024);
    }

    #[test]
    fn test_builder_pattern() {
        let config = ParquetIndexingConfig::default()
            .with_max_rows(500_000)
            .with_max_bytes(64 * 1024 * 1024);

        assert_eq!(config.max_rows, 500_000);
        assert_eq!(config.max_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn test_with_writer_config() {
        let writer_config = ParquetWriterConfig::new().with_compression(Compression::Snappy);

        let config = ParquetIndexingConfig::default().with_writer_config(writer_config);

        assert_eq!(config.writer_config.compression, Compression::Snappy);
    }
}
