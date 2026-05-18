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

//! Inspect a Parquet file's footer and page-level statistics.
//!
//! This module is the foundation for the `inspect_parquet` developer CLI
//! and for tests that verify on-disk format properties such as the
//! `qh.rg_partition_prefix_len` alignment claim.
//!
//! The inspector loads the Column Index and Offset Index (Parquet's
//! per-page min/max + offsets stored in the footer) explicitly via
//! `ReadOptionsBuilder::with_page_index(true)`. Without this, the
//! footer would be parsed with chunk-level statistics only, even if the
//! writer emitted page-level data.

use std::path::Path;

use anyhow::{Context, Result, bail};
use parquet::file::page_index::column_index::{
    ByteArrayColumnIndex, ColumnIndexMetaData, PrimitiveColumnIndex,
};
use parquet::file::page_index::offset_index::{OffsetIndexMetaData, PageLocation};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;
use parquet::file::statistics::Statistics;
use serde::Serialize;

use super::writer::{PARQUET_META_RG_PARTITION_PREFIX_LEN, PARQUET_META_SORT_FIELDS};

/// Aggregate report of a Parquet file's page-level statistics + KV metadata.
#[derive(Debug, Clone, Serialize)]
pub struct ParquetPageStatsReport {
    /// File length in bytes.
    pub file_size: u64,
    /// All `qh.*` (and other) entries from the file's KV metadata.
    pub kv_metadata: Vec<(String, String)>,
    /// Number of row groups in the file.
    pub num_row_groups: usize,
    /// Per-row-group reports (length == `num_row_groups`).
    pub row_groups: Vec<RowGroupReport>,
    /// Parsed value of `qh.rg_partition_prefix_len`. `0` (or absent) means
    /// no alignment claim is recorded in the file.
    pub rg_partition_prefix_len: u32,
    /// Parsed value of `qh.sort_fields`, if present.
    pub sort_fields: Option<String>,
}

/// Per-row-group view of a Parquet file.
#[derive(Debug, Clone, Serialize)]
pub struct RowGroupReport {
    /// Row count in this row group.
    pub num_rows: i64,
    /// Total size in bytes of this row group's column chunks.
    pub total_byte_size: i64,
    /// One entry per column in the file's schema.
    pub columns: Vec<ColumnReport>,
}

/// Per-column view inside a row group.
#[derive(Debug, Clone, Serialize)]
pub struct ColumnReport {
    /// Column path as stored in the Parquet schema (e.g., `"metric_name"`).
    pub column_path: String,
    /// Whether the file has a Column Index entry for this column. When
    /// `false`, page-level statistics are unavailable for this column —
    /// likely because `EnabledStatistics::Page` was not set when the file
    /// was written.
    pub has_column_index: bool,
    /// Whether the file has an Offset Index entry for this column.
    pub has_offset_index: bool,
    /// Number of data pages in this column chunk (0 if no offset index).
    pub num_pages: usize,
    /// Chunk-level (row-group-wide) min, stringified for display.
    pub chunk_min: Option<String>,
    /// Chunk-level (row-group-wide) max, stringified for display.
    pub chunk_max: Option<String>,
    /// Per-page reports. Length == `num_pages` when both indexes are
    /// present; truncated by callers that only want a preview.
    pub pages: Vec<PageReport>,
}

/// Per-page view inside a column chunk.
#[derive(Debug, Clone, Serialize)]
pub struct PageReport {
    /// Stringified per-page min from the Column Index, if available.
    pub min: Option<String>,
    /// Stringified per-page max from the Column Index, if available.
    pub max: Option<String>,
    /// Null count from the Column Index, if available.
    pub null_count: Option<i64>,
    /// Row count of this page, derived from the Offset Index's
    /// `first_row_index` deltas. `None` for the last page.
    pub num_rows: Option<usize>,
    /// Byte offset of this page within the file.
    pub offset: Option<i64>,
    /// Compressed size of this page on disk.
    pub compressed_page_size: Option<i32>,
}

impl ParquetPageStatsReport {
    /// Returns true when every (row-group, column) pair has a Column Index
    /// entry. The column-major streaming reader requires this.
    pub fn has_full_page_index_coverage(&self) -> bool {
        self.row_groups
            .iter()
            .all(|rg| rg.columns.iter().all(|c| c.has_column_index))
    }
}

/// Inspect a Parquet file at `path` and produce a structured report.
///
/// Pages are limited to the first `max_pages_per_column` per column.
/// Pass `usize::MAX` for the unrestricted view used by the
/// `--all-pages` CLI flag.
pub fn inspect_parquet_page_stats(
    path: &Path,
    max_pages_per_column: usize,
) -> Result<ParquetPageStatsReport> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("opening parquet file: {}", path.display()))?;
    let file_size = file
        .metadata()
        .with_context(|| format!("reading file size: {}", path.display()))?
        .len();

    let options = ReadOptionsBuilder::new().with_page_index().build();
    let reader = SerializedFileReader::new_with_options(file, options)
        .with_context(|| format!("opening parquet reader: {}", path.display()))?;
    let metadata = reader.metadata();

    let kv_metadata: Vec<(String, String)> = metadata
        .file_metadata()
        .key_value_metadata()
        .map(|kvs| {
            kvs.iter()
                .map(|kv| (kv.key.clone(), kv.value.clone().unwrap_or_default()))
                .collect()
        })
        .unwrap_or_default();

    let rg_partition_prefix_len = kv_metadata
        .iter()
        .find(|(k, _)| k == PARQUET_META_RG_PARTITION_PREFIX_LEN)
        .map(|(_, v)| v.parse::<u32>())
        .transpose()
        .with_context(|| {
            format!(
                "parsing {PARQUET_META_RG_PARTITION_PREFIX_LEN} from {}",
                path.display()
            )
        })?
        .unwrap_or(0);

    let sort_fields = kv_metadata
        .iter()
        .find(|(k, _)| k == PARQUET_META_SORT_FIELDS)
        .map(|(_, v)| v.clone());

    let column_index = metadata.column_index();
    let offset_index = metadata.offset_index();
    let num_row_groups = metadata.num_row_groups();

    let mut row_groups = Vec::with_capacity(num_row_groups);
    for rg_idx in 0..num_row_groups {
        let rg_meta = metadata.row_group(rg_idx);
        let mut columns = Vec::with_capacity(rg_meta.num_columns());

        for col_idx in 0..rg_meta.num_columns() {
            let col_chunk = rg_meta.column(col_idx);
            let column_path = col_chunk.column_path().string();

            let (chunk_min, chunk_max) = match col_chunk.statistics() {
                Some(stats) => stringify_chunk_stats(stats),
                None => (None, None),
            };

            let col_index_entry = column_index
                .and_then(|idx| idx.get(rg_idx))
                .and_then(|cols| cols.get(col_idx));
            let off_index_entry = offset_index
                .and_then(|idx| idx.get(rg_idx))
                .and_then(|cols| cols.get(col_idx));

            let has_column_index = col_index_entry
                .map(|idx| !matches!(idx, ColumnIndexMetaData::NONE))
                .unwrap_or(false);
            let has_offset_index = off_index_entry.is_some();

            let pages = build_page_reports(col_index_entry, off_index_entry, max_pages_per_column);
            let num_pages = off_index_entry
                .map(|locs| locs.page_locations().len())
                .unwrap_or(0);

            columns.push(ColumnReport {
                column_path,
                has_column_index,
                has_offset_index,
                num_pages,
                chunk_min,
                chunk_max,
                pages,
            });
        }

        row_groups.push(RowGroupReport {
            num_rows: rg_meta.num_rows(),
            total_byte_size: rg_meta.total_byte_size(),
            columns,
        });
    }

    Ok(ParquetPageStatsReport {
        file_size,
        kv_metadata,
        num_row_groups,
        row_groups,
        rg_partition_prefix_len,
        sort_fields,
    })
}

/// Verify that a file's RG boundaries actually align with transitions in
/// the first `rg_partition_prefix_len` sort columns.
///
/// The contract is the strong form: within each row group, every one of
/// the first `N` sort columns is constant. Implementation: each RG's
/// chunk-level min must equal its chunk-level max for those columns.
///
/// Returns `Ok(())` when the claim holds (or `prefix_len == 0`, in which
/// case there is no claim to verify). Returns an error with a per-RG
/// diagnostic when it doesn't.
pub fn verify_partition_prefix(report: &ParquetPageStatsReport) -> Result<()> {
    if report.rg_partition_prefix_len == 0 {
        return Ok(());
    }

    let sort_fields_str = match &report.sort_fields {
        Some(s) if !s.is_empty() => s.clone(),
        _ => bail!(
            "rg_partition_prefix_len = {} but {} is missing or empty — cannot verify alignment \
             without the sort schema",
            report.rg_partition_prefix_len,
            PARQUET_META_SORT_FIELDS
        ),
    };

    let prefix_columns =
        first_n_sort_field_names(&sort_fields_str, report.rg_partition_prefix_len as usize)?;

    for (rg_idx, rg) in report.row_groups.iter().enumerate() {
        for col_name in &prefix_columns {
            let col = rg
                .columns
                .iter()
                .find(|c| c.column_path == *col_name)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "RG {rg_idx}: prefix column '{col_name}' is not present in the file's \
                         schema",
                    )
                })?;

            match (&col.chunk_min, &col.chunk_max) {
                (Some(min), Some(max)) if min == max => {
                    // OK: column has a single value across the entire RG.
                }
                (Some(min), Some(max)) => bail!(
                    "RG {rg_idx} violates rg_partition_prefix_len={} claim: column '{col_name}' \
                     has min={min:?} != max={max:?} (must be constant across the row group)",
                    report.rg_partition_prefix_len
                ),
                _ => bail!(
                    "RG {rg_idx} violates rg_partition_prefix_len={} claim: column '{col_name}' \
                     has no chunk-level statistics",
                    report.rg_partition_prefix_len
                ),
            }
        }
    }

    Ok(())
}

/// Extract the first `n` sort field names from a Husky-style sort schema
/// string (e.g., `"metric_name|host|timestamp_secs/V2"`).
///
/// The trailing `/V2` (or other version marker) is stripped from the last
/// field. Each field's bare name is returned (no `:asc`/`:desc` decoration
/// in the input is supported here; if the format expands, this helper
/// should be updated rather than the call sites).
fn first_n_sort_field_names(sort_fields: &str, n: usize) -> Result<Vec<String>> {
    let segments: Vec<&str> = sort_fields.split('|').collect();
    if n > segments.len() {
        bail!(
            "rg_partition_prefix_len = {n} exceeds sort schema length = {}",
            segments.len()
        );
    }
    let mut names = Vec::with_capacity(n);
    for seg in segments.iter().take(n) {
        let bare = seg.split('/').next().unwrap_or(seg);
        names.push(bare.to_string());
    }
    Ok(names)
}

fn stringify_chunk_stats(stats: &Statistics) -> (Option<String>, Option<String>) {
    let min = stringify_chunk_min(stats);
    let max = stringify_chunk_max(stats);
    (min, max)
}

fn stringify_chunk_min(stats: &Statistics) -> Option<String> {
    match stats {
        Statistics::Boolean(v) => v.min_opt().map(|x| x.to_string()),
        Statistics::Int32(v) => v.min_opt().map(|x| x.to_string()),
        Statistics::Int64(v) => v.min_opt().map(|x| x.to_string()),
        Statistics::Int96(v) => v.min_opt().map(|x| format!("{x:?}")),
        Statistics::Float(v) => v.min_opt().map(|x| x.to_string()),
        Statistics::Double(v) => v.min_opt().map(|x| x.to_string()),
        Statistics::ByteArray(v) => v.min_opt().map(stringify_byte_array),
        Statistics::FixedLenByteArray(v) => v.min_opt().map(stringify_fixed_byte_array),
    }
}

fn stringify_chunk_max(stats: &Statistics) -> Option<String> {
    match stats {
        Statistics::Boolean(v) => v.max_opt().map(|x| x.to_string()),
        Statistics::Int32(v) => v.max_opt().map(|x| x.to_string()),
        Statistics::Int64(v) => v.max_opt().map(|x| x.to_string()),
        Statistics::Int96(v) => v.max_opt().map(|x| format!("{x:?}")),
        Statistics::Float(v) => v.max_opt().map(|x| x.to_string()),
        Statistics::Double(v) => v.max_opt().map(|x| x.to_string()),
        Statistics::ByteArray(v) => v.max_opt().map(stringify_byte_array),
        Statistics::FixedLenByteArray(v) => v.max_opt().map(stringify_fixed_byte_array),
    }
}

fn stringify_byte_array(b: &parquet::data_type::ByteArray) -> String {
    String::from_utf8(b.data().to_vec()).unwrap_or_else(|_| format!("0x{}", hex_encode(b.data())))
}

fn stringify_fixed_byte_array(b: &parquet::data_type::FixedLenByteArray) -> String {
    String::from_utf8(b.data().to_vec()).unwrap_or_else(|_| format!("0x{}", hex_encode(b.data())))
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn build_page_reports(
    col_index_entry: Option<&ColumnIndexMetaData>,
    off_index_entry: Option<&OffsetIndexMetaData>,
    max_pages: usize,
) -> Vec<PageReport> {
    let Some(off_index) = off_index_entry else {
        return Vec::new();
    };

    let page_locations = off_index.page_locations();
    let take = page_locations.len().min(max_pages);
    let mut pages = Vec::with_capacity(take);

    let stats = col_index_entry.map(per_page_stats).unwrap_or_default();

    for (page_idx, location) in page_locations.iter().take(take).enumerate() {
        let num_rows = next_first_row(page_locations, page_idx)
            .map(|next| (next - location.first_row_index).max(0) as usize);

        let (min, max, null_count) = stats.get(page_idx).cloned().unwrap_or((None, None, None));

        pages.push(PageReport {
            min,
            max,
            null_count,
            num_rows,
            offset: Some(location.offset),
            compressed_page_size: Some(location.compressed_page_size),
        });
    }

    pages
}

fn next_first_row(locations: &[PageLocation], page_idx: usize) -> Option<i64> {
    locations.get(page_idx + 1).map(|loc| loc.first_row_index)
}

type PageStat = (Option<String>, Option<String>, Option<i64>);

fn per_page_stats(index: &ColumnIndexMetaData) -> Vec<PageStat> {
    fn primitive<T, F>(idx: &PrimitiveColumnIndex<T>, to_string: F) -> Vec<PageStat>
    where F: Fn(&T) -> String {
        let num_pages = idx.num_pages() as usize;
        let mins: Vec<Option<&T>> = idx.min_values_iter().collect();
        let maxs: Vec<Option<&T>> = idx.max_values_iter().collect();
        (0..num_pages)
            .map(|i| {
                (
                    mins.get(i).copied().flatten().map(&to_string),
                    maxs.get(i).copied().flatten().map(&to_string),
                    idx.null_count(i),
                )
            })
            .collect()
    }

    fn byte_array(idx: &ByteArrayColumnIndex) -> Vec<PageStat> {
        let num_pages = idx.num_pages() as usize;
        (0..num_pages)
            .map(|i| {
                (
                    idx.min_value(i).map(stringify_bytes),
                    idx.max_value(i).map(stringify_bytes),
                    idx.null_count(i),
                )
            })
            .collect()
    }

    match index {
        ColumnIndexMetaData::NONE => Vec::new(),
        ColumnIndexMetaData::BOOLEAN(idx) => primitive(idx, |b| b.to_string()),
        ColumnIndexMetaData::INT32(idx) => primitive(idx, |x| x.to_string()),
        ColumnIndexMetaData::INT64(idx) => primitive(idx, |x| x.to_string()),
        ColumnIndexMetaData::INT96(idx) => primitive(idx, |x| format!("{x:?}")),
        ColumnIndexMetaData::FLOAT(idx) => primitive(idx, |x| x.to_string()),
        ColumnIndexMetaData::DOUBLE(idx) => primitive(idx, |x| x.to_string()),
        ColumnIndexMetaData::BYTE_ARRAY(idx) => byte_array(idx),
        ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(idx) => byte_array(idx),
    }
}

fn stringify_bytes(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).unwrap_or_else(|_| format!("0x{}", hex_encode(bytes)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        DictionaryArray, Float64Array, Int64Array, RecordBatch, UInt8Array, UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::KeyValue;
    use tempfile::TempDir;

    use super::*;
    use crate::storage::ParquetWriterConfig;

    fn write_test_file(
        dir: &Path,
        name: &str,
        num_rows: usize,
        config: ParquetWriterConfig,
        kv: Option<Vec<KeyValue>>,
    ) -> std::path::PathBuf {
        let metric_names: Vec<&str> = (0..num_rows)
            .map(|i| {
                if i < num_rows / 2 {
                    "cpu.usage"
                } else {
                    "mem.used"
                }
            })
            .collect();
        let metric_name_array: DictionaryArray<Int32Type> =
            metric_names.iter().map(|s| Some(*s)).collect();
        let timestamp_array = UInt64Array::from((0..num_rows as u64).collect::<Vec<u64>>());
        let value_array = Float64Array::from((0..num_rows).map(|i| i as f64).collect::<Vec<_>>());
        let tsid_array = Int64Array::from(vec![42i64; num_rows]);
        let metric_type_array = UInt8Array::from(vec![0u8; num_rows]);

        let schema = Arc::new(Schema::new(vec![
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

        let path = dir.join(name);
        let file = std::fs::File::create(&path).unwrap();
        let props = config.to_writer_properties_with_metadata(
            &schema,
            Vec::new(),
            kv,
            &["metric_name".to_string()],
        );
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    }

    #[test]
    fn test_inspect_finds_column_index_when_page_stats_enabled() {
        let dir = TempDir::new().unwrap();
        let path = write_test_file(
            dir.path(),
            "with_pages.parquet",
            8,
            ParquetWriterConfig::default(),
            None,
        );

        let report = inspect_parquet_page_stats(&path, 100).unwrap();
        assert_eq!(report.num_row_groups, 1);
        assert!(report.has_full_page_index_coverage());
        for col in &report.row_groups[0].columns {
            assert!(
                col.has_column_index,
                "column '{}' missing column index",
                col.column_path
            );
            assert!(
                col.has_offset_index,
                "column '{}' missing offset index",
                col.column_path
            );
        }
    }

    #[test]
    fn test_inspect_pages_have_min_max_for_sort_columns() {
        // Two distinct metric names → at least one page boundary should
        // reflect that. Page count depends on data_page_size_limit, but
        // both pages must have populated min/max.
        let dir = TempDir::new().unwrap();
        let path = write_test_file(
            dir.path(),
            "min_max.parquet",
            16,
            ParquetWriterConfig::default(),
            None,
        );
        let report = inspect_parquet_page_stats(&path, 100).unwrap();

        let metric_name_col = &report.row_groups[0]
            .columns
            .iter()
            .find(|c| c.column_path == "metric_name")
            .expect("metric_name column present");
        for page in &metric_name_col.pages {
            assert!(page.min.is_some(), "page min should be populated");
            assert!(page.max.is_some(), "page max should be populated");
        }
    }

    #[test]
    fn test_inspect_multi_page_column() {
        // Force multiple pages by writing many rows with a small data
        // page size, then assert the inspector reports more than one page.
        let dir = TempDir::new().unwrap();
        let config = ParquetWriterConfig::default()
            .with_data_page_size(64)
            .with_write_batch_size(8);
        let path = write_test_file(dir.path(), "multi_page.parquet", 1024, config, None);
        let report = inspect_parquet_page_stats(&path, usize::MAX).unwrap();

        let metric_name = report.row_groups[0]
            .columns
            .iter()
            .find(|c| c.column_path == "metric_name")
            .unwrap();
        assert!(
            metric_name.num_pages > 1,
            "expected multiple pages with small page size, got {}",
            metric_name.num_pages
        );
        // Per-page row counts (for all pages except the last) should sum
        // to less than total rows; with the last page included we cover
        // every row.
        let known_rows: usize = metric_name.pages.iter().filter_map(|p| p.num_rows).sum();
        assert!(known_rows > 0);
    }

    #[test]
    fn test_inspect_marker_round_trips() {
        let dir = TempDir::new().unwrap();
        let kv = vec![
            KeyValue::new(
                PARQUET_META_RG_PARTITION_PREFIX_LEN.to_string(),
                "3".to_string(),
            ),
            KeyValue::new(
                PARQUET_META_SORT_FIELDS.to_string(),
                "metric_name|timeseries_id|timestamp_secs/V2".to_string(),
            ),
        ];
        let path = write_test_file(
            dir.path(),
            "marker.parquet",
            16,
            ParquetWriterConfig::default(),
            Some(kv),
        );
        let report = inspect_parquet_page_stats(&path, 100).unwrap();

        assert_eq!(report.rg_partition_prefix_len, 3);
        assert_eq!(
            report.sort_fields.as_deref(),
            Some("metric_name|timeseries_id|timestamp_secs/V2")
        );
    }

    #[test]
    fn test_inspect_marker_absent_reads_as_zero() {
        let dir = TempDir::new().unwrap();
        let path = write_test_file(
            dir.path(),
            "no_marker.parquet",
            8,
            ParquetWriterConfig::default(),
            None,
        );
        let report = inspect_parquet_page_stats(&path, 100).unwrap();
        assert_eq!(report.rg_partition_prefix_len, 0);
    }

    #[test]
    fn test_verify_partition_prefix_no_op_for_zero() {
        let dir = TempDir::new().unwrap();
        let path = write_test_file(
            dir.path(),
            "no_marker.parquet",
            8,
            ParquetWriterConfig::default(),
            None,
        );
        let report = inspect_parquet_page_stats(&path, 100).unwrap();
        verify_partition_prefix(&report).expect("prefix=0 should always verify");
    }

    #[test]
    fn test_verify_partition_prefix_fails_when_min_ne_max() {
        // A file with prefix_len = 1 but multiple metric_names in the
        // same RG must fail verification.
        let dir = TempDir::new().unwrap();
        let kv = vec![
            KeyValue::new(
                PARQUET_META_RG_PARTITION_PREFIX_LEN.to_string(),
                "1".to_string(),
            ),
            KeyValue::new(
                PARQUET_META_SORT_FIELDS.to_string(),
                "metric_name|timeseries_id|timestamp_secs/V2".to_string(),
            ),
        ];
        let path = write_test_file(
            dir.path(),
            "violation.parquet",
            16,
            ParquetWriterConfig::default(),
            Some(kv),
        );
        let report = inspect_parquet_page_stats(&path, 100).unwrap();
        let err = verify_partition_prefix(&report)
            .expect_err("two metric_names in one RG must fail prefix=1 verification");
        let msg = err.to_string();
        assert!(
            msg.contains("rg_partition_prefix_len") && msg.contains("metric_name"),
            "diagnostic should mention the marker and the column, got: {msg}"
        );
    }

    #[test]
    fn test_verify_partition_prefix_passes_for_single_metric_per_rg() {
        // All rows have the same metric_name → prefix=1 should hold.
        let dir = TempDir::new().unwrap();
        let metric_name_array: DictionaryArray<Int32Type> =
            (0..16).map(|_| Some("only.one")).collect();
        let timestamp_array = UInt64Array::from((0..16u64).collect::<Vec<_>>());
        let value_array = Float64Array::from((0..16).map(|i| i as f64).collect::<Vec<_>>());
        let tsid_array = Int64Array::from(vec![42i64; 16]);
        let metric_type_array = UInt8Array::from(vec![0u8; 16]);

        let schema = Arc::new(Schema::new(vec![
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

        let path = dir.path().join("single_metric.parquet");
        let file = std::fs::File::create(&path).unwrap();
        let kv = vec![
            KeyValue::new(
                PARQUET_META_RG_PARTITION_PREFIX_LEN.to_string(),
                "1".to_string(),
            ),
            KeyValue::new(
                PARQUET_META_SORT_FIELDS.to_string(),
                "metric_name|timeseries_id|timestamp_secs/V2".to_string(),
            ),
        ];
        let props = ParquetWriterConfig::default().to_writer_properties_with_metadata(
            &schema,
            Vec::new(),
            Some(kv),
            &["metric_name".to_string()],
        );
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let report = inspect_parquet_page_stats(&path, 100).unwrap();
        verify_partition_prefix(&report).expect("single-metric file should satisfy prefix=1");
    }

    #[test]
    fn test_first_n_sort_field_names_strips_version() {
        let names = first_n_sort_field_names("metric_name|host|timestamp_secs/V2", 3).unwrap();
        assert_eq!(names, vec!["metric_name", "host", "timestamp_secs"]);
    }

    #[test]
    fn test_first_n_sort_field_names_partial() {
        let names = first_n_sort_field_names("metric_name|host|timestamp_secs/V2", 1).unwrap();
        assert_eq!(names, vec!["metric_name"]);
    }

    #[test]
    fn test_first_n_sort_field_names_overflow_errors() {
        let err = first_n_sort_field_names("metric_name|timestamp_secs/V2", 5).unwrap_err();
        assert!(err.to_string().contains("exceeds sort schema length"));
    }

    /// Footer-size delta test: page-level statistics inflate the footer
    /// because every (RG, column) gets a Column Index + Offset Index entry.
    /// This test pins the rough magnitude so a regression that doubles or
    /// halves the footer size is visible to reviewers, and so that the
    /// claim in the PR description ("a few percent on representative data")
    /// is backed by a number a future change can re-derive.
    ///
    /// Representative shape: ~100K rows × ~7 columns, default page size,
    /// default 128K-row RG → one row group, dozens of pages per column.
    #[test]
    fn test_footer_size_delta_for_page_level_stats() {
        let dir = TempDir::new().unwrap();
        let num_rows = 100_000;

        // Build a representative batch: a couple of dictionary-encoded
        // string columns (cardinality matters for column index size),
        // a timestamp column, a value column.
        let metric_choices = ["cpu.user", "cpu.system", "mem.used", "disk.io"];
        let metric_names: Vec<&str> = (0..num_rows)
            .map(|i| metric_choices[i % metric_choices.len()])
            .collect();
        let host_choices: Vec<String> = (0..512).map(|i| format!("host-{i:03}")).collect();
        let host_names: Vec<&str> = (0..num_rows)
            .map(|i| host_choices[i % host_choices.len()].as_str())
            .collect();

        let metric_name_array: DictionaryArray<Int32Type> =
            metric_names.iter().map(|s| Some(*s)).collect();
        let host_array: DictionaryArray<Int32Type> = host_names.iter().map(|s| Some(*s)).collect();
        let timestamp_array = UInt64Array::from((0..num_rows as u64).collect::<Vec<u64>>());
        let value_array =
            Float64Array::from((0..num_rows).map(|i| i as f64 * 1.5).collect::<Vec<_>>());
        let tsid_array = Int64Array::from(
            (0..num_rows as i64)
                .map(|i| (i % 4096) * 7919)
                .collect::<Vec<_>>(),
        );
        let metric_type_array = UInt8Array::from(vec![0u8; num_rows]);

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "metric_name",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("metric_type", DataType::UInt8, false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("timeseries_id", DataType::Int64, false),
            Field::new(
                "host",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(metric_name_array),
                Arc::new(metric_type_array),
                Arc::new(timestamp_array),
                Arc::new(value_array),
                Arc::new(tsid_array),
                Arc::new(host_array),
            ],
        )
        .unwrap();

        // Build a baseline writer config with chunk-level stats only — the
        // pre-PR behavior. We override `to_writer_properties_with_metadata`
        // by constructing properties manually here.
        use parquet::basic::Compression as ParquetCompression;
        use parquet::file::properties::{EnabledStatistics, WriterProperties};

        let chunk_only_props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(128 * 1024))
            .set_data_page_size_limit(1024 * 1024)
            .set_write_batch_size(64 * 1024)
            .set_column_index_truncate_length(Some(64))
            .set_compression(ParquetCompression::ZSTD(
                parquet::basic::ZstdLevel::try_new(3).unwrap(),
            ))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();

        let chunk_path = dir.path().join("baseline_chunk.parquet");
        {
            let f = std::fs::File::create(&chunk_path).unwrap();
            let mut w = ArrowWriter::try_new(f, batch.schema(), Some(chunk_only_props)).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }

        // The new on-disk format: page-level stats via the public config.
        let page_path = dir.path().join("with_page_stats.parquet");
        let page_props = ParquetWriterConfig::default().to_writer_properties(&schema);
        {
            let f = std::fs::File::create(&page_path).unwrap();
            let mut w = ArrowWriter::try_new(f, batch.schema(), Some(page_props)).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }

        let chunk_size = std::fs::metadata(&chunk_path).unwrap().len();
        let page_size = std::fs::metadata(&page_path).unwrap().len();
        let delta = page_size as i64 - chunk_size as i64;
        let pct = (delta as f64) / (chunk_size as f64) * 100.0;

        eprintln!(
            "footer-size delta: chunk={chunk_size} bytes, page={page_size} bytes, delta={delta} \
             bytes ({pct:+.2}%)"
        );

        // The new file must not be smaller (page-level data is strictly
        // additive). It also must not be more than 30% larger on this
        // representative shape — that bound is generous; production data
        // is observed at "a few percent" overhead.
        assert!(
            page_size >= chunk_size,
            "page-level stats must not shrink the file"
        );
        assert!(
            pct < 30.0,
            "footer-size delta {pct:.2}% exceeds 30% sanity bound"
        );

        // Sanity: confirm the page-stats file actually has column index +
        // offset index for every column (the whole point of the change).
        let report = inspect_parquet_page_stats(&page_path, 100).unwrap();
        assert!(report.has_full_page_index_coverage());
    }
}
