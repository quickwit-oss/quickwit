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

//! Per-output state, writer setup, and finalization for the streaming
//! merge engine.
//!
//! Each output file in a streaming merge owns:
//! - An [`OutputWriterStorage`] holding the live parquet writer and a running count of row groups
//!   written to it (used to verify MS-3).
//! - An [`OutputAccumulator`] that concatenates this output's sort-col contributions across regions
//!   so per-output metadata (`qh.row_keys`, `qh.zonemap_regexes`, metric_names, time_range) can be
//!   computed from the output's actual rows at finalize time.
//!
//! Per-output `qh.row_keys` and `qh.zonemap_regexes` are *appended* to
//! the parquet footer just before close — they cannot be set at writer
//! construction because they depend on row content that the streaming
//! pass only finishes accumulating in `finalize_output`. The static
//! KV entries (sort schema, window, num_merge_ops, prefix_len) are set
//! at construction since they're identical regardless of row content.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use arrow::datatypes::{Schema as ArrowSchema, SchemaRef};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ulid::Ulid;

use super::super::writer::{build_merge_kv_metadata, resolve_sort_field_names};
use super::super::{InputMetadata, MergeOutputFile};
use crate::row_keys;
use crate::sort_fields::{is_timestamp_column_name, parse_sort_fields};
use crate::split::TAG_SERVICE;
use crate::storage::split_writer::{extract_metric_names, extract_time_range};
use crate::storage::streaming_writer::StreamingParquetWriter;
use crate::storage::{
    PARQUET_META_ROW_KEYS, PARQUET_META_ROW_KEYS_JSON, PARQUET_META_ZONEMAP_REGEXES,
};
use crate::zonemap::{self, ZonemapOptions};

/// Per-output-file mutable state owned across regions assigned to that file.
pub(crate) struct OutputWriterStorage {
    pub(crate) output_idx: usize,
    pub(crate) output_path: PathBuf,
    pub(crate) writer: StreamingParquetWriter<std::fs::File>,
    /// Number of parquet row groups written into this file so far. One
    /// row group is opened per region in the per-region processor, so
    /// this equals the count of regions assigned to this output once
    /// processing is done. Reported on [`MergeOutputFile::num_row_groups`].
    pub(crate) num_row_groups: usize,
}

/// Per-output-file accumulator. Each region's sort-col contribution is
/// merged into `accumulated_sort_batch`; per-output metadata
/// (row_keys, zonemap, metric_names, time_range) is computed once at
/// `finalize_output` time. Service names are collected during the
/// streaming write of the service body col within each region.
pub(crate) struct OutputAccumulator {
    pub(crate) output_idx: usize,
    /// Concatenated sort-col `RecordBatch` across all regions written
    /// to this output. Memory bounded by total sort col bytes in the
    /// output file (small — sort cols are narrow).
    pub(crate) accumulated_sort_batch: Option<RecordBatch>,
    /// Service names collected across regions' body-col writes for
    /// this output file.
    pub(crate) service_names: HashSet<String>,
    /// Cumulative row count = sum of regions' total_rows assigned here.
    pub(crate) num_rows: usize,
}

impl OutputAccumulator {
    pub(crate) fn new(output_idx: usize) -> Self {
        Self {
            output_idx,
            accumulated_sort_batch: None,
            service_names: HashSet::new(),
            num_rows: 0,
        }
    }

    pub(crate) fn append_sort_batch(&mut self, batch: RecordBatch) -> Result<()> {
        match self.accumulated_sort_batch.take() {
            None => {
                self.accumulated_sort_batch = Some(batch);
            }
            Some(prev) => {
                let schema = prev.schema();
                let combined = arrow::compute::concat_batches(&schema, [&prev, &batch].into_iter())
                    .context("appending region sort batch to output accumulator")?;
                self.accumulated_sort_batch = Some(combined);
            }
        }
        Ok(())
    }
}

/// Open a streaming Parquet writer for one output file. Caller is
/// responsible for calling `start_row_group` per region and writing
/// columns.
pub(crate) fn open_output_writer_for_streaming(
    output_idx: usize,
    output_dir: &Path,
    union_schema: &SchemaRef,
    input_meta: &InputMetadata,
    writer_config: &crate::storage::ParquetWriterConfig,
) -> Result<OutputWriterStorage> {
    let output_prefix_len = input_meta.rg_partition_prefix_len;
    // `qh.row_keys` and `qh.zonemap_regexes` MUST be derived from the
    // rows that end up in THIS output file, not from inputs — the
    // merge eliminates key overlap between outputs, so an output's
    // key metadata can be very different from any input's. We can't
    // compute those values until every region has been written, so
    // they are appended to the file's footer KV metadata in
    // `finalize_output` via `StreamingParquetWriter::append_key_value_metadata`
    // just before close. The KV entries set here cover the "static"
    // keys (sort_fields, window, num_merge_ops, prefix_len) that are
    // identical regardless of which rows the output contains.
    let kv_entries = build_merge_kv_metadata(input_meta, &None, &HashMap::new(), output_prefix_len);

    let sort_field_names = resolve_sort_field_names(&input_meta.sort_fields)?;
    let sorting_cols = build_sorting_columns_from_schema(union_schema, &input_meta.sort_fields)?;

    let props = writer_config.to_writer_properties_with_metadata(
        union_schema,
        sorting_cols,
        Some(kv_entries),
        &sort_field_names,
    );

    let output_filename = format!("merge_output_{}.parquet", Ulid::new());
    let output_path = output_dir.join(&output_filename);
    let file = std::fs::File::create(&output_path)
        .with_context(|| format!("creating output file: {}", output_path.display()))?;
    let writer = StreamingParquetWriter::try_new(file, Arc::clone(union_schema), props)
        .with_context(|| format!("opening streaming writer for output {output_idx}"))?;

    Ok(OutputWriterStorage {
        output_idx,
        output_path,
        writer,
        num_row_groups: 0,
    })
}

/// Compute `SortingColumn` entries from the union schema (no
/// RecordBatch needed — we just need the col indices).
pub(crate) fn build_sorting_columns_from_schema(
    schema: &SchemaRef,
    sort_fields_str: &str,
) -> Result<Vec<parquet::file::metadata::SortingColumn>> {
    let parsed = parse_sort_fields(sort_fields_str)?;
    let mut cols = Vec::new();
    for sf in &parsed.column {
        // Schema may use `timestamp_secs` for what the sort schema
        // calls `timestamp`. Match the existing alias handling.
        let resolved =
            if is_timestamp_column_name(&sf.name) && schema.index_of("timestamp_secs").is_ok() {
                "timestamp_secs"
            } else {
                sf.name.as_str()
            };
        let Ok(col_idx) = schema.index_of(resolved) else {
            continue;
        };
        cols.push(parquet::file::metadata::SortingColumn {
            column_idx: col_idx as i32,
            descending: sf.sort_direction
                == quickwit_proto::sortschema::SortColumnDirection::SortDirectionDescending as i32,
            nulls_first: false,
        });
    }
    Ok(cols)
}

/// Finalize one output file: close writer, gather size, compute
/// per-output static metadata from the accumulator's sort col data,
/// return the `MergeOutputFile`.
pub(crate) fn finalize_output(
    writer_state: OutputWriterStorage,
    accumulator: OutputAccumulator,
    input_meta: &InputMetadata,
) -> Result<MergeOutputFile> {
    let OutputWriterStorage {
        output_idx,
        output_path,
        mut writer,
        num_row_groups,
    } = writer_state;

    // Compute per-output metadata from the rows that actually landed
    // in THIS output file. Merging eliminates key overlap between
    // outputs, so the row_keys / zonemap / metric_names / time_range
    // each output advertises must come from its own accumulated sort
    // batch — they cannot be carried over from any input file.
    let sort_batch = accumulator
        .accumulated_sort_batch
        .unwrap_or_else(|| RecordBatch::new_empty(Arc::new(ArrowSchema::empty())));

    let row_keys_proto = if sort_batch.num_rows() > 0 {
        row_keys::extract_row_keys(&input_meta.sort_fields, &sort_batch)
            .with_context(|| format!("extracting row keys for output {output_idx}"))?
            .map(|rk| row_keys::encode_row_keys_proto(&rk))
    } else {
        None
    };

    let zonemap_opts = ZonemapOptions::default();
    let zonemap_regexes = if sort_batch.num_rows() > 0 {
        zonemap::extract_zonemap_regexes(&input_meta.sort_fields, &sort_batch, &zonemap_opts)
            .with_context(|| format!("extracting zonemap regexes for output {output_idx}"))?
    } else {
        HashMap::new()
    };

    let metric_names =
        if sort_batch.num_rows() > 0 && sort_batch.schema().index_of("metric_name").is_ok() {
            extract_metric_names(&sort_batch)
                .with_context(|| format!("extracting metric names for output {output_idx}"))?
        } else {
            HashSet::new()
        };

    let time_range =
        if sort_batch.num_rows() > 0 && sort_batch.schema().index_of("timestamp_secs").is_ok() {
            extract_time_range(&sort_batch)
                .with_context(|| format!("extracting time range for output {output_idx}"))?
        } else {
            crate::split::TimeRange::new(0, 0)
        };

    // Write the per-output `qh.row_keys` / `qh.zonemap_regexes` into
    // the file's KV metadata so downstream tools that read the
    // Parquet footer directly see the same values that
    // `MergeOutputFile` carries in memory.
    append_per_output_kv_metadata(&mut writer, row_keys_proto.as_ref(), &zonemap_regexes);

    let footer_metadata = writer
        .close()
        .with_context(|| format!("closing writer for output {output_idx}"))?;

    // MS-3: the count we report on `MergeOutputFile.num_row_groups`
    // must agree with what the on-disk parquet footer actually
    // contains. `num_row_groups` is bumped per `start_row_group` call
    // in the per-region processor; if we ever skip one or double-
    // count, this catches the drift in debug builds before downstream
    // metadata consumers see the inconsistency.
    debug_assert_eq!(
        footer_metadata.num_row_groups(),
        num_row_groups,
        "MergeOutputFile.num_row_groups ({num_row_groups}) disagrees with footer ({})",
        footer_metadata.num_row_groups(),
    );

    let size_bytes = std::fs::metadata(&output_path)
        .with_context(|| format!("stat output file: {}", output_path.display()))?
        .len();

    // If `service` is a sort column for this schema, it took the
    // sort-col write path in `process_region` and the body-col
    // `track_service` branch never saw it. Fold in the names from the
    // accumulated sort batch so `TAG_SERVICE` metadata stays accurate
    // regardless of which path wrote the column.
    let mut service_names = accumulator.service_names;
    if sort_batch.num_rows() > 0
        && let Ok(service_col_idx) = sort_batch.schema().index_of("service")
    {
        super::body_assembler::collect_service_names_from_page(
            sort_batch.column(service_col_idx).as_ref(),
            &mut service_names,
        )
        .with_context(|| {
            format!("collecting service names from sort col for output {output_idx}")
        })?;
    }

    let mut low_cardinality_tags: HashMap<String, HashSet<String>> = HashMap::new();
    if !service_names.is_empty() {
        low_cardinality_tags.insert(TAG_SERVICE.to_string(), service_names);
    }

    Ok(MergeOutputFile {
        path: output_path,
        num_rows: accumulator.num_rows,
        num_row_groups,
        size_bytes,
        row_keys_proto,
        zonemap_regexes,
        metric_names,
        time_range,
        low_cardinality_tags,
    })
}

/// Append the per-output `qh.row_keys` / `qh.zonemap_regexes` KV
/// entries to the streaming writer just before close. Encoded the
/// same way as the non-streaming writer (`build_merge_kv_metadata`):
/// base64 for the proto, JSON for the zonemap map, plus the optional
/// human-readable `qh.row_keys_json`.
fn append_per_output_kv_metadata(
    writer: &mut StreamingParquetWriter<std::fs::File>,
    row_keys_proto: Option<&Vec<u8>>,
    zonemap_regexes: &HashMap<String, String>,
) {
    if let Some(rk_bytes) = row_keys_proto {
        writer.append_key_value_metadata(parquet::file::metadata::KeyValue::new(
            PARQUET_META_ROW_KEYS.to_string(),
            BASE64.encode(rk_bytes),
        ));
        if let Ok(rk) =
            <quickwit_proto::sortschema::RowKeys as prost::Message>::decode(rk_bytes.as_slice())
            && let Ok(json) = serde_json::to_string(&rk)
        {
            writer.append_key_value_metadata(parquet::file::metadata::KeyValue::new(
                PARQUET_META_ROW_KEYS_JSON.to_string(),
                json,
            ));
        }
    }

    if !zonemap_regexes.is_empty() {
        let json = serde_json::to_string(zonemap_regexes)
            .expect("HashMap<String, String> JSON serialization cannot fail");
        writer.append_key_value_metadata(parquet::file::metadata::KeyValue::new(
            PARQUET_META_ZONEMAP_REGEXES.to_string(),
            json,
        ));
    }
}
