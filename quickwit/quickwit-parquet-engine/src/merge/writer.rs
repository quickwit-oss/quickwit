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

//! Output writing for merge results.
//!
//! Applies the merge order permutation to produce sorted RecordBatches,
//! extracts metadata (row keys, zonemaps), and writes Parquet files with
//! complete `qh.*` key-value metadata.

use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Array, BinaryArray, RecordBatch};
use arrow::compute::take;
use arrow::datatypes::SchemaRef;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::{KeyValue, SortingColumn};
use parquet::file::properties::WriterProperties;
use prost::Message;
use ulid::Ulid;

use super::merge_order::MergeRun;
use super::{InputMetadata, MergeConfig, MergeOutputFile};
use crate::row_keys;
use crate::sort_fields::parse_sort_fields;
use crate::sorted_series::SORTED_SERIES_COLUMN;
use crate::split::TAG_SERVICE;
use crate::storage::split_writer::{
    extract_metric_names, extract_service_names, extract_time_range,
};
use crate::storage::{
    PARQUET_META_NUM_MERGE_OPS, PARQUET_META_RG_PARTITION_PREFIX_LEN, PARQUET_META_ROW_KEYS,
    PARQUET_META_ROW_KEYS_JSON, PARQUET_META_SORT_FIELDS, PARQUET_META_WINDOW_DURATION,
    PARQUET_META_WINDOW_START, PARQUET_META_ZONEMAP_REGEXES,
};
use crate::zonemap::{self, ZonemapOptions};

/// Write the merge outputs.
///
/// For each output boundary range, applies the merge order permutation to
/// produce a sorted RecordBatch, extracts metadata, and writes a Parquet file.
pub fn write_merge_outputs(
    inputs: &[RecordBatch],
    union_schema: &SchemaRef,
    merge_order: &[MergeRun],
    boundaries: &[Range<usize>],
    output_dir: &Path,
    config: &MergeConfig,
    input_meta: &InputMetadata,
) -> Result<Vec<MergeOutputFile>> {
    let mut outputs = Vec::with_capacity(boundaries.len());

    for boundary in boundaries {
        let runs = &merge_order[boundary.clone()];

        // Build the take indices for this output's rows.
        let sorted_batch = apply_merge_permutation(inputs, union_schema, runs)?;

        if sorted_batch.num_rows() == 0 {
            continue;
        }

        // MC-3: verify the output batch is sorted by (sorted_series ASC,
        // timestamp_secs <direction per sort schema>). We check sorted_series
        // is non-decreasing; within equal sorted_series, timestamp_secs
        // respects the schema's sort direction.
        verify_sort_order(&sorted_batch, &input_meta.sort_fields);

        // Optimize the output batch: strip all-null columns and choose
        // the best encoding for each column based on its actual data.
        // String columns are dictionary-encoded when cardinality is low.
        // This runs per-output-file so each file's schema reflects its data.
        let sorted_batch = super::schema::optimize_output_batch(&sorted_batch);

        // Extract metadata from the optimized output batch.
        // Row keys and zonemaps are computed from the actual output data,
        // reflecting only the columns present in this file.
        let row_keys_proto = row_keys::extract_row_keys(&input_meta.sort_fields, &sorted_batch)
            .context("extracting row keys from merge output")?
            .map(|rk| row_keys::encode_row_keys_proto(&rk));

        let zonemap_opts = ZonemapOptions::default();
        let zonemap_regexes =
            zonemap::extract_zonemap_regexes(&input_meta.sort_fields, &sorted_batch, &zonemap_opts)
                .context("extracting zonemap regexes from merge output")?;

        // Predict the output's row group count. ArrowWriter rolls over a
        // new RG every `row_group_size` rows; we don't set a byte-based
        // threshold so the row count is the only driver. This prediction
        // determines the prefix alignment claim we embed in the file's KV
        // metadata: single-RG output vacuously satisfies any prefix, so
        // we propagate the input prefix; multi-RG output (with arbitrary
        // row-count-driven boundaries) must claim 0. PR-6 will replace
        // this with proper sort-prefix-aligned boundaries.
        let predicted_num_rgs =
            predict_num_row_groups(sorted_batch.num_rows(), config.writer_config.row_group_size);
        let output_prefix_len = if predicted_num_rgs <= 1 {
            input_meta.rg_partition_prefix_len
        } else {
            0
        };

        // Build KV metadata.
        let kv_entries = build_merge_kv_metadata(
            input_meta,
            &row_keys_proto,
            &zonemap_regexes,
            output_prefix_len,
        );

        // Build sorting_columns for Parquet metadata.
        let sorting_cols = build_sorting_columns(&sorted_batch, &input_meta.sort_fields)?;

        // Build writer properties.
        let sort_field_names = resolve_sort_field_names(&input_meta.sort_fields)?;
        let props = config.writer_config.to_writer_properties_with_metadata(
            &sorted_batch.schema(),
            sorting_cols,
            Some(kv_entries),
            &sort_field_names,
        );

        // Write the output file.
        let output_filename = format!("merge_output_{}.parquet", Ulid::new());
        let output_path = output_dir.join(&output_filename);

        // Extract per-output logical metadata from the actual rows.
        let metric_names = extract_metric_names(&sorted_batch)
            .context("extracting metric names from merge output")?;
        let time_range =
            extract_time_range(&sorted_batch).context("extracting time range from merge output")?;
        let service_names = extract_service_names(&sorted_batch)
            .context("extracting service names from merge output")?;

        let mut low_cardinality_tags = std::collections::HashMap::new();
        if !service_names.is_empty() {
            low_cardinality_tags.insert(TAG_SERVICE.to_string(), service_names);
        }

        let written = write_parquet_file(&sorted_batch, &output_path, props)?;

        // Confirm prediction matches reality. If this ever fires, somebody
        // enabled a byte-based RG threshold in the writer config and the
        // KV's `rg_partition_prefix_len` will be inconsistent with the
        // actual on-disk layout.
        debug_assert_eq!(
            predicted_num_rgs, written.num_row_groups,
            "predicted RG count {} does not match actual {} — rg_partition_prefix_len in KV \
             metadata may be wrong",
            predicted_num_rgs, written.num_row_groups,
        );

        outputs.push(MergeOutputFile {
            path: output_path,
            num_rows: sorted_batch.num_rows(),
            num_row_groups: written.num_row_groups,
            size_bytes: written.size_bytes,
            row_keys_proto,
            zonemap_regexes,
            metric_names,
            time_range,
            low_cardinality_tags,
        });
    }

    Ok(outputs)
}

/// Apply the merge permutation to produce a single sorted RecordBatch.
///
/// Takes the relevant row ranges from each input according to the merge runs,
/// concatenates into a single batch, and applies the permutation via `take`.
fn apply_merge_permutation(
    inputs: &[RecordBatch],
    union_schema: &SchemaRef,
    runs: &[MergeRun],
) -> Result<RecordBatch> {
    if runs.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(union_schema)));
    }

    let total_rows: usize = runs.iter().map(|r| r.row_count).sum();

    // Build per-input row slices grouped by input index, preserving merge order.
    // We'll concatenate all inputs and use global indices for `take`.
    //
    // First, compute the row offset of each input in the concatenated view.
    let mut input_offsets: Vec<usize> = Vec::with_capacity(inputs.len());
    let mut offset = 0;
    for input in inputs {
        input_offsets.push(offset);
        offset += input.num_rows();
    }

    // Build global take indices by walking the merge runs.
    // Use u64 to avoid silent truncation when concatenated inputs exceed u32::MAX rows.
    let mut take_indices: Vec<u64> = Vec::with_capacity(total_rows);
    for run in runs {
        let base = input_offsets[run.input_index] + run.start_row;
        for row in 0..run.row_count {
            take_indices.push((base + row) as u64);
        }
    }

    // Concatenate all inputs into one batch.
    let all_batches: Vec<&RecordBatch> = inputs.iter().collect();
    let concatenated = arrow::compute::concat_batches(union_schema, all_batches.into_iter())
        .context("concatenating inputs for merge")?;

    // Apply permutation. Use UInt64 indices to support >4B row merges.
    let indices = arrow::array::UInt64Array::from(take_indices);
    let columns: Vec<Arc<dyn arrow::array::Array>> = concatenated
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("applying merge permutation")?;

    let result = RecordBatch::try_new(Arc::clone(union_schema), columns)
        .context("building merge output batch")?;

    Ok(result)
}

/// Predict the number of row groups `ArrowWriter` will produce for a
/// batch of `num_rows` rows. Assumes `row_group_size` is the only RG
/// rollover threshold (which is the case as long as the writer config
/// does not set `set_max_row_group_bytes`). Returns at least 1.
fn predict_num_row_groups(num_rows: usize, row_group_size: usize) -> usize {
    if num_rows == 0 || row_group_size == 0 {
        return 1;
    }
    num_rows.div_ceil(row_group_size).max(1)
}

/// Build Parquet KV metadata entries for a merge output file.
///
/// `output_prefix_len` is the alignment claim to embed in the output's
/// `qh.rg_partition_prefix_len` KV — caller computes this based on
/// whether the file is going to be single-RG (preserve input prefix)
/// or multi-RG (must be 0).
fn build_merge_kv_metadata(
    input_meta: &InputMetadata,
    row_keys_proto: &Option<Vec<u8>>,
    zonemap_regexes: &std::collections::HashMap<String, String>,
    output_prefix_len: u32,
) -> Vec<KeyValue> {
    let mut kvs = Vec::new();

    if !input_meta.sort_fields.is_empty() {
        kvs.push(KeyValue::new(
            PARQUET_META_SORT_FIELDS.to_string(),
            input_meta.sort_fields.clone(),
        ));
    }

    if let Some(ws) = input_meta.window_start_secs {
        kvs.push(KeyValue::new(
            PARQUET_META_WINDOW_START.to_string(),
            ws.to_string(),
        ));
    }

    if input_meta.window_duration_secs > 0 {
        kvs.push(KeyValue::new(
            PARQUET_META_WINDOW_DURATION.to_string(),
            input_meta.window_duration_secs.to_string(),
        ));
    }

    kvs.push(KeyValue::new(
        PARQUET_META_NUM_MERGE_OPS.to_string(),
        input_meta.num_merge_ops.to_string(),
    ));

    if output_prefix_len > 0 {
        kvs.push(KeyValue::new(
            PARQUET_META_RG_PARTITION_PREFIX_LEN.to_string(),
            output_prefix_len.to_string(),
        ));
    }

    if let Some(rk_bytes) = row_keys_proto {
        kvs.push(KeyValue::new(
            PARQUET_META_ROW_KEYS.to_string(),
            BASE64.encode(rk_bytes),
        ));

        // Best-effort human-readable JSON for debugging.
        // Allow nested if: let-chain form triggers a rustfmt parse error.
        #[allow(clippy::collapsible_if)]
        if let Ok(rk) = quickwit_proto::sortschema::RowKeys::decode(rk_bytes.as_slice()) {
            if let Ok(json) = serde_json::to_string(&rk) {
                kvs.push(KeyValue::new(PARQUET_META_ROW_KEYS_JSON.to_string(), json));
            }
        }
    }

    if !zonemap_regexes.is_empty() {
        let json = serde_json::to_string(&zonemap_regexes)
            .expect("HashMap<String, String> JSON serialization cannot fail");
        kvs.push(KeyValue::new(
            PARQUET_META_ZONEMAP_REGEXES.to_string(),
            json,
        ));
    }

    kvs
}

/// Build `SortingColumn` entries for Parquet file metadata.
fn build_sorting_columns(batch: &RecordBatch, sort_fields_str: &str) -> Result<Vec<SortingColumn>> {
    let sort_schema = parse_sort_fields(sort_fields_str)?;
    let schema = batch.schema();

    let mut sorting_cols = Vec::new();
    for col in &sort_schema.column {
        let col_name = crate::sort_fields::normalize_column_name(&col.name);
        if let Ok(idx) = schema.index_of(col_name) {
            let descending = col.sort_direction
                == quickwit_proto::sortschema::SortColumnDirection::SortDirectionDescending as i32;
            sorting_cols.push(SortingColumn {
                column_idx: idx as i32,
                descending,
                nulls_first: false,
            });
        }
    }

    Ok(sorting_cols)
}

/// Resolve sort field names from the sort schema string.
/// Normalizes legacy names (e.g. "timestamp" → "timestamp_secs").
fn resolve_sort_field_names(sort_fields_str: &str) -> Result<Vec<String>> {
    let sort_schema = parse_sort_fields(sort_fields_str)?;
    Ok(sort_schema
        .column
        .iter()
        .map(|c| crate::sort_fields::normalize_column_name(&c.name).to_string())
        .collect())
}

/// MC-3: Verify the output batch is sorted by (sorted_series ASC,
/// timestamp_secs <direction per sort schema>).
///
/// Checks that sorted_series values are non-decreasing, and within equal
/// sorted_series values, timestamp_secs respects the schema's sort direction.
fn verify_sort_order(batch: &RecordBatch, sort_fields_str: &str) {
    if batch.num_rows() <= 1 {
        return;
    }

    // Determine timestamp sort direction from the sort schema.
    let sort_schema =
        parse_sort_fields(sort_fields_str).expect("sort schema must parse for MC-3 check");
    let ts_descending = sort_schema
        .column
        .iter()
        .find(|c| crate::sort_fields::is_timestamp_column_name(&c.name))
        .map(|c| {
            c.sort_direction
                == quickwit_proto::sortschema::SortColumnDirection::SortDirectionDescending as i32
        })
        .unwrap_or(true);

    let ss_idx = batch
        .schema()
        .index_of(SORTED_SERIES_COLUMN)
        .expect("sorted_series column must exist for MC-3 check");
    let ss_col = batch
        .column(ss_idx)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("sorted_series must be Binary");

    let ts_idx = batch
        .schema()
        .index_of(crate::sort_fields::TIMESTAMP_SECS)
        .expect("timestamp_secs column must exist for MC-3 check");
    let ts_col = batch.column(ts_idx);

    // Timestamp may be UInt64 or Int64 depending on schema.
    let ts_values: Vec<i64> =
        if let Some(arr) = ts_col.as_any().downcast_ref::<arrow::array::UInt64Array>() {
            arr.values().iter().map(|&v| v as i64).collect()
        } else if let Some(arr) = ts_col.as_any().downcast_ref::<arrow::array::Int64Array>() {
            arr.values().to_vec()
        } else {
            panic!("timestamp_secs must be UInt64 or Int64 for MC-3 check");
        };

    for i in 0..batch.num_rows() - 1 {
        let ss_a = ss_col.value(i);
        let ss_b = ss_col.value(i + 1);

        match ss_a.cmp(ss_b) {
            std::cmp::Ordering::Greater => {
                quickwit_dst::check_invariant!(
                    quickwit_dst::invariants::InvariantId::MC3,
                    false,
                    ": sorted_series decreased at row {}",
                    i
                );
            }
            std::cmp::Ordering::Equal => {
                // Within same series, timestamp must respect the schema direction.
                // Use the shared compare_with_null_ordering — same function the
                // Stateright model uses — so production and model agree.
                let ts_ascending = !ts_descending;
                let cmp = quickwit_dst::invariants::sort::compare_with_null_ordering(
                    Some(&ts_values[i]),
                    Some(&ts_values[i + 1]),
                    ts_ascending,
                );
                let direction_str = if ts_descending {
                    "descending"
                } else {
                    "ascending"
                };
                quickwit_dst::check_invariant!(
                    quickwit_dst::invariants::InvariantId::MC3,
                    cmp != std::cmp::Ordering::Greater,
                    ": timestamp_secs not {} within series at row {}: {} vs {}",
                    direction_str,
                    i,
                    ts_values[i],
                    ts_values[i + 1]
                );
            }
            std::cmp::Ordering::Less => {
                // sorted_series increased — correct.
            }
        }
    }
}

/// Result of writing a single Parquet output file.
struct WrittenFile {
    /// File size in bytes on disk after `ArrowWriter::close()`.
    size_bytes: u64,
    /// Number of row groups the writer produced.
    num_row_groups: usize,
}

/// Write a RecordBatch to a Parquet file. Returns its on-disk size and
/// the number of row groups produced by the writer.
fn write_parquet_file(
    batch: &RecordBatch,
    path: &Path,
    props: WriterProperties,
) -> Result<WrittenFile> {
    let file = std::fs::File::create(path)
        .with_context(|| format!("creating output file: {}", path.display()))?;

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
        .with_context(|| format!("creating parquet writer: {}", path.display()))?;

    writer
        .write(batch)
        .with_context(|| format!("writing batch: {}", path.display()))?;

    let metadata = writer
        .close()
        .with_context(|| format!("closing parquet writer: {}", path.display()))?;
    let num_row_groups = metadata.num_row_groups();

    let size_bytes = std::fs::metadata(path)
        .with_context(|| format!("reading file size: {}", path.display()))?
        .len();

    Ok(WrittenFile {
        size_bytes,
        num_row_groups,
    })
}
