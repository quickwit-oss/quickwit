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

use std::collections::HashSet;

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::Int32Type;
use quickwit_metastore::StageParquetSplitsRequestExt;
use quickwit_parquet_engine::split::{ParquetSplitId, ParquetSplitMetadata, TimeRange};
use quickwit_parquet_engine::storage::{ParquetWriter, ParquetWriterConfig};
use quickwit_parquet_engine::table_config::TableConfig;
use quickwit_proto::metastore::{
    MetastoreService, MetastoreServiceClient, PublishMetricsSplitsRequest,
    StageMetricsSplitsRequest,
};
use quickwit_proto::types::IndexUid;

/// Write `batch` as a parquet file under `data_dir`, stage it in the metastore,
/// and publish it.
///
/// Tag columns (`service`, `env`, `datacenter`, `region`, `host`) present in
/// the batch schema are indexed for split-level pruning.
pub async fn publish_split(
    metastore: &MetastoreServiceClient,
    index_uid: &IndexUid,
    data_dir: &std::path::Path,
    split_name: &str,
    batch: &RecordBatch,
) {
    publish_split_with_tag_metadata(metastore, index_uid, data_dir, split_name, batch, true).await;
}

pub(crate) async fn publish_split_with_tag_metadata(
    metastore: &MetastoreServiceClient,
    index_uid: &IndexUid,
    data_dir: &std::path::Path,
    split_name: &str,
    batch: &RecordBatch,
    include_low_cardinality_tags: bool,
) {
    let (parquet_bytes, (row_keys_proto, zonemap_regexes)) =
        ParquetWriter::new(ParquetWriterConfig::default(), &TableConfig::default())
            .unwrap()
            .write_to_bytes(batch, None)
            .expect("parquet encode");
    let size_bytes = parquet_bytes.len() as u64;
    std::fs::write(
        data_dir.join(format!("{split_name}.parquet")),
        &parquet_bytes,
    )
    .expect("write parquet file");

    let batch_schema = batch.schema();
    let ts_idx = batch_schema.index_of("timestamp_secs").unwrap();
    let ts_col = batch
        .column(ts_idx)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap();
    let min_ts = (0..ts_col.len())
        .map(|i| ts_col.value(i))
        .min()
        .unwrap_or(0);
    let max_ts = (0..ts_col.len())
        .map(|i| ts_col.value(i))
        .max()
        .unwrap_or(0);

    let mn_idx = batch_schema.index_of("metric_name").unwrap();
    let dict = batch
        .column(mn_idx)
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<Int32Type>>()
        .unwrap();
    let values = dict
        .values()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let metric_names: HashSet<String> = (0..values.len())
        .filter(|i| !values.is_null(*i))
        .map(|i| values.value(i).to_string())
        .collect();

    let mut builder = ParquetSplitMetadata::metrics_builder()
        .split_id(ParquetSplitId::new(split_name))
        .index_uid(index_uid.to_string())
        .time_range(TimeRange::new(min_ts, max_ts + 1))
        .num_rows(batch.num_rows() as u64)
        .size_bytes(size_bytes)
        .sort_fields(TableConfig::default().effective_sort_fields());
    for name in &metric_names {
        builder = builder.add_metric_name(name.clone());
    }
    if let Some(row_keys_proto) = row_keys_proto {
        builder = builder.row_keys_proto(row_keys_proto);
    }
    for (column, regex) in zonemap_regexes {
        builder = builder.add_zonemap_regex(column, regex);
    }

    if include_low_cardinality_tags {
        for tag_col in &["service", "env", "datacenter", "region", "host"] {
            if let Ok(col_idx) = batch_schema.index_of(tag_col) {
                let col = batch.column(col_idx);
                let values: HashSet<String> = if let Some(dict) =
                    col.as_any()
                        .downcast_ref::<arrow::array::DictionaryArray<Int32Type>>()
                {
                    let keys = dict
                        .keys()
                        .as_any()
                        .downcast_ref::<arrow::array::Int32Array>()
                        .unwrap();
                    let vals = dict
                        .values()
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .unwrap();
                    (0..batch.num_rows())
                        .filter(|i| !keys.is_null(*i))
                        .map(|i| vals.value(keys.value(i) as usize).to_string())
                        .collect()
                } else {
                    HashSet::new()
                };
                for v in values {
                    builder = builder.add_low_cardinality_tag(tag_col.to_string(), v);
                }
            }
        }
    }

    metastore
        .clone()
        .stage_metrics_splits(
            StageMetricsSplitsRequest::try_from_splits_metadata(
                index_uid.clone(),
                &[builder.build()],
            )
            .unwrap(),
        )
        .await
        .expect("stage_metrics_splits");
    metastore
        .clone()
        .publish_metrics_splits(PublishMetricsSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_name.to_string()],
            replaced_split_ids: vec![],
            index_checkpoint_delta_json_opt: None,
            publish_token_opt: None,
        })
        .await
        .expect("publish_metrics_splits");
}
