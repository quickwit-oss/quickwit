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
use quickwit_proto::metastore::{
    MetastoreService, MetastoreServiceClient, PublishSketchSplitsRequest, StageSketchSplitsRequest,
};
use quickwit_proto::types::IndexUid;

/// Write `batch` as a sketch parquet file, stage it in the sketch split table,
/// and publish it.
///
/// This intentionally writes the Arrow batch directly through Parquet's
/// `ArrowWriter`: the production split writer still assumes metrics rows have
/// a `timeseries_id` sort key, while the sketch schema under test stores only
/// the DDSketch payload columns plus tags.
pub async fn publish_sketch_split(
    metastore: &MetastoreServiceClient,
    index_uid: &IndexUid,
    data_dir: &std::path::Path,
    split_name: &str,
    batch: &RecordBatch,
) {
    let file_path = data_dir.join(format!("{split_name}.parquet"));
    let file = std::fs::File::create(&file_path).expect("create sketch parquet file");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(batch).expect("write sketch parquet batch");
    writer.close().expect("close sketch parquet writer");
    let size_bytes = std::fs::metadata(&file_path)
        .expect("sketch parquet metadata")
        .len();

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

    let mut builder = ParquetSplitMetadata::sketches_builder()
        .split_id(ParquetSplitId::new(split_name))
        .index_uid(index_uid.to_string())
        .time_range(TimeRange::new(min_ts, max_ts + 1))
        .num_rows(batch.num_rows() as u64)
        .size_bytes(size_bytes);
    for name in &metric_names {
        builder = builder.add_metric_name(name.clone());
    }

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

    metastore
        .clone()
        .stage_sketch_splits(
            StageSketchSplitsRequest::try_from_splits_metadata(
                index_uid.clone(),
                &[builder.build()],
            )
            .unwrap(),
        )
        .await
        .expect("stage_sketch_splits");
    metastore
        .clone()
        .publish_sketch_splits(PublishSketchSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_name.to_string()],
            replaced_split_ids: vec![],
            index_checkpoint_delta_json_opt: None,
            publish_token_opt: None,
        })
        .await
        .expect("publish_sketch_splits");
}
