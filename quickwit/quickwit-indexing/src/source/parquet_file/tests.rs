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

use std::num::NonZeroUsize;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use quickwit_actors::{Command, Universe};
use quickwit_common::uri::Uri;
use quickwit_config::{FileSourceParams, SourceConfig, SourceInputFormat, SourceParams};
use quickwit_proto::types::IndexUid;
use serde_json::json;

use super::*;
use crate::actors::DocProcessor;
use crate::models::RawDocBatch;
use crate::source::tests::SourceRuntimeBuilder;
use crate::source::{SourceActor, SourceFactory};

const SOURCE_ID: &str = "test-parquet-source";

/// Writes `num_rows` rows `{"id": i, "message": "message-{i}"}` in row groups of
/// `row_group_num_rows` rows.
fn write_parquet_file(path: &Path, num_rows: usize, row_group_num_rows: usize) -> Uri {
    let json_docs: Vec<serde_json::Value> = (0..num_rows)
        .map(|id| json!({"id": id, "message": format!("message-{id}")}))
        .collect();
    write_json_docs_as_parquet_file(path, &json_docs, row_group_num_rows).unwrap();
    Uri::from_str(path.to_str().unwrap()).unwrap()
}

fn parquet_source_runtime(
    index_uid: IndexUid,
    file_uri: &Uri,
    input_format: SourceInputFormat,
) -> crate::source::SourceRuntime {
    let source_config = SourceConfig {
        source_id: SOURCE_ID.to_string(),
        num_pipelines: NonZeroUsize::MIN,
        enabled: true,
        source_params: SourceParams::File(FileSourceParams::Filepath(file_uri.clone())),
        transform_config: None,
        input_format,
    };
    SourceRuntimeBuilder::new(index_uid, source_config)
        .with_mock_metastore(None)
        .build()
}

/// Creates a source reading `source_file_uri` from a plan reading `plan_file_uri`.
async fn create_parquet_source(
    plan_file_uri: &Uri,
    source_file_uri: &Uri,
    input_format: SourceInputFormat,
) -> anyhow::Result<Box<dyn crate::source::Source>> {
    let plan = Arc::new(ParquetLoadPlan::try_new(plan_file_uri.clone(), 3).unwrap());
    let index_uid = IndexUid::new_with_random_ulid("test-index");
    let source_runtime = parquet_source_runtime(index_uid, source_file_uri, input_format);
    ParquetSourceFactory::new(plan)
        .create_source(source_runtime)
        .await
}

#[tokio::test]
async fn test_parquet_source() {
    let temp_dir = tempfile::tempdir().unwrap();
    let file_uri = write_parquet_file(&temp_dir.path().join("test.parquet"), 10, 4);
    let index_uid = IndexUid::new_with_random_ulid("test-index");

    let plan = ParquetLoadPlan::try_new(file_uri.clone(), 3).unwrap();
    assert_eq!(plan.num_rows(), 10);
    assert_eq!(plan.num_row_groups(), 3);

    let universe = Universe::with_accelerated_time();
    let (doc_processor_mailbox, doc_processor_inbox) =
        universe.create_test_mailbox::<DocProcessor>();
    let source_runtime = parquet_source_runtime(index_uid, &file_uri, SourceInputFormat::Json);
    let parquet_source = ParquetSourceFactory::new(Arc::new(plan))
        .create_source(source_runtime)
        .await
        .unwrap();
    let source_actor = SourceActor::new(parquet_source, doc_processor_mailbox);
    let (_source_mailbox, source_handle) = universe.spawn_builder().spawn(source_actor);
    let (exit_status, observable_state) = source_handle.join().await;
    assert!(exit_status.is_success());
    assert_eq!(observable_state["num_rows_emitted"], 10);

    let messages = doc_processor_inbox.drain_for_test();
    assert!(matches!(
        messages.last().unwrap().downcast_ref::<Command>().unwrap(),
        Command::ExitWithSuccess
    ));
    let raw_doc_batches: Vec<&RawDocBatch> = messages
        .iter()
        .filter_map(|message| message.downcast_ref::<RawDocBatch>())
        .collect();
    // Row groups of 4, 4 and 2 rows, read in batches of 3 rows.
    assert_eq!(raw_doc_batches.len(), 5);
    assert!(
        raw_doc_batches
            .iter()
            .all(|raw_doc_batch| raw_doc_batch.checkpoint_delta.is_empty())
    );
    let ids: Vec<u64> = raw_doc_batches
        .iter()
        .flat_map(|raw_doc_batch| raw_doc_batch.docs.iter())
        .map(|doc| {
            let json_doc: serde_json::Value = serde_json::from_slice(doc).unwrap();
            json_doc["id"].as_u64().unwrap()
        })
        .collect();
    assert_eq!(ids, (0..10).collect::<Vec<u64>>());
}

#[tokio::test]
async fn test_parquet_source_requires_the_plan_file() {
    let temp_dir = tempfile::tempdir().unwrap();
    let plan_file_uri = write_parquet_file(&temp_dir.path().join("plan.parquet"), 3, 2);
    let other_file_uri = write_parquet_file(&temp_dir.path().join("other.parquet"), 3, 2);
    let error = create_parquet_source(&plan_file_uri, &other_file_uri, SourceInputFormat::Json)
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("the Parquet load plan reads"));
}

#[tokio::test]
async fn test_parquet_source_requires_the_json_input_format() {
    let temp_dir = tempfile::tempdir().unwrap();
    let file_uri = write_parquet_file(&temp_dir.path().join("test.parquet"), 3, 2);
    let error = create_parquet_source(&file_uri, &file_uri, SourceInputFormat::PlainText)
        .await
        .err()
        .unwrap();
    assert!(
        error
            .to_string()
            .contains("require the `json` input format")
    );
}

#[test]
fn test_parquet_load_plan_rejects_remote_files() {
    let file_uri = Uri::for_test("s3://bucket/test.parquet");
    let error = ParquetLoadPlan::try_new(file_uri, 10).unwrap_err();
    assert!(error.to_string().contains("only supports local files"));
}
