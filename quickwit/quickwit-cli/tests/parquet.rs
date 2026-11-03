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

#![cfg(feature = "parquet")]

use std::num::NonZeroUsize;

use quickwit_cli::tool::{LocalIngestDocsArgs, local_ingest_docs_cli};
use quickwit_common::rand::append_random_suffix;
use quickwit_common::uri::Uri;
use quickwit_config::{ConfigFormat, SourceInputFormat, load_index_config_from_user_config};
use quickwit_index_management::IndexService;
use quickwit_indexing::source::parquet_file_source::{
    ParquetSharedReader, write_f64_values_as_parquet_file, write_json_docs_as_parquet_file,
};
use quickwit_metastore::{
    ListSplitsQuery, ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, SplitMetadata,
    SplitState,
};
use quickwit_proto::metastore::{ListSplitsRequest, MetastoreService};
use serde_json::json;

use crate::helpers::{TestEnv, TestStorageType, create_test_env, uri_from_path};

#[allow(dead_code)]
mod helpers;

const NUM_ROWS: usize = 1_000;
const ROW_GROUP_NUM_ROWS: usize = 50;

const INDEX_CONFIG: &str = r#"
    version: 0.8
    index_id: #index_id
    index_uri: #index_uri
    doc_mapping:
      field_mappings:
        - name: ts
          type: datetime
          input_formats: [unix_timestamp]
          fast: true
        - name: event
          type: text
        - name: city
          type: text
          tokenizer: raw
      timestamp_field: ts
    indexing_settings:
      commit_timeout_secs: 3600
      split_num_docs_target: #split_num_docs_target
      merge_policy: #merge_policy
      resources:
        heap_size: 50MB
"#;

/// Creates an index and a Parquet file with `NUM_ROWS` valid documents.
async fn setup_parquet_index(
    split_num_docs_target: usize,
    merge_policy: &str,
) -> (TestEnv, String, Vec<serde_json::Value>, Uri) {
    quickwit_common::setup_logging_for_tests();
    let index_id = append_random_suffix("test-parquet-local-ingest");
    let test_env = create_test_env(index_id.clone(), TestStorageType::LocalFileSystem)
        .await
        .unwrap();
    let index_config_yaml = INDEX_CONFIG
        .replace("#index_id", &index_id)
        .replace("#index_uri", test_env.index_uri.as_str())
        .replace("#split_num_docs_target", &split_num_docs_target.to_string())
        .replace("#merge_policy", merge_policy);
    let index_config = load_index_config_from_user_config(
        ConfigFormat::Yaml,
        index_config_yaml.as_bytes(),
        &test_env.index_uri,
    )
    .unwrap();
    let metastore = test_env.metastore().await;
    let mut index_service = IndexService::new(metastore, test_env.storage_resolver.clone());
    index_service
        .create_index(index_config, false)
        .await
        .unwrap();

    let json_docs: Vec<serde_json::Value> = (0..NUM_ROWS)
        .map(|row_idx| {
            let city = ["paris", "tokyo", "london"][row_idx % 3];
            json!({
                "ts": 1_700_000_000 + row_idx as i64,
                "event": format!("event-{row_idx}"),
                "city": city,
            })
        })
        .collect();
    let parquet_path = test_env.data_dir_path.join("docs.parquet");
    write_json_docs_as_parquet_file(&parquet_path, &json_docs, ROW_GROUP_NUM_ROWS).unwrap();
    let parquet_uri: Uri = uri_from_path(&parquet_path);
    (test_env, index_id, json_docs, parquet_uri)
}

#[tokio::test]
async fn test_local_ingest_parquet_with_multiple_pipelines() {
    let (test_env, index_id, json_docs, parquet_uri) =
        setup_parquet_index(300, "{type: no_merge}").await;

    // An unknown excluded column is rejected before indexing anything.
    let mut args = parquet_args(&test_env, &index_id, &parquet_uri, false);
    args.excluded_columns = vec!["unknown".to_string()];
    let error = local_ingest_docs_cli(args).await.unwrap_err();
    assert!(format!("{error:#}").contains("does not exist"), "{error:#}");
    assert!(list_published_splits(&test_env).await.is_empty());

    let mut args = parquet_args(&test_env, &index_id, &parquet_uri, false);
    args.num_pipelines = NonZeroUsize::new(4).unwrap();
    local_ingest_docs_cli(args).await.unwrap();

    let splits = list_published_splits(&test_env).await;
    let num_docs: usize = splits.iter().map(|split| split.num_docs).sum();
    assert_eq!(num_docs, NUM_ROWS);
    // No merges: split at 300 docs (plus at most one row-group batch) or pipeline EOF.
    assert!(splits.len() >= 4);
    assert!(
        splits
            .iter()
            .all(|split| split.num_docs <= 300 + ROW_GROUP_NUM_ROWS)
    );

    // Like an NDJSON file, loading the same file again resumes from the checkpoint: no-op.
    local_ingest_docs_cli(parquet_args(&test_env, &index_id, &parquet_uri, false))
        .await
        .unwrap();
    assert_eq!(list_published_splits(&test_env).await.len(), splits.len());

    // Another file is loaded into the same index.
    let other_parquet_path = test_env.data_dir_path.join("other-docs.parquet");
    write_json_docs_as_parquet_file(&other_parquet_path, &json_docs, ROW_GROUP_NUM_ROWS).unwrap();
    let other_parquet_uri: Uri = uri_from_path(&other_parquet_path);
    local_ingest_docs_cli(parquet_args(
        &test_env,
        &index_id,
        &other_parquet_uri,
        false,
    ))
    .await
    .unwrap();
    let num_docs: usize = list_published_splits(&test_env)
        .await
        .iter()
        .map(|split| split.num_docs)
        .sum();
    assert_eq!(num_docs, 2 * NUM_ROWS);

    // A missing timestamp must fail the load.
    let mut invalid_json_docs = json_docs;
    invalid_json_docs[500] = json!({"event": "no-timestamp"});
    let invalid_parquet_path = test_env.data_dir_path.join("invalid-docs.parquet");
    write_json_docs_as_parquet_file(
        &invalid_parquet_path,
        &invalid_json_docs,
        ROW_GROUP_NUM_ROWS,
    )
    .unwrap();
    let invalid_parquet_uri: Uri = uri_from_path(&invalid_parquet_path);
    let error = local_ingest_docs_cli(parquet_args(
        &test_env,
        &index_id,
        &invalid_parquet_uri,
        false,
    ))
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("failed to ingest all the documents"),
        "{error:#}"
    );
}

#[tokio::test]
async fn test_local_ingest_parquet_merges_splits() {
    // Each load produces one immature split. The second load merges it with the first one.
    let (test_env, index_id, json_docs, parquet_uri) = setup_parquet_index(
        10_000_000,
        "{type: stable_log, merge_factor: 2, max_merge_factor: 2, min_level_num_docs: 100000}",
    )
    .await;
    let other_parquet_path = test_env.data_dir_path.join("other-docs.parquet");
    write_json_docs_as_parquet_file(&other_parquet_path, &json_docs, ROW_GROUP_NUM_ROWS).unwrap();
    let other_parquet_uri: Uri = uri_from_path(&other_parquet_path);
    for uri in [&parquet_uri, &other_parquet_uri] {
        let mut args = parquet_args(&test_env, &index_id, uri, false);
        args.num_pipelines = NonZeroUsize::MIN;
        local_ingest_docs_cli(args).await.unwrap();
    }

    let splits = list_published_splits(&test_env).await;
    let num_docs: usize = splits.iter().map(|split| split.num_docs).sum();
    assert_eq!(num_docs, 2 * NUM_ROWS);
    assert!(
        splits.iter().any(|split| split.num_merge_ops > 0),
        "expected at least one merged split: {splits:?}"
    );
}

#[tokio::test]
async fn test_local_ingest_parquet_fails_on_nonfinite_later_batch() {
    quickwit_common::setup_logging_for_tests();
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let index_id = append_random_suffix("test-parquet-nonfinite");
        let test_env = create_test_env(index_id.clone(), TestStorageType::LocalFileSystem)
            .await
            .unwrap();
        let config = json!({
            "version": "0.8",
            "index_id": index_id,
            "index_uri": test_env.index_uri.as_str(),
            "doc_mapping": {"field_mappings": [{"name": "value", "type": "f64"}]},
            "indexing_settings": {
                "split_num_docs_target": 100,
                "merge_policy": {"type": "no_merge"},
                "resources": {"heap_size": "50MB"}
            }
        });
        let index_config = load_index_config_from_user_config(
            ConfigFormat::Json,
            &serde_json::to_vec(&config).unwrap(),
            &test_env.index_uri,
        )
        .unwrap();
        let mut index_service = IndexService::new(
            test_env.metastore().await,
            test_env.storage_resolver.clone(),
        );
        index_service
            .create_index(index_config, false)
            .await
            .unwrap();
        let path = test_env.data_dir_path.join("nonfinite.parquet");
        let mut values = vec![1.0; NUM_ROWS];
        values.push(value);
        write_f64_values_as_parquet_file(&path, &values, values.len()).unwrap();
        let uri = uri_from_path(&path);
        // The preflight only checks the first row; the invalid value is decoded during the load.
        ParquetSharedReader::try_new(uri.clone(), &[]).unwrap();
        let mut args = parquet_args(&test_env, &index_id, &uri, false);
        args.num_pipelines = NonZeroUsize::MIN;
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            local_ingest_docs_cli(args),
        )
        .await
        .expect("non-finite input must not hang shutdown")
        .unwrap_err();
        assert!(
            error.to_string().contains("indexing pipeline failed"),
            "{error:#}"
        );
    }
}

fn parquet_args(
    test_env: &TestEnv,
    index_id: &str,
    parquet_uri: &Uri,
    overwrite: bool,
) -> LocalIngestDocsArgs {
    LocalIngestDocsArgs {
        config_uri: test_env.resource_files.config.clone(),
        index_id: index_id.to_string(),
        input_path_opt: Some(parquet_uri.clone()),
        input_format: SourceInputFormat::Json,
        overwrite,
        vrl_script: None,
        clear_cache: true,
        num_pipelines: NonZeroUsize::new(2).unwrap(),
        excluded_columns: Vec::new(),
    }
}

async fn list_published_splits(test_env: &TestEnv) -> Vec<SplitMetadata> {
    let index_uid = test_env.index_metadata().await.unwrap().index_uid;
    let query = ListSplitsQuery::for_index(index_uid).with_split_state(SplitState::Published);
    test_env
        .metastore()
        .await
        .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
        .await
        .unwrap()
        .collect_splits_metadata()
        .await
        .unwrap()
}
