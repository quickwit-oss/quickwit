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

#![recursion_limit = "256"]
#![allow(clippy::bool_assert_comparison)]

mod helpers;

use std::path::Path;

use anyhow::Result;
use clap::error::ErrorKind;
use helpers::{uri_from_path, TestEnv, TestStorageType};
use quickwit_cli::checklist::ChecklistError;
use quickwit_cli::cli::build_cli;
use quickwit_cli::index::{
    create_index_cli, delete_index_cli, search_index, update_index_cli, CreateIndexArgs,
    DeleteIndexArgs, SearchIndexArgs, UpdateIndexArgs,
};
use quickwit_cli::tool::{
    garbage_collect_index_cli, local_ingest_docs_cli, GarbageCollectIndexArgs, LocalIngestDocsArgs,
};
use quickwit_common::fs::get_cache_directory_path;
use quickwit_common::rand::append_random_suffix;
use quickwit_common::uri::Uri;
use quickwit_config::{RetentionPolicy, SourceInputFormat, CLI_SOURCE_ID};
use quickwit_metastore::{
    ListSplitsRequestExt, MetastoreResolver, MetastoreServiceExt, MetastoreServiceStreamSplitsExt,
    SplitMetadata, SplitState, StageSplitsRequestExt,
};
use quickwit_proto::metastore::{
    DeleteSplitsRequest, EntityKind, IndexMetadataRequest, ListSplitsRequest,
    MarkSplitsForDeletionRequest, MetastoreError, MetastoreService, StageSplitsRequest,
};
use serde_json::{json, Number, Value};
use tokio::time::{sleep, Duration};

use crate::helpers::{create_test_env, upload_test_file, PACKAGE_BIN_NAME};

async fn create_logs_index(test_env: &TestEnv) -> anyhow::Result<()> {
    let args = CreateIndexArgs {
        client_args: test_env.default_client_args(),
        index_config_uri: test_env.resource_files.index_config.clone(),
        overwrite: false,
        assume_yes: true,
    };
    create_index_cli(args).await
}

async fn local_ingest_docs(uri: Uri, test_env: &TestEnv) -> anyhow::Result<()> {
    let args = LocalIngestDocsArgs {
        config_uri: test_env.resource_files.config.clone(),
        index_id: test_env.index_id.clone(),
        input_path_opt: Some(uri),
        input_format: SourceInputFormat::Json,
        overwrite: false,
        clear_cache: true,
        vrl_script: None,
    };
    local_ingest_docs_cli(args).await
}

async fn local_ingest_log_docs(test_env: &TestEnv) -> anyhow::Result<()> {
    local_ingest_docs(test_env.resource_files.log_docs.clone(), test_env).await
}

#[test]
fn test_cmd_help() {
    let cmd = build_cli();
    let error = cmd
        .try_get_matches_from(vec![PACKAGE_BIN_NAME, "--help"])
        .unwrap_err();
    // on `--help` clap returns an error.
    assert_eq!(error.kind(), ErrorKind::DisplayHelp);
}

#[tokio::test]
async fn test_cmd_create() {
    quickwit_common::setup_logging_for_tests();
    let index_id = append_random_suffix("test-create-cmd");
    let test_env = create_test_env(index_id, TestStorageType::LocalFileSystem)
        .await
        .unwrap();
    test_env.start_server().await.unwrap();
    create_logs_index(&test_env).await.unwrap();

    let index_metadata = test_env.index_metadata().await.unwrap();
    assert_eq!(index_metadata.index_id(), test_env.index_id);

    // Creating an existing index should fail.
    let error = create_logs_index(&test_env).await.unwrap_err();
    assert!(error.to_string().contains("already exist(s)"),);
}

#[tokio::test]
async fn test_cmd_create_no_index_uri() {
    quickwit_common::setup_logging_for_tests();
    let index_id = append_random_suffix("test-create-cmd-no-index-uri");
    let test_env = create_test_env(index_id, TestStorageType::LocalFileSystem)
        .await
        .unwrap();
    test_env.start_server().await.unwrap();

    let index_config_without_uri = test_env.resource_files.index_config_without_uri.clone();
    let args = CreateIndexArgs {
        client_args: test_env.default_client_args(),
        index_config_uri: index_config_without_uri,
        overwrite: false,
        assume_yes: true,
    };

    let response = create_index_cli(args).await;
    response.unwrap();

    let index_metadata = test_env.index_metadata().await.unwrap();
    assert_eq!(index_metadata.index_id(), test_env.index_id);
    assert_eq!(index_metadata.index_uri(), &test_env.index_uri);
}

#[tokio::test]
async fn test_cmd_create_overwrite() {
    // Create non existing index with --overwrite.
    let index_id = append_random_suffix("test-create-non-existing-index-with-overwrite");
    let test_env = create_test_env(index_id, TestStorageType::LocalFileSystem)
        .await
        .unwrap();
    test_env.start_server().await.unwrap();

    let index_config_without_uri = test_env.resource_files.index_config_without_uri.clone();
    let args = CreateIndexArgs {
        client_args: test_env.default_client_args(),
        index_config_uri: index_config_without_uri,
        overwrite: true,
        assume_yes: true,
    };

    create_index_cli(args).await.unwrap();

    let index_metadata = test_env.index_metadata().await.unwrap();
    assert_eq!(index_metadata.index_id(), &test_env.index_id);
    assert_eq!(index_metadata.index_uri(), &test_env.index_uri);
}

#[test]
fn test_cmd_create_with_ill_formed_command() {
    // Attempt to create with ill-formed new command.
    let app = build_cli();
    let error = app
        .try_get_matches_from(vec![PACKAGE_BIN_NAME, "index", "create"])
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
}

#[tokio::test]
async fn test_cmd_ingest_on_non_existing_index() {
    let index_id = append_random_suffix("index-does-not-exist");
    let test_env = create_test_env(index_id, TestStorageType::LocalFileSystem)
        .await
        .unwrap();

    let args = LocalIngestDocsArgs {
        config_uri: test_env.resource_files.config,
        index_id: "index-does-not-exist".to_string(),
        input_path_opt: Some(test_env.resource_files.log_docs.clone()),
        input_format: SourceInputFormat::Json,
        overwrite: false,
        clear_cache: true,
        vrl_script: None,
    };

    let error = local_ingest_docs_cli(args).await.unwrap_err();

    assert_eq!(
        error.root_cause().downcast_ref::<MetastoreError>().unwrap(),
        &MetastoreError::NotFound(EntityKind::Index {
            index_id: "index-does-not-exist".to_string()
        })
    );
}

#[tokio::test]
async fn test_ingest_docs_cli_keep_cache() {
    quickwit_common::setup_logging_for_tests();
    let index_id = append_random_suffix("test-index-keep-cache");
    let test_env = create_test_env(index_id.clone(), TestStorageType::LocalFileSystem)
        .await
        .unwrap();
    test_env.start_server().await.unwrap();
    create_logs_index(&test_env).await.unwrap();

    let args = LocalIngestDocsArgs {
        config_uri: test_env.resource_files.config,
        index_id,
        input_path_opt: Some(test_env.resource_files.log_docs.clone()),
        input_format: SourceInputFormat::Json,
        overwrite: false,
        clear_cache: false,
        vrl_script: None,
    };

    local_ingest_docs_cli(args).await.unwrap();
    // Ensure cache directory is not empty.
    let cache_directory_path = get_cache_directory_path(&test_env.data_dir_path);
    assert!(cache_directory_path.read_dir().unwrap().next().is_some());
}

#[tokio::test]
async fn test_ingest_docs_cli() {
    quickwit_common::setup_logging_for_tests();
    let index_id = append_random_suffix("test-index-simple");
    let test_env = create_test_env(index_id.clone(), TestStorageType::LocalFileSystem)
        .await
        .unwrap();
    test_env.start_server().await.unwrap();
    create_logs_index(&test_env).await.unwrap();
    let index_uid = test_env.index_metadata().await.unwrap().index_uid;

    let args = LocalIngestDocsArgs {
        config_uri: test_env.resource_files.config.clone(),
        index_id: index_id.clone(),
        input_path_opt: Some(test_env.resource_files.log_docs.clone()),
        input_format: SourceInputFormat::Json,
        overwrite: false,
        clear_cache: true,
        vrl_script: None,
    };

    local_ingest_docs_cli(args).await.unwrap();

    let splits_metadata: Vec<SplitMetadata> = test_env
        .metastore()
        .await
        .list_splits(ListSplitsRequest::try_from_index_uid(index_uid).unwrap())
        .await
        .unwrap()
        .collect_splits_metadata()
        .await
        .unwrap();

    assert_eq!(splits_metadata.len(), 1);
    assert_eq!(splits_metadata[0].num_docs, 5);

    // Ensure cache directory is empty.
    let cache_directory_path = get_cache_directory_path(&test_env.data_dir_path);
    assert!(cache_directory_path.read_dir().unwrap().next().is_none());

    let does_not_exist_uri = uri_from_path(&test_env.data_dir_path)
        .join("file-does-not-exist.json")
        .unwrap();

    // Ingest a non-existing file should fail.
    let args = LocalIngestDocsArgs {
        config_uri: test_env.resource_files.config,
        index_id: test_env.index_id,
        input_path_opt: Some(does_not_exist_uri),
        input_format: SourceInputFormat::Json,
        overwrite: false,
        clear_cache: true,
        vrl_script: None,
    };

    let error = local_ingest_docs_cli(args).await.unwrap_err();

    assert!(matches!(
        error.root_cause().downcast_ref::<ChecklistError>().unwrap(),
        ChecklistError {
            errors
        } if errors.len() == 1 && errors[0].0 == CLI_SOURCE_ID
    ));
}

#[tokio::test]
async fn test_reingest_same_file_cli() {
    quickwit_common::setup_logging_for_tests();
    let index_id = append_random_suffix("test-index-simple");
    let test_env = create_test_env(index_id.clone(), TestStorageType::LocalFileSystem)
        .await
        .unwrap();
    test_env.start_server().await.unwrap();
    create_logs_index(&test_env).await.unwrap();
    let index_uid = test_env.index_metadata().await.unwrap().index_uid;

    for _ in 0..2 {
        let args = LocalIngestDocsArgs {
            config_uri: test_env.resource_files.config.clone(),
            index_id: index_id.clone(),
            input_path_opt: Some(test_env.resource_files.log_docs.clone()),
            input_format: SourceInputFormat::Json,
            overwrite: false,
            clear_cache: true,
            vrl_script: None,
        };

        local_ingest_docs_cli(args).await.unwrap();
    }

    let splits_metadata: Vec<SplitMetadata> = test_env
        .metastore()
        .await
        .list_splits(ListSplitsRequest::try_from_index_uid(index_uid).unwrap())
        .await
        .unwrap()
        .collect_splits_metadata()
        .await
        .unwrap();

    assert_eq!(splits_metadata.len(), 1);
    assert_eq!(splits_metadata[0].num_docs, 5);
}

/// Helper function to compare a json payload.
///
/// It will serialize and deserialize the value in order
/// to make sure floating points are the exact value obtained via
/// JSON deserialization.
#[track_caller]
fn assert_flexible_json_eq(value_json: &serde_json::Value, expected_json: &serde_json::Value) {
    match (value_json, expected_json) {
        (Value::Array(left_arr), Value::Array(right_arr)) => {
            assert_eq!(
                left_arr.len(),
                right_arr.len(),
                "left: {left_arr:?} right: {right_arr:?}"
            );
            for i in 0..left_arr.len() {
                assert_flexible_json_eq(&left_arr[i], &right_arr[i]);
            }
        }
        (Value::Object(left_obj), Value::Object(right_obj)) => {
            assert_eq!(
                left_obj.len(),
                right_obj.len(),
                "left: {left_obj:?} right: {right_obj:?}"
            );
            for (k, v) in left_obj {
                if let Some(right_value) = right_obj.get(k) {
                    assert_flexible_json_eq(v, right_value);
                } else {
                    panic!("Missing key `{k}`");
                }
            }
        }
        (Value::Number(left_num), Value::Number(right_num)) => {
            let left = left_num.as_f64().unwrap();
            let right = right_num.as_f64().unwrap();
            assert!(
                (left - right).abs() / (1e-32 + left + right).abs() < 1e-4,
                "left: {left:?} right: {right:?}"
            );
        }
        (left, right) => {
            assert_eq!(left, right);
        }
    }
}

#[tokio::test]
async fn test_cmd_search_aggregation() {
    quickwit_common::setup_logging_for_tests();
    let index_id = append_random_suffix("test-search-cmd");
    let test_env = create_test_env(index_id, TestStorageType::LocalFileSystem)
        .await
        .unwrap();
    test_env.start_server().await.unwrap();
    create_logs_index(&test_env).await.unwrap();

    local_ingest_log_docs(&test_env).await.unwrap();

    let aggregation: Value = json!(
    {
      "range_buckets": {
        "range": {
          "field": "ts",
          "ranges": [
            { "to": 72057597000000000f64 },
            { "from": 72057597000000000f64, "to": 72057600000000000f64 },
            { "from": 72057600000000000f64, "to": 72057604000000000f64 },
            { "from": 72057604000000000f64 },
          ]
        },
        "aggs": {
          "average_ts": {
            "avg": { "field": "ts" }
          }
        }
      }
    });

    // search with aggregation
    let args = SearchIndexArgs {
        index_id: test_env.index_id.clone(),
        query: "paris OR tokio OR london".to_string(),
        aggregation: Some(serde_json::to_string(&aggregation).unwrap()),
        max_hits: 10,
        start_offset: 0,
        search_fields: Some(vec!["city".to_string()]),
        snippet_fields: None,
        start_timestamp: None,
        end_timestamp: None,
        client_args: test_env.default_client_args(),
        sort_by_score: false,
    };
    let search_response = search_index(args).await.unwrap();

    let aggregation_res = search_response.aggregations.unwrap();
    let expected_json = serde_json::json!({
        "range_buckets": {
            "buckets": [
                {
                    "average_ts": {
                        "value": null
                    },
                    "doc_count": 0,
                    "key": "*-1972-04-13T23:59:57Z",
                    "to": 72057597000000000f64,
                    "to_as_string": "1972-04-13T23:59:57Z"
                },
                {
                    "average_ts": {
                        "value": 72057597500000000f64
                    },
                    "doc_count": 2,
                    "from": 72057597000000000f64,
                    "from_as_string": "1972-04-13T23:59:57Z",
                    "key": "1972-04-13T23:59:57Z-1972-04-14T00:00:00Z",
                    "to": 72057600000000000f64,
                    "to_as_string": "1972-04-14T00:00:00Z"
                },
                {
                    "average_ts": {
                        "value": null
                    },
                    "doc_count": 0,
                    "from": 72057600000000000f64,
                    "from_as_string": "1972-04-14T00:00:00Z",
                    "key": "1972-04-14T00:00:00Z-1972-04-14T00:00:04Z",
                    "to": 72057604000000000f64,
                    "to_as_string": "1972-04-14T00:00:04Z"
                },
                {
                    "average_ts": {
                        "value": 72057606333333330f64
                    },
                    "doc_count": 3,
                    "from": 72057604000000000f64,
                    "from_as_string": "1972-04-14T00:00:04Z",
                    "key": "1972-04-14T00:00:04Z-*"
                }
            ]
        }
    });
    assert_flexible_json_eq(&aggregation_res, &expected_json);
}

#[tokio::test]
async fn test_cmd_search_with_snippets() -> Result<()> {
    quickwit_common::setup_logging_for_tests();
    let index_id = append_random_suffix("test-search-cmd");
    let test_env = create_test_env(index_id, TestStorageType::LocalFileSystem)
        .await
        .unwrap();
    test_env.start_server().await.unwrap();
    create_logs_index(&test_env).await.unwrap();

    local_ingest_log_docs(&test_env).await.unwrap();

    // search with snippets
    let args = SearchIndexArgs {
        index_id: test_env.index_id.clone(),
        query: "event:baz".to_string(),
        aggregation: None,
        max_hits: 10,
        start_offset: 0,
        search_fields: None,
        snippet_fields: Some(vec!["event".to_string()]),
        start_timestamp: None,
        end_timestamp: None,
        client_args: test_env.default_client_args(),
        sort_by_score: false,
    };
    let search_response = search_index(args).await.unwrap();
    assert_eq!(search_response.hits.len(), 1);
    let hit = &search_response.hits[0];
    assert_eq!(hit, &json!({"event": "baz", "ts": 72057604}));
    assert_eq!(
        search_response.snippets.unwrap()[0],
        json!({
            "event": [ "<b>baz</b>"]
        })
    );
    Ok(())
}

#[tokio::test]
async fn test_search_index_cli() {
    quickwit_common::setup_logging_for_tests();
    let index_id = append_random_suffix("test-search-cmd");
    let test_env = create_test_env(index_id.clone(), TestStorageType::LocalFileSystem)
        .await
        .unwrap();
    test_env.start_server().await.unwrap();
    create_logs_index(&test_env).await.unwrap();

    let create_search_args = |query: &str| SearchIndexArgs {
        client_args: test_env.default_client_args(),
        index_id: index_id.clone(),
        query: query.to_string(),
        aggregation: None,
        max_hits: 20,
        start_offset: 0,
        search_fields: None,
        snippet_fields: None,
        start_timestamp: None,
        end_timestamp: None,
        sort_by_score: false,
    };

    local_ingest_log_docs(&test_env).await.unwrap();

    let args = create_search_args("level:info");

    // search_index_cli calls search_index and prints the SearchResponse
    let search_res = search_index(args).await.unwrap();
    assert_eq!(search_res.num_hits, 2);

    // search with tag pruning
    let args = create_search_args("+level:info +city:paris");

    // search_index_cli calls search_index and prints the SearchResponse
    let search_res = search_index(args).await.unwrap();
    assert_eq!(search_res.num_hits, 1);

    // search with tag pruning
    let args = create_search_args("level:info AND city:conakry");

    // search_index_cli calls search_index and prints the SearchResponse
    let search_res = search_index(args).await.unwrap();
    assert_eq!(search_res.num_hits, 0);
}

#[tokio::test]
async fn test_cmd_update_index() {
    quickwit_common::setup_logging_for_tests();
    let index_id = append_random_suffix("test-update-cmd");
    let test_env = create_test_env(index_id.clone(), TestStorageType::LocalFileSystem)
        .await
        .unwrap();
    test_env.start_server().await.unwrap();
    create_logs_index(&test_env).await.unwrap();

    // add retention policy
    let args = UpdateIndexArgs {
        client_args: test_env.default_client_args(),
        index_id: index_id.clone(),
        index_config_uri: test_env.resource_files.index_config_with_retention.clone(),
        assume_yes: true,
    };
    update_index_cli(args).await.unwrap();
    let index_metadata = test_env.index_metadata().await.unwrap();
    assert_eq!(index_metadata.index_id(), test_env.index_id);
    assert_eq!(
        index_metadata.index_config.retention_policy_opt,
        Some(RetentionPolicy {
            retention_period: String::from("1 week"),
            evaluation_schedule: String::from("daily"),
            jitter_secs: None,
        })
    );

    // remove retention policy
    let args = UpdateIndexArgs {
        client_args: test_env.default_client_args(),
        index_id,
        index_config_uri: test_env.resource_files.index_config.clone(),
        assume_yes: true,
    };
    update_index_cli(args).await.unwrap();
    let index_metadata = test_env.index_metadata().await.unwrap();
    assert_eq!(index_metadata.index_id(), test_env.index_id);
    assert_eq!(index_metadata.index_config.retention_policy_opt, None);
}

#[tokio::test]
async fn test_delete_index_cli_dry_run() {
    quickwit_common::setup_logging_for_tests();
    let index_id = append_random_suffix("test-delete-cmd--dry-run");
    let test_env = create_test_env(index_id.clone(), TestStorageType::LocalFileSystem)
        .await
        .unwrap();
    test_env.start_server().await.unwrap();
    create_logs_index(&test_env).await.unwrap();

    let refresh_metastore = |metastore| async {
        // In this test we rely on the file backed metastore
        // and the file backed metastore caches results.
        // Therefore we need to force reading the disk to fetch updates.
        //
        // We do that by dropping and recreating our metastore.
        drop(metastore);
        MetastoreResolver::unconfigured()
            .resolve(&test_env.metastore_uri)
            .await
    };

    let create_delete_args = |dry_run| DeleteIndexArgs {
        client_args: test_env.default_client_args(),
        index_id: index_id.clone(),
        dry_run,
        assume_yes: true,
    };

    let mut metastore = MetastoreResolver::unconfigured()
        .resolve(&test_env.metastore_uri)
        .await
        .unwrap();

    assert!(metastore.index_exists(&index_id).await.unwrap());
    // On empty index.
    let args = create_delete_args(true);

    delete_index_cli(args).await.unwrap();
    // On dry run index should still exist
    let mut metastore = refresh_metastore(metastore).await.unwrap();
    metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap();
    assert!(metastore.index_exists(&index_id).await.unwrap());

    local_ingest_log_docs(&test_env).await.unwrap();

    // On non-empty index
    let args = create_delete_args(true);

    delete_index_cli(args).await.unwrap();
    // On dry run index should still exist
    let mut metastore = refresh_metastore(metastore).await.unwrap();
    metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap();
    assert!(metastore.index_exists(&index_id).await.unwrap());
}

#[tokio::test]
async fn test_delete_index_cli() {
    let index_id = append_random_suffix("test-delete-cmd");
    let test_env = create_test_env(index_id.clone(), TestStorageType::LocalFileSystem)
        .await
        .unwrap();
    test_env.start_server().await.unwrap();
    create_logs_index(&test_env).await.unwrap();

    local_ingest_log_docs(&test_env).await.unwrap();

    let args = DeleteIndexArgs {
        client_args: test_env.default_client_args(),
        index_id: index_id.clone(),
        assume_yes: true,
        dry_run: false,
    };

    delete_index_cli(args).await.unwrap();

    assert!(test_env.index_metadata().await.is_err());
}

#[tokio::test]
async fn test_garbage_collect_cli_no_grace() {
    quickwit_common::setup_logging_for_tests();
    let index_id = append_random_suffix("test-gc-cmd--no-grace-period");
    let test_env = create_test_env(index_id.clone(), TestStorageType::LocalFileSystem)
        .await
        .unwrap();
    test_env.start_server().await.unwrap();
    create_logs_index(&test_env).await.unwrap();
    let index_uid = test_env.index_metadata().await.unwrap().index_uid;
    local_ingest_log_docs(&test_env).await.unwrap();

    let metastore = MetastoreResolver::unconfigured()
        .resolve(&test_env.metastore_uri)
        .await
        .unwrap();

    let refresh_metastore = |metastore| async {
        // In this test we rely on the file backed metastore and write on
        // a different process. The file backed metastore caches results.
        // Therefore we need to force reading the disk.
        //
        // We do that by dropping and recreating our metastore.
        drop(metastore);
        MetastoreResolver::unconfigured()
            .resolve(&test_env.metastore_uri)
            .await
    };

    let create_gc_args = |dry_run| GarbageCollectIndexArgs {
        config_uri: test_env.resource_files.config.clone(),
        index_id: index_id.clone(),
        grace_period: Duration::from_secs(3600),
        dry_run,
    };

    let splits_metadata = metastore
        .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
        .await
        .unwrap()
        .collect_splits_metadata()
        .await
        .unwrap();
    assert_eq!(splits_metadata.len(), 1);

    let args = create_gc_args(false);

    garbage_collect_index_cli(args).await.unwrap();

    // On gc splits within grace period should still exist.
    let index_path = test_env.indexes_dir_path.join(&test_env.index_id);
    assert_eq!(index_path.try_exists().unwrap(), true);

    let split_ids = vec![splits_metadata[0].split_id().to_string()];
    let metastore = refresh_metastore(metastore).await.unwrap();
    let mark_for_deletion_request =
        MarkSplitsForDeletionRequest::new(index_uid.clone(), split_ids.clone());
    metastore
        .mark_splits_for_deletion(mark_for_deletion_request)
        .await
        .unwrap();

    let args = create_gc_args(true);

    garbage_collect_index_cli(args).await.unwrap();

    // On `dry_run = true` splits `MarkedForDeletion` should still exist.
    for split_id in split_ids.iter() {
        let split_file = quickwit_common::split_file(split_id);
        let split_filepath = index_path.join(split_file);
        assert_eq!(split_filepath.try_exists().unwrap(), true);
    }

    let args = create_gc_args(false);

    garbage_collect_index_cli(args).await.unwrap();

    // If split is `MarkedForDeletion` it should be deleted after gc run
    for split_id in split_ids.iter() {
        let split_file = quickwit_common::split_file(split_id);
        let split_filepath = index_path.join(split_file);
        assert_eq!(split_filepath.try_exists().unwrap(), false);
    }

    let metastore = refresh_metastore(metastore).await.unwrap();
    assert_eq!(
        metastore
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid).unwrap())
            .await
            .unwrap()
            .collect_splits_metadata()
            .await
            .unwrap()
            .len(),
        0
    );

    let args = DeleteIndexArgs {
        client_args: test_env.default_client_args(),
        index_id,
        dry_run: false,
        assume_yes: true,
    };

    delete_index_cli(args).await.unwrap();

    assert_eq!(index_path.try_exists().unwrap(), false);
}

#[tokio::test]
async fn test_garbage_collect_index_cli() {
    let index_id = append_random_suffix("test-gc-cmd");
    let test_env = create_test_env(index_id.clone(), TestStorageType::LocalFileSystem)
        .await
        .unwrap();
    test_env.start_server().await.unwrap();
    create_logs_index(&test_env).await.unwrap();
    let index_uid = test_env.index_metadata().await.unwrap().index_uid;
    local_ingest_log_docs(&test_env).await.unwrap();

    let refresh_metastore = |metastore| async {
        // In this test we rely on the file backed metastore and
        // modify it but the file backed metastore caches results.
        // Therefore we need to force reading the disk to update split info.
        //
        // We do that by dropping and recreating our metastore.
        drop(metastore);
        MetastoreResolver::unconfigured()
            .resolve(&test_env.metastore_uri)
            .await
    };

    let create_gc_args = |grace_period_secs| GarbageCollectIndexArgs {
        config_uri: test_env.resource_files.config.clone(),
        index_id: index_id.clone(),
        grace_period: Duration::from_secs(grace_period_secs),
        dry_run: false,
    };

    let metastore = MetastoreResolver::unconfigured()
        .resolve(&test_env.metastore_uri)
        .await
        .unwrap();

    let splits_metadata = metastore
        .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
        .await
        .unwrap()
        .collect_splits_metadata()
        .await
        .unwrap();
    assert_eq!(splits_metadata.len(), 1);

    let index_path = test_env.indexes_dir_path.join(&test_env.index_id);
    let split_filename = quickwit_common::split_file(splits_metadata[0].split_id.as_str());
    let split_path = index_path.join(&split_filename);
    assert_eq!(split_path.try_exists().unwrap(), true);

    let args = create_gc_args(3600);

    garbage_collect_index_cli(args).await.unwrap();

    // Split should still exists within grace period.
    let metastore = refresh_metastore(metastore).await.unwrap();
    let splits_metadata = metastore
        .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
        .await
        .unwrap()
        .collect_splits_metadata()
        .await
        .unwrap();
    assert_eq!(splits_metadata.len(), 1);

    // The following steps help turn an existing published split into a staged one
    // without deleting the files.
    let split_metadata = splits_metadata[0].clone();
    metastore
        .mark_splits_for_deletion(MarkSplitsForDeletionRequest::new(
            index_uid.clone(),
            vec![split_metadata.split_id.to_string()],
        ))
        .await
        .unwrap();
    metastore
        .delete_splits(DeleteSplitsRequest {
            index_uid: Some(index_uid.clone()),
            split_ids: splits_metadata
                .into_iter()
                .map(|split_metadata| split_metadata.split_id)
                .collect(),
        })
        .await
        .unwrap();
    metastore
        .stage_splits(
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata)
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(split_path.try_exists().unwrap(), true);

    let metastore = refresh_metastore(metastore).await.unwrap();
    let splits = metastore
        .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
        .await
        .unwrap()
        .collect_splits()
        .await
        .unwrap();
    assert_eq!(splits[0].split_state, SplitState::Staged);

    let args = create_gc_args(3600);

    garbage_collect_index_cli(args).await.unwrap();

    assert_eq!(split_path.try_exists().unwrap(), true);
    // Staged splits should still exist within grace period.
    let metastore = refresh_metastore(metastore).await.unwrap();
    let splits = metastore
        .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
        .await
        .unwrap()
        .collect_splits()
        .await
        .unwrap();
    assert_eq!(splits.len(), 1);
    assert_eq!(splits[0].split_state, SplitState::Staged);

    // Wait for grace period.
    // TODO: edit split update timestamps and remove this sleep.
    sleep(Duration::from_secs(2)).await;

    let args = create_gc_args(1);

    garbage_collect_index_cli(args).await.unwrap();

    let metastore = refresh_metastore(metastore).await.unwrap();
    let splits = metastore
        .list_splits(ListSplitsRequest::try_from_index_uid(index_uid).unwrap())
        .await
        .unwrap()
        .collect_splits()
        .await
        .unwrap();
    // Splits should be deleted from both metastore and file system.
    assert_eq!(splits.len(), 0);
    assert_eq!(split_path.try_exists().unwrap(), false);
}

/// testing the api via cli commands
#[tokio::test]
async fn test_all_local_index() {
    quickwit_common::setup_logging_for_tests();
    let index_id = append_random_suffix("test-all");
    let test_env = create_test_env(index_id.clone(), TestStorageType::LocalFileSystem)
        .await
        .unwrap();
    test_env.start_server().await.unwrap();
    create_logs_index(&test_env).await.unwrap();

    let metadata_file_exists = test_env
        .storage
        .exists(&Path::new(&test_env.index_id).join("metastore.json"))
        .await
        .unwrap();
    assert!(metadata_file_exists);

    local_ingest_log_docs(&test_env).await.unwrap();

    let query_response = reqwest::get(format!(
        "http://127.0.0.1:{}/api/v1/{}/search?query=level:info",
        test_env.rest_listen_port, test_env.index_id
    ))
    .await
    .unwrap()
    .text()
    .await
    .unwrap();

    let result: Value = serde_json::from_str(&query_response).unwrap();
    assert_eq!(result["num_hits"], Value::Number(Number::from(2i64)));

    let search_stream_response = reqwest::get(format!(
        "http://127.0.0.1:{}/api/v1/{}/search/stream?query=level:info&output_format=csv&fast_field=ts",
        test_env.rest_listen_port,
        test_env.index_id
    ))
    .await
    .unwrap()
    .text()
    .await
    .unwrap();
    assert_eq!(search_stream_response, "72057597000000\n72057608000000\n");

    let args = DeleteIndexArgs {
        client_args: test_env.default_client_args(),
        index_id,
        dry_run: false,
        assume_yes: true,
    };
    delete_index_cli(args).await.unwrap();

    let metadata_file_exists = test_env
        .storage
        .exists(&Path::new(&test_env.index_id).join("metastore.json"))
        .await
        .unwrap();
    assert_eq!(metadata_file_exists, false);
}

/// testing the api via cli commands
#[tokio::test]
#[cfg_attr(not(feature = "ci-test"), ignore)]
async fn test_all_with_s3_localstack_cli() {
    let index_id = append_random_suffix("test-all--cli-s3-localstack");
    let test_env = create_test_env(index_id.clone(), TestStorageType::S3)
        .await
        .unwrap();
    test_env.start_server().await.unwrap();
    create_logs_index(&test_env).await.unwrap();

    let s3_uri = upload_test_file(
        test_env.storage_resolver.clone(),
        test_env
            .resource_files
            .log_docs
            .filepath()
            .unwrap()
            .to_path_buf(),
        "quickwit-integration-tests",
        "sources/",
        &append_random_suffix("test-all--cli-s3-localstack"),
    )
    .await;

    local_ingest_docs(s3_uri, &test_env).await.unwrap();

    // Cli search
    let args = SearchIndexArgs {
        client_args: test_env.default_client_args(),
        index_id: index_id.clone(),
        query: "level:info".to_string(),
        aggregation: None,
        max_hits: 20,
        start_offset: 0,
        search_fields: None,
        snippet_fields: None,
        start_timestamp: None,
        end_timestamp: None,
        sort_by_score: false,
    };

    let search_res = search_index(args).await.unwrap();
    assert_eq!(search_res.num_hits, 2);

    let query_response = reqwest::get(format!(
        "http://127.0.0.1:{}/api/v1/{}/search?query=level:info",
        test_env.rest_listen_port, test_env.index_id,
    ))
    .await
    .unwrap()
    .text()
    .await
    .unwrap();

    let result: Value = serde_json::from_str(&query_response).unwrap();
    assert_eq!(result["num_hits"], Value::Number(Number::from(2i64)));

    let args = DeleteIndexArgs {
        client_args: test_env.default_client_args(),
        index_id: index_id.clone(),
        dry_run: false,
        assume_yes: true,
    };

    delete_index_cli(args).await.unwrap();

    assert_eq!(
        test_env
            .storage
            .exists(Path::new(&test_env.index_id))
            .await
            .unwrap(),
        false
    );
}
