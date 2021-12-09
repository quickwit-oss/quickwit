// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

#![allow(clippy::bool_assert_comparison)]

mod helpers;

use std::io::Read;
use std::path::Path;
use std::str::from_utf8;

use anyhow::Result;
use helpers::{TestEnv, TestStorageType};
use predicates::prelude::*;
use quickwit_cli::index::{create_index_cli, CreateIndexArgs};
use quickwit_common::rand::append_random_suffix;
use quickwit_metastore::{Metastore, MetastoreUriResolver};
use serde_json::{Number, Value};
use serial_test::serial;
use tokio::time::{sleep, Duration};

use crate::helpers::{create_test_env, make_command, spawn_command};

fn create_logs_index(test_env: &TestEnv) {
    make_command(
        format!(
            "index create --index-config-uri {} --metastore-uri {}",
            test_env.resource_files["index_config"].display(),
            test_env.metastore_uri,
        )
        .as_str(),
    )
    .assert()
    .success();
}

fn ingest_docs(index_id: &str, input_path: &Path, metastore_uri: &str, data_dir_path: &Path) {
    make_command(
        format!(
            "index ingest --index-id {} --input-path {} --metastore-uri {} --data-dir-path {}",
            index_id,
            input_path.display(),
            metastore_uri,
            data_dir_path.display(),
        )
        .as_str(),
    )
    .assert()
    .success()
    .stdout(predicate::str::contains("Indexed"))
    .stdout(predicate::str::contains("documents in"))
    .stdout(predicate::str::contains(
        "Now, you can query the index with",
    ));
}

#[test]
fn test_cmd_help() -> anyhow::Result<()> {
    let mut cmd = make_command("--help");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("USAGE"));
    Ok(())
}

#[tokio::test]
async fn test_cmd_create_fail() -> Result<()> {
    let index_id = append_random_suffix("test-create-cmd");
    let test_env = create_test_env(index_id, TestStorageType::LocalFileSystem)?;
    create_logs_index(&test_env);
    let index_metadata = test_env
        .metastore()
        .index_metadata(&test_env.index_id)
        .await
        .unwrap();
    assert_eq!(index_metadata.index_id, test_env.index_id);

    // Attempt to create with ill-formed new command.
    make_command("index create")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "--index-config-uri <INDEX CONFIG URI>",
        ));
    Ok(())
}

#[test]
fn test_cmd_create_on_existing_index() -> Result<()> {
    let index_id = append_random_suffix("test-create-cmd--index-already-exists");
    let test_env = create_test_env(index_id, TestStorageType::LocalFileSystem)?;
    create_logs_index(&test_env);

    make_command(
        format!(
            "index create --index-config-uri {} --metastore-uri {}",
            test_env.resource_files["index_config"].display(),
            test_env.metastore_uri,
        )
        .as_str(),
    )
    .assert()
    .failure()
    .stderr(predicate::str::contains("already exists"));

    Ok(())
}

#[test]
fn test_cmd_ingest_on_non_existing_index() -> Result<()> {
    let index_id = append_random_suffix("index-does-not exist");
    let test_env = create_test_env(index_id, TestStorageType::LocalFileSystem)?;
    make_command(
        format!(
            "index ingest --index-id {} --input-path {} --metastore-uri {} --data-dir-path {}",
            "index-does-no-exist",
            test_env.resource_files["logs"].display(),
            test_env.metastore_uri,
            test_env.data_dir_path.display()
        )
        .as_str(),
    )
    .assert()
    .failure();
    // .stderr(predicate::str::contains("✖ index")); TODO: Re-enable.
    Ok(())
}

#[test]
fn test_cmd_ingest_on_non_existing_file() -> Result<()> {
    let index_id = append_random_suffix("test-new-cmd--file-does-not-exist");
    let test_env = create_test_env(index_id, TestStorageType::LocalFileSystem)?;
    create_logs_index(&test_env);
    make_command(
        format!(
            "index ingest --index-id {} --input-path {} --metastore-uri {} --data-dir-path {}",
            test_env.index_id,
            test_env
                .data_dir_path
                .join("file-does-not-exist.json")
                .display(),
            &test_env.metastore_uri,
            test_env.data_dir_path.display()
        )
        .as_str(),
    )
    .assert()
    .failure()
    .stderr(predicate::str::contains("✖ source"));
    Ok(())
}

#[test]
fn test_cmd_ingest_simple() -> Result<()> {
    let index_id = append_random_suffix("test-index-simple");
    let test_env = create_test_env(index_id, TestStorageType::LocalFileSystem)?;
    create_logs_index(&test_env);

    ingest_docs(
        &test_env.index_id,
        test_env.resource_files["logs"].as_path(),
        &test_env.metastore_uri,
        test_env.data_dir_path.as_path(),
    );

    // using piped input.
    // let log_path = test_env.resource_files["logs"].clone();
    // make_command(
    //     format!(
    //         "index ingest --index-id {} --metastore-uri {} --data-dir-path {}",
    //         test_env.index_id,
    //         test_env.metastore_uri,
    //         test_env.data_dir_path.display()
    //     )
    //     .as_str(),
    // )
    // .pipe_stdin(log_path)?
    // .assert()
    // .success()
    // .stdout(predicate::str::contains("Indexed"))
    // .stdout(predicate::str::contains("documents in"))
    // .stdout(predicate::str::contains(
    //     "You can now query your index with",
    // ));
    Ok(())
}

#[test]
fn test_cmd_search() -> Result<()> {
    let index_id = append_random_suffix("test-search-cmd");
    let test_env = create_test_env(index_id, TestStorageType::LocalFileSystem)?;
    create_logs_index(&test_env);

    ingest_docs(
        &test_env.index_id,
        test_env.resource_files["logs"].as_path(),
        &test_env.metastore_uri,
        test_env.data_dir_path.as_path(),
    );

    make_command(
        format!(
            "index search --index-id {} --metastore-uri {} --query level:info",
            test_env.index_id, test_env.metastore_uri,
        )
        .as_str(),
    )
    .assert()
    .success()
    .stdout(predicate::function(|output: &[u8]| {
        println!("{}", from_utf8(output).unwrap());
        let result: Value = serde_json::from_slice(output).unwrap();
        result["numHits"] == Value::Number(Number::from(2i64))
    }));

    // search with tags
    make_command(
        format!(
            "index search --index-id {} --metastore-uri {} --query level:info --tags city:paris \
             device:rpi",
            test_env.index_id, test_env.metastore_uri,
        )
        .as_str(),
    )
    .assert()
    .success()
    .stdout(predicate::function(|output: &[u8]| {
        let result: Value = serde_json::from_slice(output).unwrap();
        result["numHits"] == Value::Number(Number::from(2i64))
    }));

    make_command(
        format!(
            "index search --index-id {} --metastore-uri {} --query level:info --tags city:conakry",
            test_env.index_id, &test_env.metastore_uri,
        )
        .as_str(),
    )
    .assert()
    .success()
    .stdout(predicate::function(|output: &[u8]| {
        let result: Value = serde_json::from_slice(output).unwrap();
        result["numHits"] == Value::Number(Number::from(0i64))
    }));
    Ok(())
}

#[test]
fn test_cmd_delete_index_dry_run() -> Result<()> {
    let index_id = append_random_suffix("test-delete-cmd--dry-run");
    let test_env = create_test_env(index_id, TestStorageType::LocalFileSystem)?;
    create_logs_index(&test_env);

    // Empty index.
    make_command(
        format!(
            "index delete --index-id {} --metastore-uri {} --dry-run",
            test_env.index_id, test_env.metastore_uri
        )
        .as_str(),
    )
    .assert()
    .success()
    .stdout(predicate::str::contains("Only the index will be deleted"));

    ingest_docs(
        &test_env.index_id,
        test_env.resource_files["logs"].as_path(),
        &test_env.metastore_uri,
        test_env.data_dir_path.as_path(),
    );

    // Non-empty index
    make_command(
        format!(
            "index delete --index-id {} --metastore-uri {} --dry-run",
            test_env.index_id, test_env.metastore_uri
        )
        .as_str(),
    )
    .assert()
    .success()
    .stdout(predicate::str::contains(
        "The following files will be removed",
    ))
    .stdout(predicate::str::contains(".split"));

    Ok(())
}

#[tokio::test]
async fn test_cmd_delete() -> Result<()> {
    let index_id = append_random_suffix("test-delete-cmd");
    let test_env = create_test_env(index_id, TestStorageType::LocalFileSystem)?;
    create_logs_index(&test_env);

    ingest_docs(
        &test_env.index_id,
        test_env.resource_files["logs"].as_path(),
        &test_env.metastore_uri,
        test_env.data_dir_path.as_path(),
    );
    make_command(
        format!(
            "index gc --index-id {} --metastore-uri {}",
            test_env.index_id, test_env.metastore_uri
        )
        .as_str(),
    )
    .assert()
    .success()
    .stdout(predicate::str::contains(
        "No dangling files to garbage collect",
    ));

    make_command(
        format!(
            "index delete --index-id {} --metastore-uri {}",
            test_env.index_id, test_env.metastore_uri
        )
        .as_str(),
    )
    .assert()
    .success();
    assert!(test_env
        .metastore()
        .index_metadata(&test_env.index_id)
        .await
        .is_err(),);
    Ok(())
}

#[tokio::test]
async fn test_cmd_garbage_collect_no_grace() -> Result<()> {
    let index_id = append_random_suffix("test-gc-cmd--no-grace-period");
    let test_env = create_test_env(index_id, TestStorageType::LocalFileSystem)?;
    create_logs_index(&test_env);
    ingest_docs(
        &test_env.index_id,
        test_env.resource_files["logs"].as_path(),
        &test_env.metastore_uri,
        test_env.data_dir_path.as_path(),
    );

    let metastore = MetastoreUriResolver::default()
        .resolve(&test_env.metastore_uri)
        .await?;
    let splits = metastore.list_all_splits(&test_env.index_id).await?;
    assert_eq!(splits.len(), 1);
    make_command(
        format!(
            "index gc --index-id {} --metastore-uri {}",
            test_env.index_id, test_env.metastore_uri
        )
        .as_str(),
    )
    .assert()
    .success()
    .stdout(predicate::str::contains(
        "No dangling files to garbage collect",
    ));

    let index_path = test_env.indexes_dir_path.join(&test_env.index_id);
    assert_eq!(index_path.exists(), true);

    let split_ids = [splits[0].split_id()];
    metastore
        .mark_splits_for_deletion(&test_env.index_id, &split_ids)
        .await?;
    make_command(
        format!(
            "index gc --index-id {} --metastore-uri {} --dry-run --grace-period 10m",
            test_env.index_id, test_env.metastore_uri,
        )
        .as_str(),
    )
    .assert()
    .success()
    .stdout(predicate::str::contains(
        "The following files will be garbage collected.",
    ))
    .stdout(predicate::str::contains(".split"));

    for split_id in split_ids {
        let split_file = quickwit_common::split_file(split_id);
        let split_filepath = index_path.join(&split_file);
        assert_eq!(split_filepath.exists(), true);
    }

    make_command(
        format!(
            "index gc --index-id {} --metastore-uri {} --grace-period 10m",
            test_env.index_id, test_env.metastore_uri
        )
        .as_str(),
    )
    .assert()
    .success()
    .stdout(predicate::str::contains(format!(
        "Index `{}` successfully garbage collected",
        test_env.index_id
    )));

    for split_id in split_ids {
        let split_file = quickwit_common::split_file(split_id);
        let split_filepath = index_path.join(&split_file);
        assert_eq!(split_filepath.exists(), false);
    }

    let metastore = MetastoreUriResolver::default()
        .resolve(&test_env.metastore_uri)
        .await?;
    assert_eq!(
        metastore.list_all_splits(&test_env.index_id).await?.len(),
        0
    );

    make_command(
        format!(
            "index delete --index-id {} --metastore-uri {}",
            test_env.index_id, test_env.metastore_uri
        )
        .as_str(),
    )
    .assert()
    .success();
    assert_eq!(index_path.exists(), false);
    Ok(())
}

#[tokio::test]
async fn test_cmd_garbage_collect_spares_files_within_grace_period() -> Result<()> {
    let index_id = append_random_suffix("test-gc-cmd");
    let test_env = create_test_env(index_id, TestStorageType::LocalFileSystem)?;
    create_logs_index(&test_env);
    ingest_docs(
        &test_env.index_id,
        test_env.resource_files["logs"].as_path(),
        &test_env.metastore_uri,
        test_env.data_dir_path.as_path(),
    );

    let metastore = test_env.metastore();
    let splits = metastore.list_all_splits(&test_env.index_id).await?;
    assert_eq!(splits.len(), 1);
    make_command(
        format!(
            "index gc --index-id {} --metastore-uri {}",
            test_env.index_id, test_env.metastore_uri
        )
        .as_str(),
    )
    .assert()
    .success()
    .stdout(predicate::str::contains(
        "No dangling files to garbage collect",
    ));

    let index_path = test_env.indexes_dir_path.join(&test_env.index_id);
    let split_filename = quickwit_common::split_file(splits[0].split_metadata.split_id.as_str());
    let split_path = index_path.join(&split_filename);
    assert_eq!(split_path.exists(), true);

    // The following steps help turn an existing published split into a staged one
    // without deleting the files.
    let split = splits[0].clone();
    let split_ids = [split.split_metadata.split_id.as_str()];
    metastore
        .mark_splits_for_deletion(&test_env.index_id, &split_ids)
        .await?;
    metastore
        .delete_splits(&test_env.index_id, &split_ids)
        .await?;
    metastore
        .stage_split(&test_env.index_id, split.split_metadata)
        .await?;
    assert_eq!(split_path.exists(), true);

    make_command(
        format!(
            "index gc --index-id {} --metastore-uri {} --grace-period 2s",
            test_env.index_id, test_env.metastore_uri
        )
        .as_str(),
    )
    .assert()
    .success()
    .stdout(predicate::str::contains(
        "No dangling files to garbage collect",
    ));
    assert_eq!(split_path.exists(), true);

    // Wait for grace period.
    // TODO: edit split update timestamps and remove this sleep.
    sleep(Duration::from_secs(3)).await;
    make_command(
        format!(
            "index gc --index-id {} --metastore-uri {} --dry-run --grace-period 2s",
            test_env.index_id, test_env.metastore_uri,
        )
        .as_str(),
    )
    .assert()
    .success()
    .stdout(predicate::str::contains(
        "The following files will be garbage collected.",
    ))
    .stdout(predicate::str::contains(&split_filename));
    assert_eq!(split_path.exists(), true);

    make_command(
        format!(
            "index gc --index-id {} --metastore-uri {} --grace-period 2s",
            test_env.index_id, test_env.metastore_uri
        )
        .as_str(),
    )
    .assert()
    .success()
    .stdout(predicate::str::contains(format!(
        "Index `{}` successfully garbage collected",
        test_env.index_id
    )));
    assert_eq!(split_path.exists(), false);

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "ci-test"), ignore)]
async fn test_cmd_dry_run_delete_on_s3_localstack() -> Result<()> {
    let index_id = append_random_suffix("test-delete-cmd--s3-localstack");
    let test_env = create_test_env(index_id, TestStorageType::S3)?;
    make_command(
        format!(
            "index create --metastore-uri {} --index-config-uri {}",
            test_env.metastore_uri,
            test_env.resource_files["index_config"].display()
        )
        .as_str(),
    )
    .assert()
    .success();

    ingest_docs(
        &test_env.index_id,
        test_env.resource_files["logs"].as_path(),
        &test_env.metastore_uri,
        test_env.data_dir_path.as_path(),
    );

    make_command(
        format!(
            "index gc --index-id {} --metastore-uri {}",
            test_env.index_id, test_env.metastore_uri
        )
        .as_str(),
    )
    .assert()
    .success()
    .stdout(predicate::str::contains(
        "No dangling files to garbage collect",
    ));

    make_command(
        format!(
            "index delete --index-id {} --metastore-uri {} --dry-run",
            test_env.index_id, test_env.metastore_uri
        )
        .as_str(),
    )
    .assert()
    .success()
    .stdout(predicate::str::contains(
        "The following files will be removed",
    ))
    .stdout(predicate::str::contains(".split"));

    make_command(
        format!(
            "index delete --index-id {} --metastore-uri {}",
            test_env.index_id, test_env.metastore_uri
        )
        .as_str(),
    )
    .assert()
    .success();

    Ok(())
}

/// testing the api via cli commands
#[tokio::test]
#[serial]
async fn test_all_local_index() -> Result<()> {
    let index_id = append_random_suffix("test-all");
    let test_env = create_test_env(index_id, TestStorageType::LocalFileSystem)?;
    make_command(
        format!(
            "index create --metastore-uri {} --index-config-uri {}",
            test_env.metastore_uri,
            test_env.resource_files["index_config"].display()
        )
        .as_str(),
    )
    .assert()
    .success();

    let metadata_file_exists = test_env
        .storage
        .exists(&Path::new(&test_env.index_id).join("quickwit.json"))
        .await?;
    assert_eq!(metadata_file_exists, true);

    ingest_docs(
        &test_env.index_id,
        test_env.resource_files["logs"].as_path(),
        &test_env.metastore_uri,
        test_env.data_dir_path.as_path(),
    );

    // serve & api-search
    let mut server_process = spawn_command(
        format!(
            "service run searcher --server-config-uri {}",
            test_env.resource_files["server_config"].display(),
        )
        .as_str(),
    )
    .unwrap();
    // TODO: wait until port server accepts incoming connections and remove sleep.
    sleep(Duration::from_secs(2)).await;
    let mut process_output_str = String::new();
    let _ = server_process
        .stdout
        .as_mut()
        .expect("Failed to get server process output")
        .take(800)
        .read_to_string(&mut process_output_str)
        .expect("Cannot read output");
    assert!(process_output_str.contains("http://127.0.0.1:"));

    let query_response = reqwest::get(format!(
        "http://127.0.0.1:{}/api/v1/{}/search?query=level:info",
        test_env.searcher_rest_listen_port, test_env.index_id
    ))
    .await?
    .text()
    .await?;

    let result: Value =
        serde_json::from_str(&query_response).expect("Couldn't deserialize response.");
    assert_eq!(result["numHits"], Value::Number(Number::from(2i64)));

    let search_stream_response = reqwest::get(format!(
        "http://127.0.0.1:{}/api/v1/{}/search/stream?query=level:info&outputFormat=csv&fastField=ts",
        test_env.searcher_rest_listen_port,
        test_env.index_id
    ))
    .await?
    .text()
    .await?;
    assert_eq!(search_stream_response, "2\n13\n");

    server_process.kill().unwrap();

    make_command(
        format!(
            "index delete --index-id {} --metastore-uri {}",
            test_env.index_id, test_env.metastore_uri
        )
        .as_str(),
    )
    .assert()
    .success();
    let metadata_file_exists = test_env
        .storage
        .exists(&Path::new(&test_env.index_id).join("quickwit.json"))
        .await?;
    assert_eq!(metadata_file_exists, false);

    Ok(())
}

/// testing the api via cli commands
#[tokio::test]
#[serial]
#[cfg_attr(not(feature = "ci-test"), ignore)]
async fn test_all_with_s3_localstack_cli() -> Result<()> {
    let index_id = append_random_suffix("test-all--cli-s3-localstack");
    let test_env = create_test_env(index_id, TestStorageType::S3)?;
    make_command(
        format!(
            "index create --metastore-uri {} --index-config-uri {}",
            test_env.metastore_uri,
            test_env.resource_files["index_config"].display()
        )
        .as_str(),
    )
    .assert()
    .success();

    let index_metadata = test_env
        .metastore()
        .index_metadata(&test_env.index_id)
        .await;
    assert_eq!(index_metadata.is_ok(), true);

    ingest_docs(
        &test_env.index_id,
        test_env.resource_files["logs"].as_path(),
        &test_env.metastore_uri,
        test_env.data_dir_path.as_path(),
    );

    // cli search
    make_command(
        format!(
            "index search --index-id {} --metastore-uri {} --query level:info",
            test_env.index_id, test_env.metastore_uri,
        )
        .as_str(),
    )
    .assert()
    .success()
    .stdout(predicate::function(|output: &[u8]| {
        let result: Value = serde_json::from_slice(output).unwrap();
        result["numHits"] == Value::Number(Number::from(2i64))
    }));

    // serve & api-search
    // TODO: ditto.
    let mut server_process = spawn_command(
        format!(
            "service run searcher --server-config-uri {}",
            test_env.resource_files["server_config"].display(),
        )
        .as_str(),
    )
    .unwrap();
    // TODO: ditto.
    sleep(Duration::from_secs(2)).await;
    let mut data = vec![0; 600];
    server_process
        .stdout
        .as_mut()
        .expect("Failed to get server process output")
        .read_exact(&mut data)
        .expect("Cannot read output");
    let process_output_str = String::from_utf8(data).unwrap();
    let query_response = reqwest::get(format!(
        "http://127.0.0.1:{}/api/v1/{}/search?query=level:info",
        test_env.searcher_rest_listen_port, test_env.index_id,
    ))
    .await?
    .text()
    .await?;
    server_process.kill().unwrap();

    assert!(process_output_str.contains("http://127.0.0.1:"));
    let result: Value =
        serde_json::from_str(&query_response).expect("Couldn't deserialize response.");
    assert_eq!(result["numHits"], Value::Number(Number::from(2i64)));

    make_command(
        format!(
            "index delete --index-id {} --metastore-uri {}",
            test_env.index_id, test_env.metastore_uri
        )
        .as_str(),
    )
    .assert()
    .success();
    assert_eq!(
        test_env
            .storage
            .exists(Path::new(&test_env.index_id))
            .await?,
        false
    );

    Ok(())
}

/// testing the api via structs of the lib (if available)
#[tokio::test]
#[serial]
#[cfg_attr(not(feature = "ci-test"), ignore)]
async fn test_all_with_s3_localstack_internal_api() -> Result<()> {
    let index_id = append_random_suffix("test-all--cli-API");
    let test_env = create_test_env(index_id, TestStorageType::S3)?;
    let args = CreateIndexArgs {
        metastore_uri: test_env.metastore_uri.clone(),
        index_config_uri: test_env.resource_files["index_config"]
            .to_str()
            .unwrap()
            .to_string(),
        overwrite: false,
    };
    create_index_cli(args).await?;
    let index_metadata = test_env
        .metastore()
        .index_metadata(&test_env.index_id)
        .await;
    assert_eq!(index_metadata.is_ok(), true);
    ingest_docs(
        &test_env.index_id,
        test_env.resource_files["logs"].as_path(),
        &test_env.metastore_uri,
        test_env.data_dir_path.as_path(),
    );

    // cli search
    make_command(
        format!(
            "index search --index-id {} --metastore-uri {} --query level:info",
            test_env.index_id, test_env.metastore_uri,
        )
        .as_str(),
    )
    .assert()
    .success()
    .stdout(predicate::function(|output: &[u8]| {
        let result: Value = serde_json::from_slice(output).unwrap();
        result["numHits"] == Value::Number(Number::from(2i64))
    }));

    // serve & api-search
    // TODO: ditto.
    let mut server_process = spawn_command(
        format!(
            "service run searcher --server-config-uri {}",
            test_env.resource_files["server_config"].display(),
        )
        .as_str(),
    )
    .unwrap();
    // TODO: ditto.
    sleep(Duration::from_secs(2)).await;
    let mut data = vec![0; 600];
    server_process
        .stdout
        .as_mut()
        .expect("Failed to get server process output")
        .read_exact(&mut data)
        .expect("Cannot read output");
    let process_output_str = String::from_utf8(data).unwrap();
    let query_response = reqwest::get(format!(
        "http://127.0.0.1:{}/api/v1/{}/search?query=level:info",
        test_env.searcher_rest_listen_port, test_env.index_id,
    ))
    .await?
    .text()
    .await?;
    server_process.kill().unwrap();

    assert!(process_output_str.contains("http://127.0.0.1:"));
    let result: Value =
        serde_json::from_str(&query_response).expect("Couldn't deserialize response.");
    assert_eq!(result["numHits"], Value::Number(Number::from(2i64)));

    make_command(
        format!(
            "index delete --index-id {} --metastore-uri {}",
            test_env.index_id, test_env.metastore_uri
        )
        .as_str(),
    )
    .assert()
    .success();
    assert_eq!(
        test_env
            .storage
            .exists(Path::new(&test_env.index_id))
            .await?,
        false
    );

    Ok(())
}
