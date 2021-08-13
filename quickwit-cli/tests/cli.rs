/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#![allow(clippy::bool_assert_comparison)]

mod helpers;

use crate::helpers::{create_test_env, make_command, spawn_command};
use anyhow::Result;
use helpers::{TestEnv, TestStorageType};
use predicates::prelude::*;
use quickwit_cli::{create_index_cli, CreateIndexArgs};
use quickwit_common::extract_metastore_uri_and_index_id_from_index_uri;
use quickwit_metastore::{MetastoreUriResolver, SplitState};
use quickwit_storage::{localstack_region, S3CompatibleObjectStorage, Storage};
use serde_json::{Number, Value};
use serial_test::serial;
use std::{
    io::Read,
    path::{Path, PathBuf},
};
use tokio::time::{sleep, Duration};

fn create_logs_index(test_env: &TestEnv) {
    make_command(
        format!(
            "new --index-uri {} --index-config-path {}",
            test_env.index_uri,
            test_env.resource_files["config"].display()
        )
        .as_str(),
    )
    .assert()
    .success();
    assert!(test_env.local_directory_path.join("quickwit.json").exists());
}

fn index_data(uri: &str, input_path: &Path) {
    make_command(
        format!(
            "index --index-uri {} --input-path {}",
            uri,
            input_path.display(),
        )
        .as_str(),
    )
    .assert()
    .success()
    .stdout(predicate::str::contains("Indexed"))
    .stdout(predicate::str::contains("documents in"))
    .stdout(predicate::str::contains(
        "You can now query your index with",
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

#[test]
fn test_cmd_new() -> Result<()> {
    let test_env = create_test_env(TestStorageType::LocalFileSystem)?;
    create_logs_index(&test_env);

    // Attempt to create with ill-formed new command.
    make_command("new")
        .assert()
        .failure()
        .stderr(predicate::str::contains("--index-uri <INDEX URI>"));

    Ok(())
}

#[test]
fn test_cmd_new_on_existing_index() -> Result<()> {
    let test_env = create_test_env(TestStorageType::LocalFileSystem)?;
    create_logs_index(&test_env);

    make_command(
        format!(
            "new --index-uri {} --index-config-path {}",
            test_env.index_uri,
            test_env.resource_files["config"].display()
        )
        .as_str(),
    )
    .assert()
    .failure()
    .stderr(predicate::str::contains("already exists"));

    Ok(())
}

#[test]
fn test_cmd_index_on_non_existing_index() -> Result<()> {
    let test_env = create_test_env(TestStorageType::LocalFileSystem)?;
    make_command(
        format!(
            "index --index-uri {}/non-existing-index --input-path {}",
            test_env.index_uri,
            test_env.resource_files["logs"].display(),
        )
        .as_str(),
    )
    .assert()
    .failure()
    .stderr(predicate::str::contains("does not exist"));

    Ok(())
}

#[test]
fn test_cmd_index_on_non_existing_file() -> Result<()> {
    let test_env = create_test_env(TestStorageType::LocalFileSystem)?;
    create_logs_index(&test_env);
    make_command(
        format!(
            "index --index-uri {} --input-path {}",
            test_env.index_uri,
            test_env
                .local_directory_path
                .join("non-existing-data.json")
                .display(),
        )
        .as_str(),
    )
    .assert()
    .failure()
    .stderr(predicate::str::contains("Command failed"));

    Ok(())
}

#[test]
fn test_cmd_index() -> Result<()> {
    let test_env = create_test_env(TestStorageType::LocalFileSystem)?;
    create_logs_index(&test_env);

    index_data(
        &test_env.index_uri,
        test_env.resource_files["logs"].as_path(),
    );

    // using piped input.
    let log_path = test_env.resource_files["logs"].clone();
    make_command(format!("index --index-uri {} ", test_env.index_uri).as_str())
        .pipe_stdin(log_path)?
        .assert()
        .success()
        .stdout(predicate::str::contains("Indexed"))
        .stdout(predicate::str::contains("documents in"))
        .stdout(predicate::str::contains(
            "You can now query your index with",
        ));

    Ok(())
}

#[test]
fn test_cmd_delete_index_dry_run() -> Result<()> {
    let test_env = create_test_env(TestStorageType::LocalFileSystem)?;
    create_logs_index(&test_env);

    // Empty index.
    make_command(format!("delete --index-uri {} --dry-run", test_env.index_uri).as_str())
        .assert()
        .success()
        .stdout(predicate::str::contains("Only the index will be deleted"));

    index_data(
        &test_env.index_uri,
        test_env.resource_files["logs"].as_path(),
    );

    // Non-empty index
    make_command(format!("delete --index-uri {} --dry-run", test_env.index_uri).as_str())
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "The following splits will be removed",
        ));

    Ok(())
}

#[test]
fn test_cmd_delete() -> Result<()> {
    let test_env = create_test_env(TestStorageType::LocalFileSystem)?;
    create_logs_index(&test_env);

    index_data(
        &test_env.index_uri,
        test_env.resource_files["logs"].as_path(),
    );

    make_command(format!("gc --index-uri {}", test_env.index_uri).as_str())
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "No dangling splits to garbage collect",
        ));

    make_command(format!("delete --index-uri {} ", test_env.index_uri).as_str())
        .assert()
        .success();
    assert_eq!(test_env.local_directory_path.exists(), false);

    Ok(())
}

#[tokio::test]
async fn test_cmd_garbage_collect() -> Result<()> {
    let test_env = create_test_env(TestStorageType::LocalFileSystem)?;
    create_logs_index(&test_env);
    index_data(
        &test_env.index_uri,
        test_env.resource_files["logs"].as_path(),
    );

    let (metastore_uri, index_id) =
        extract_metastore_uri_and_index_id_from_index_uri(&test_env.index_uri)?;
    let metastore = MetastoreUriResolver::default()
        .resolve(metastore_uri)
        .await?;
    let splits = metastore.list_all_splits(index_id).await?;
    assert_eq!(splits.len(), 1);
    make_command(format!("gc --index-uri {}", test_env.index_uri).as_str())
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "No dangling splits to garbage collect",
        ));

    let split_path = test_env
        .local_directory_path
        .join(splits[0].split_id.as_str());
    assert_eq!(split_path.exists(), true);

    let split_ids = vec![splits[0].split_id.as_str()];
    metastore
        .mark_splits_as_deleted(index_id, &split_ids)
        .await?;
    make_command(
        format!(
            "gc --index-uri {} --dry-run --grace-period 10m",
            test_env.index_uri
        )
        .as_str(),
    )
    .assert()
    .success()
    .stdout(predicate::str::contains(
        "The following splits will be garbage collected.",
    ));
    assert_eq!(split_path.exists(), true);

    make_command(format!("gc --index-uri {} --grace-period 10m", test_env.index_uri).as_str())
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Index successfully garbage collected",
        ));

    assert_eq!(split_path.exists(), false);
    let metastore = MetastoreUriResolver::default()
        .resolve(metastore_uri)
        .await?;
    assert_eq!(metastore.list_all_splits(index_id).await?.len(), 0);

    make_command(format!("delete --index-uri {} ", test_env.index_uri).as_str())
        .assert()
        .success();
    assert_eq!(test_env.local_directory_path.exists(), false);
    Ok(())
}

#[tokio::test]
async fn test_cmd_garbage_collect_spares_files_within_grace_period() -> Result<()> {
    let test_env = create_test_env(TestStorageType::LocalFileSystem)?;
    create_logs_index(&test_env);
    index_data(
        &test_env.index_uri,
        test_env.resource_files["logs"].as_path(),
    );

    let (metastore_uri, index_id) =
        extract_metastore_uri_and_index_id_from_index_uri(&test_env.index_uri)?;
    let metastore = MetastoreUriResolver::default()
        .resolve(metastore_uri)
        .await?;
    let splits = metastore.list_all_splits(index_id).await?;
    assert_eq!(splits.len(), 1);
    make_command(format!("gc --index-uri {}", test_env.index_uri).as_str())
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "No dangling splits to garbage collect",
        ));

    let split_path = test_env
        .local_directory_path
        .join(splits[0].split_id.as_str());
    assert_eq!(split_path.exists(), true);

    // The following steps help turn an existing published split into a staged one
    // without deleting the files.
    let split_ids = vec![splits[0].split_id.as_str()];
    metastore
        .mark_splits_as_deleted(index_id, &split_ids)
        .await?;
    metastore.delete_splits(index_id, &split_ids).await?;
    let mut split_meta = splits[0].clone();
    split_meta.split_state = SplitState::New;
    metastore.stage_split(index_id, split_meta).await?;
    assert_eq!(split_path.exists(), true);

    make_command(format!("gc --index-uri {} --grace-period 2s", test_env.index_uri).as_str())
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "No dangling splits to garbage collect",
        ));
    assert_eq!(split_path.exists(), true);

    // wait for grace period
    sleep(Duration::from_secs(3)).await;
    make_command(
        format!(
            "gc --index-uri {} --dry-run --grace-period 2s",
            test_env.index_uri
        )
        .as_str(),
    )
    .assert()
    .success()
    .stdout(predicate::str::contains(
        "The following splits will be garbage collected.",
    ));
    assert_eq!(split_path.exists(), true);

    make_command(format!("gc --index-uri {} --grace-period 2s", test_env.index_uri).as_str())
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Index successfully garbage collected",
        ));
    assert_eq!(split_path.exists(), false);

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "ci-test"), ignore)]
async fn test_cmd_dry_run_delete_on_s3_localstack() -> Result<()> {
    let s3_path = PathBuf::from("quickwit-integration-tests/indices/data2");
    let test_env = create_test_env(TestStorageType::S3ViaLocalStorage(s3_path))?;
    make_command(
        format!(
            "new --index-uri {} --index-config-path {}",
            test_env.index_uri,
            test_env.resource_files["config"].display()
        )
        .as_str(),
    )
    .assert()
    .success();

    index_data(
        &test_env.index_uri,
        test_env.resource_files["logs"].as_path(),
    );

    make_command(format!("gc --index-uri {}", test_env.index_uri).as_str())
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "No dangling splits to garbage collect",
        ));

    make_command(format!("delete --index-uri {} --dry-run", test_env.index_uri).as_str())
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "The following splits will be removed",
        ))
        .stdout(predicate::str::contains("/hotcache"))
        .stdout(predicate::str::contains("/.manifest"));

    make_command(format!("delete --index-uri {} ", test_env.index_uri).as_str())
        .assert()
        .success();

    Ok(())
}

/// testing the api via cli commands
#[tokio::test]
#[serial]
#[cfg_attr(not(feature = "ci-test"), ignore)]
async fn test_all_with_s3_localstack_cli() -> Result<()> {
    let data_endpoint = "data1";
    let s3_path =
        PathBuf::from(&("quickwit-integration-tests/indices/".to_string() + data_endpoint));
    let test_env = create_test_env(TestStorageType::S3ViaLocalStorage(s3_path))?;
    let object_storage =
        S3CompatibleObjectStorage::from_uri(localstack_region(), &test_env.index_uri)?;

    make_command(
        format!(
            "new --index-uri {} --index-config-path {}",
            test_env.index_uri,
            test_env.resource_files["config"].display()
        )
        .as_str(),
    )
    .assert()
    .success();

    let metadata_file_exist = object_storage.exists(Path::new("quickwit.json")).await?;
    assert_eq!(metadata_file_exist, true);

    index_data(
        &test_env.index_uri,
        test_env.resource_files["logs"].as_path(),
    );

    // cli search
    make_command(
        format!(
            "search --index-uri {} --query level:info",
            test_env.index_uri
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
    let mut server_process = spawn_command(
        format!(
            "serve --index-uri {} --host 127.0.0.1 --port 8182",
            test_env.index_uri
        )
        .as_str(),
    )
    .unwrap();
    sleep(Duration::from_secs(2)).await;
    let mut data = vec![0; 512];
    server_process
        .stdout
        .as_mut()
        .expect("Failed to get server process output")
        .read_exact(&mut data)
        .expect("Cannot read output");
    let process_output_str = String::from_utf8(data).unwrap();
    let query_response = reqwest::get(format!(
        "http://127.0.0.1:8182/api/v1/{}/search?query=level:info",
        data_endpoint
    ))
    .await?
    .text()
    .await?;
    server_process.kill().unwrap();

    assert!(process_output_str.contains("http://127.0.0.1:8182"));
    let result: Value =
        serde_json::from_str(&query_response).expect("Couldn't deserialize response.");
    assert_eq!(result["numHits"], Value::Number(Number::from(2i64)));

    make_command(format!("delete --index-uri {} ", test_env.index_uri).as_str())
        .assert()
        .success();
    let metadata_file_exist = object_storage.exists(Path::new("quickwit.json")).await?;
    assert_eq!(metadata_file_exist, false);

    Ok(())
}

/// testing the api via structs of the lib (if available)
#[tokio::test]
#[serial]
#[cfg_attr(not(feature = "ci-test"), ignore)]
async fn test_all_with_s3_localstack_internal_api() -> Result<()> {
    let data_endpoint = "data11";
    let s3_path =
        PathBuf::from(&("quickwit-integration-tests/indices/".to_string() + data_endpoint));
    let test_env = create_test_env(TestStorageType::S3ViaLocalStorage(s3_path))?;
    let object_storage =
        S3CompatibleObjectStorage::from_uri(localstack_region(), &test_env.index_uri)?;
    let args = CreateIndexArgs::new(
        test_env.index_uri.to_string(),
        test_env.resource_files["config"].to_path_buf(),
        false,
    )?;
    create_index_cli(args).await?;

    let metadata_file_exist = object_storage.exists(Path::new("quickwit.json")).await?;
    assert_eq!(metadata_file_exist, true);

    index_data(
        &test_env.index_uri,
        test_env.resource_files["logs"].as_path(),
    );

    // cli search
    make_command(
        format!(
            "search --index-uri {} --query level:info",
            test_env.index_uri
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
    let mut server_process = spawn_command(
        format!(
            "serve --index-uri {} --host 127.0.0.1 --port 8182",
            test_env.index_uri
        )
        .as_str(),
    )
    .unwrap();
    sleep(Duration::from_secs(2)).await;
    let mut data = vec![0; 512];
    server_process
        .stdout
        .as_mut()
        .expect("Failed to get server process output")
        .read_exact(&mut data)
        .expect("Cannot read output");
    let process_output_str = String::from_utf8(data).unwrap();
    let query_response = reqwest::get(format!(
        "http://127.0.0.1:8182/api/v1/{}/search?query=level:info",
        data_endpoint
    ))
    .await?
    .text()
    .await?;
    server_process.kill().unwrap();

    assert!(process_output_str.contains("http://127.0.0.1:8182"));
    let result: Value =
        serde_json::from_str(&query_response).expect("Couldn't deserialize response.");
    assert_eq!(result["numHits"], Value::Number(Number::from(2i64)));

    make_command(format!("delete --index-uri {} ", test_env.index_uri).as_str())
        .assert()
        .success();
    let metadata_file_exist = object_storage.exists(Path::new("quickwit.json")).await?;
    assert_eq!(metadata_file_exist, false);

    Ok(())
}
