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

use crate::helpers::{create_test_env, make_command};
use anyhow::Result;
use helpers::{TestEnv, TestStorageType};
use predicates::prelude::*;
use quickwit_storage::{localstack_region, S3CompatibleObjectStorage, Storage};
use std::path::{Path, PathBuf};

fn create_wikipedia_index(test_env: &TestEnv) {
    make_command(
        format!(
            "new --index-uri {} --doc-mapper-type wikipedia",
            test_env.index_uri
        )
        .as_str(),
    )
    .assert()
    .success();
    assert!(test_env.local_directory_path.join("quickwit.json").exists());
}

fn create_logs_index(test_env: &TestEnv) {
    make_command(
        format!(
            "new --index-uri {} --doc-mapper-config-path {}",
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
    create_wikipedia_index(&test_env);

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
    create_wikipedia_index(&test_env);

    make_command(
        format!(
            "new --index-uri {} --doc-mapper-type wikipedia",
            test_env.index_uri
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
    .stderr(predicate::str::contains("No such file or directory"));

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
fn test_cmd_serve() -> Result<()> {
    //TODO: implement serve and search on it.
    Ok(())
}

#[test]
fn test_cmd_delete_index_dry_run() -> Result<()> {
    let test_env = create_test_env(TestStorageType::LocalFileSystem)?;
    create_wikipedia_index(&test_env);

    // Empty index.
    make_command(format!("delete --index-uri {} --dry-run", test_env.index_uri).as_str())
        .assert()
        .success()
        .stdout(predicate::str::contains("Only the index will be deleted"));

    index_data(
        &test_env.index_uri,
        test_env.resource_files["wiki"].as_path(),
    );

    // Non-empty index
    make_command(format!("delete --index-uri {} --dry-run", test_env.index_uri).as_str())
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "The following files will be removed",
        ))
        .stdout(predicate::str::contains("/hotcache"))
        .stdout(predicate::str::contains("/.manifest"));

    Ok(())
}

#[test]
fn test_cmd_delete() -> Result<()> {
    let test_env = create_test_env(TestStorageType::LocalFileSystem)?;
    create_wikipedia_index(&test_env);

    index_data(
        &test_env.index_uri,
        test_env.resource_files["wiki"].as_path(),
    );

    make_command(format!("delete --index-uri {} ", test_env.index_uri).as_str())
        .assert()
        .success();
    assert_eq!(test_env.local_directory_path.exists(), false);

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "ci-test"), ignore)]
async fn test_cmd_dry_run_delete_on_s3_localstack() -> Result<()> {
    let s3_path = PathBuf::from("quickwit-integration-tests/indices/data2");
    let test_env = create_test_env(TestStorageType::S3ViaLocalStorage(s3_path))?;
    make_command(
        format!(
            "new --index-uri {} --doc-mapper-type wikipedia",
            test_env.index_uri
        )
        .as_str(),
    )
    .assert()
    .success();

    index_data(
        &test_env.index_uri,
        test_env.resource_files["wiki"].as_path(),
    );

    make_command(format!("delete --index-uri {} --dry-run", test_env.index_uri).as_str())
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "The following files will be removed",
        ))
        .stdout(predicate::str::contains("/hotcache"))
        .stdout(predicate::str::contains("/.manifest"));

    make_command(format!("delete --index-uri {} ", test_env.index_uri).as_str())
        .assert()
        .success();

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "ci-test"), ignore)]
async fn test_all_with_s3_localstack() -> Result<()> {
    let s3_path = PathBuf::from("quickwit-integration-tests/indices/data1");
    let test_env = create_test_env(TestStorageType::S3ViaLocalStorage(s3_path))?;
    let object_storage =
        S3CompatibleObjectStorage::from_uri(localstack_region(), &test_env.index_uri)?;

    make_command(
        format!(
            "new --index-uri {} --doc-mapper-type wikipedia",
            test_env.index_uri
        )
        .as_str(),
    )
    .assert()
    .success();

    let metadata_file_exist = object_storage.exists(Path::new("quickwit.json")).await?;
    assert_eq!(metadata_file_exist, true);

    index_data(
        &test_env.index_uri,
        test_env.resource_files["wiki"].as_path(),
    );

    //TODO: add serve  & search

    make_command(format!("delete --index-uri {} ", test_env.index_uri).as_str())
        .assert()
        .success();
    let metadata_file_exist = object_storage.exists(Path::new("quickwit.json")).await?;
    assert_eq!(metadata_file_exist, false);

    Ok(())
}
