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
use helpers::TestEnv;
use predicates::prelude::*;
use quickwit_storage::{localstack_region, S3CompatibleObjectStorage, Storage};
use std::path::Path;

fn create_wikipedia_index(test_env: &TestEnv) {
    make_command(
        format!(
            "new --index-uri {} --doc-mapper-type wikipedia",
            test_env.uri
        )
        .as_str(),
    )
    .assert()
    .success();
    assert!(test_env.path.join("quickwit.json").exists());
}

fn create_logs_index(test_env: &TestEnv) {
    make_command(
        format!(
            "new --index-uri {} --doc-mapper-config-path {}",
            test_env.uri,
            test_env.config_path.display()
        )
        .as_str(),
    )
    .assert()
    .success();
    assert!(test_env.path.join("quickwit.json").exists());
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

fn delete_index(test_env: &TestEnv) {
    make_command(format!("delete --index-uri {} ", test_env.uri).as_str())
        .assert()
        .success();
    assert_eq!(test_env.path.exists(), false);
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
    let test_env = create_test_env(false)?;
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
    let test_env = create_test_env(false)?;
    create_wikipedia_index(&test_env);

    make_command(
        format!(
            "new --index-uri {} --doc-mapper-type wikipedia",
            test_env.uri
        )
        .as_str(),
    )
    .assert()
    .failure()
    .stderr(predicate::str::contains("already exists"));

    Ok(())
}

#[test]
fn test_cmd_index() -> Result<()> {
    let test_env = create_test_env(false)?;
    create_logs_index(&test_env);

    // Indexing data on non-existing index.
    make_command(
        format!(
            "index --index-uri {}/non-existing-index --input-path {}",
            test_env.uri,
            test_env.log_data_path.display(),
        )
        .as_str(),
    )
    .assert()
    .failure()
    .stderr(predicate::str::contains("does not exist"));

    // Index on non-existing data file.
    make_command(
        format!(
            "index --index-uri {} --input-path {}",
            test_env.uri,
            test_env.dir.path().join("non-existing-data.json").display(),
        )
        .as_str(),
    )
    .assert()
    .failure()
    .stderr(predicate::str::contains("No such file or directory"));

    index_data(&test_env.uri, test_env.log_data_path.as_path());

    // Indexing data with piped input.
    make_command(format!("index --index-uri {} ", test_env.uri).as_str())
        .pipe_stdin(test_env.log_data_path)?
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
fn test_cmd_delete() -> Result<()> {
    let test_env = create_test_env(false)?;
    create_wikipedia_index(&test_env);

    // Delete empty index.
    make_command(format!("delete --index-uri {} --dry-run", test_env.uri).as_str())
        .assert()
        .success()
        .stdout(predicate::str::contains("Only the index will be deleted"));

    // Index some data.
    index_data(&test_env.uri, test_env.wiki_data_path.as_path());

    // Delete index with dry-run.
    make_command(format!("delete --index-uri {} --dry-run", test_env.uri).as_str())
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "The following files will be removed",
        ))
        .stdout(predicate::str::contains("/hotcache"))
        .stdout(predicate::str::contains("/.manifest"));

    delete_index(&test_env);

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_all_with_s3_localstack() -> Result<()> {
    let test_env = create_test_env(true)?;
    let object_storage = S3CompatibleObjectStorage::from_uri(localstack_region(), &test_env.uri)?;

    make_command(
        format!(
            "new --index-uri {} --doc-mapper-type wikipedia",
            test_env.uri
        )
        .as_str(),
    )
    .assert()
    .success();
    let metadata_file_exist = object_storage.exists(Path::new("quickwit.json")).await?;
    assert_eq!(metadata_file_exist, true);

    // Index some data.
    index_data(&test_env.uri, test_env.wiki_data_path.as_path());

    // Delete index with dry-run.
    make_command(format!("delete --index-uri {} --dry-run", test_env.uri).as_str())
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "The following files will be removed",
        ))
        .stdout(predicate::str::contains("/hotcache"))
        .stdout(predicate::str::contains("/.manifest"));

    // Delete index.
    make_command(format!("delete --index-uri {} ", test_env.uri).as_str())
        .assert()
        .success();
    let metadata_file_exist = object_storage.exists(Path::new("quickwit.json")).await?;
    assert_eq!(metadata_file_exist, false);

    Ok(())
}
