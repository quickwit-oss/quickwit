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

use assert_cmd::cargo::cargo_bin;
use assert_cmd::Command;
use predicates::str;
use quickwit_metastore::SingleFileMetastore;
use quickwit_storage::LocalFileStorage;
use quickwit_storage::RegionProvider;
use quickwit_storage::S3CompatibleObjectStorage;
use quickwit_storage::Storage;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::process::Child;
use std::process::Stdio;
use std::sync::Arc;
use tempfile::tempdir;
use tempfile::TempDir;

const PACKAGE_BIN_NAME: &str = "quickwit";

const DEFAULT_INDEX_CONFIG: &str = r#"{
    // store the whole document as _source.
    "store_source": true,
    "default_search_fields": ["event"], // Used when no field is specified in your query. 
    "timestamp_field": "ts",
    "tag_fields": ["device", "city"],
    "field_mappings": [
        {
            "name": "event",
            "type": "text"
        },
        {
            "name": "level",
            "type": "text",
            "stored": false  /* Field not stored.*/
        },
        {
            "name": "ts",
            "type": "i64",
            "fast": /* timestamp field should be fast*/ true
        },
        {
            "name": "device",
            "type": "text",
            "stored": false
        },
        {
            "name": "city",
            "type": "text",
            "stored": false
        }
    ]
}"#;

const LOGS_JSON_DOCS: &str = r#"{"event": "foo", "level": "info", "ts": 2, "device": "rpi", "city": "tokio"}
{"event": "bar", "level": "error", "ts": 3, "device": "rpi", "city": "paris"}
{"event": "baz", "level": "warning", "ts": 9, "device": "fbit", "city": "london"}
{"event": "buz", "level": "debug", "ts": 12, "device": "rpi", "city": "paris"}
{"event": "biz", "level": "info", "ts": 13, "device": "fbit", "city": "paris"}"#;

const WIKI_JSON_DOCS: &str = r#"{"body": "foo", "title": "shimroy", "url": "https://wiki.com?id=10"}
{"body": "bar", "title": "shimray", "url": "https://wiki.com?id=12"}
{"body": "baz", "title": "preshow", "url": "https://wiki.com?id=11"}
{"body": "buz", "title": "frederick", "url": "https://wiki.com?id=48"}
{"body": "biz", "title": "modern", "url": "https://wiki.com?id=13"}
"#;

const AWS_DEFAULT_REGION_ENV: &str = "AWS_DEFAULT_REGION";

/// Creates a quickwit-cli command with provided list of arguments.
pub fn make_command(arguments: &str) -> Command {
    let mut cmd = Command::cargo_bin(PACKAGE_BIN_NAME).unwrap();
    cmd.env(
        quickwit_telemetry::DISABLE_TELEMETRY_ENV_KEY,
        "disable-for-tests",
    )
    .env(AWS_DEFAULT_REGION_ENV, "us-east-1")
    .args(arguments.split_whitespace());
    cmd
}

/// Creates a quickwit-cli command running as a child process.
pub fn spawn_command(arguments: &str) -> io::Result<Child> {
    std::process::Command::new(cargo_bin(PACKAGE_BIN_NAME))
        .args(arguments.split_whitespace())
        .env(
            quickwit_telemetry::DISABLE_TELEMETRY_ENV_KEY,
            "disable-for-tests",
        )
        .env(AWS_DEFAULT_REGION_ENV, "us-east-1")
        .stdout(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
}

/// A struct to hold few info about the test environement.
pub struct TestEnv {
    /// The temporary directory of the test.
    pub local_directory: TempDir,
    /// Path of the temporary directory of the test.
    pub local_directory_path: PathBuf,
    /// Resource files needed for the test.
    pub resource_files: HashMap<&'static str, PathBuf>,
    /// The metastore uri.
    pub metastore_uri: String,
    pub storage: Arc<dyn Storage>,
}

impl TestEnv {
    pub fn index_uri(&self, index_id: &str) -> String {
        format!("{}/{}", self.metastore_uri, index_id)
    }

    // For cache reason, it's safer to always create an instance and then make your assertions.
    pub fn metastore(&self) -> SingleFileMetastore {
        SingleFileMetastore::new(self.storage.clone())
    }
}

pub enum TestStorageType {
    S3,
    LocalFileSystem,
}

/// Creates all necessary artifacts in a test environement.
pub fn create_test_env(storage_type: TestStorageType) -> anyhow::Result<TestEnv> {
    let local_directory = tempdir()?;
    let local_directory_path = local_directory.path().to_path_buf();

    // TODO: refactor when we have a singleton storage resolver.
    let (metastore_uri, storage) = match storage_type {
        TestStorageType::LocalFileSystem => {
            let metastore_uri = format!("file://{}", local_directory_path.display());
            let storage: Arc<dyn Storage> = Arc::new(LocalFileStorage::from_uri(&metastore_uri)?);
            (metastore_uri, storage)
        }
        TestStorageType::S3 => {
            let metastore_uri = "s3+localstack://quickwit-integration-tests/indices";
            let storage: Arc<dyn Storage> = Arc::new(S3CompatibleObjectStorage::from_uri(
                RegionProvider::Localstack.get_region(),
                metastore_uri,
            )?);
            (metastore_uri.to_string(), storage)
        }
    };

    let config_path = local_directory.path().join("config.json");
    fs::write(&config_path, DEFAULT_INDEX_CONFIG)?;
    let log_docs_path = local_directory.path().join("logs.json");
    fs::write(&log_docs_path, LOGS_JSON_DOCS)?;
    let wikipedia_docs_path = local_directory.path().join("wikis.json");
    fs::write(&wikipedia_docs_path, WIKI_JSON_DOCS)?;

    let mut resource_files = HashMap::new();
    resource_files.insert("config", config_path);
    resource_files.insert("logs", log_docs_path);
    resource_files.insert("wiki", wikipedia_docs_path);

    Ok(TestEnv {
        local_directory,
        local_directory_path,
        resource_files,
        metastore_uri,
        storage,
    })
}
