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

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Stdio};
use std::sync::Arc;
use std::{fs, io};

use assert_cmd::cargo::cargo_bin;
use assert_cmd::Command;
use predicates::str;
use quickwit_common::net::find_available_port;
use quickwit_metastore::SingleFileMetastore;
use quickwit_storage::{LocalFileStorage, RegionProvider, S3CompatibleObjectStorage, Storage};
use tempfile::{tempdir, TempDir};

const PACKAGE_BIN_NAME: &str = "quickwit";

const DEFAULT_INDEX_CONFIG: &str = r#"
    version: 0

    index_id: #index_id
    index_uri: #index_uri

    doc_mapping:
      field_mappings:
        - name: ts
          type: i64
          fast: true
        - name: level
          type: text
          stored: false
        - name: event
          type: text
        - name: device
          type: text
          stored: false
          tokenizer: raw
        - name: city
          type: text
          stored: false
          tokenizer: raw

      tag_fields: [city, device]

    indexing_settings:
      timestamp_field: ts
      resources:
        heap_size: 50MB

    search_settings:
      default_search_fields: [event]
"#;

const DEFAULT_SERVER_CONFIG: &str = r#"
    version: 0
    metastore_uri: #metastore_uri

    indexer:
      data_dir_path: #data_dir_path
      rest_listen_port: #indexer.rest_listen_port
      grpc_listen_port: #indexer.grpc_listen_port
      discovery_listen_port: #indexer.discovery_listen_port
 
    searcher:
      data_dir_path: #data_dir_path
      host_key_path: #data_dir_path/host_key
      rest_listen_port: #searcher.rest_listen_port
      grpc_listen_port: #searcher.grpc_listen_port
      discovery_listen_port: #searcher.discovery_listen_port
"#;

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
    _tempdir: TempDir,
    /// Path of the directory where indexing directory are created.
    pub data_dir_path: PathBuf,
    /// Path of the directory where indexes are stored.
    pub indexes_dir_path: PathBuf,
    /// Resource files needed for the test.
    pub resource_files: HashMap<&'static str, PathBuf>,
    /// The metastore uri.
    pub metastore_uri: String,
    /// The index ID.
    pub index_id: String,
    pub searcher_rest_listen_port: u16,
    pub storage: Arc<dyn Storage>,
}

impl TestEnv {
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
pub fn create_test_env(index_id: String, storage_type: TestStorageType) -> anyhow::Result<TestEnv> {
    let tempdir = tempdir()?;
    let data_dir_path = tempdir.path().join("data");
    let indexes_dir_path = tempdir.path().join("indexes");
    let resources_dir_path = tempdir.path().join("resources");

    for dir_path in [&data_dir_path, &indexes_dir_path, &resources_dir_path] {
        fs::create_dir(dir_path)?;
    }

    // TODO: refactor when we have a singleton storage resolver.
    let (metastore_uri, storage) = match storage_type {
        TestStorageType::LocalFileSystem => {
            let metastore_uri = format!("file://{}", indexes_dir_path.display());
            let storage: Arc<dyn Storage> = Arc::new(LocalFileStorage::from_uri(&metastore_uri)?);
            (metastore_uri, storage)
        }
        TestStorageType::S3 => {
            let metastore_uri = "s3+localstack://quickwit-integration-tests/indexes";
            let storage: Arc<dyn Storage> = Arc::new(S3CompatibleObjectStorage::from_uri(
                RegionProvider::Localstack.get_region(),
                metastore_uri,
            )?);
            (metastore_uri.to_string(), storage)
        }
    };

    let index_uri = format!("{}/{}", metastore_uri, index_id);
    let index_config_path = resources_dir_path.join("index_config.yaml");
    fs::write(
        &index_config_path,
        // A poor's man templating engine...
        DEFAULT_INDEX_CONFIG
            .replace("#index_id", &index_id)
            .replace("#index_uri", &index_uri),
    )?;
    let server_config_path = resources_dir_path.join("server_config.yaml");
    let init_listen_port = find_available_port()?;
    let listen_ports = (0..6)
        .map(|i| init_listen_port + i)
        .map(|port| port.to_string())
        .collect::<Vec<String>>();
    fs::write(
        &server_config_path,
        // A poor's man templating engine reloaded...
        DEFAULT_SERVER_CONFIG
            .replace("#metastore_uri", &metastore_uri)
            .replace(
                "#data_dir_path",
                &data_dir_path.to_str().unwrap().to_string(),
            )
            .replace("#indexer.rest_listen_port", &listen_ports[0])
            .replace("#indexer.grpc_listen_port", &listen_ports[1])
            .replace("#indexer.discovery_listen_port", &listen_ports[2])
            .replace("#searcher.rest_listen_port", &listen_ports[3])
            .replace("#searcher.grpc_listen_port", &listen_ports[4])
            .replace("#searcher.discovery_listen_port", &listen_ports[5]),
    )?;
    let log_docs_path = resources_dir_path.join("logs.json");
    fs::write(&log_docs_path, LOGS_JSON_DOCS)?;
    let wikipedia_docs_path = resources_dir_path.join("wikis.json");
    fs::write(&wikipedia_docs_path, WIKI_JSON_DOCS)?;

    let mut resource_files = HashMap::new();
    resource_files.insert("index_config", index_config_path);
    resource_files.insert("server_config", server_config_path);
    resource_files.insert("logs", log_docs_path);
    resource_files.insert("wiki", wikipedia_docs_path);

    Ok(TestEnv {
        _tempdir: tempdir,
        data_dir_path,
        indexes_dir_path,
        resource_files,
        metastore_uri,
        index_id,
        searcher_rest_listen_port: init_listen_port + 3,
        storage,
    })
}
