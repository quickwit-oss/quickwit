// Copyright (C) 2022 Quickwit, Inc.
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
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use predicates::str;
use quickwit_cli::service::RunCliCommand;
use quickwit_common::net::find_available_tcp_port;
use quickwit_common::test_utils::wait_for_server_ready;
use quickwit_common::uri::Uri;
use quickwit_metastore::{FileBackedMetastore, IndexMetadata, Metastore, MetastoreResult};
use quickwit_storage::{LocalFileStorage, S3CompatibleObjectStorage, Storage};
use reqwest::Url;
use tempfile::{tempdir, TempDir};
use tracing::error;

pub const PACKAGE_BIN_NAME: &str = "quickwit";

const DEFAULT_INDEX_CONFIG: &str = r#"
    version: 0.4

    index_id: #index_id
    index_uri: #index_uri

    doc_mapping:
      field_mappings:
        - name: ts
          type: datetime
          input_formats:
            - unix_timestamp
          output_format: unix_timestamp_secs
          precision: seconds
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

      timestamp_field: ts
      tag_fields: [city, device]

    indexing_settings:
      resources:
        heap_size: 50MB

    search_settings:
      default_search_fields: [event]
"#;

const DEFAULT_QUICKWIT_CONFIG: &str = r#"
    version: 0.4
    metastore_uri: #metastore_uri
    data_dir: #data_dir
    rest_listen_port: #rest_listen_port
    grpc_listen_port: #grpc_listen_port
"#;

const LOGS_JSON_DOCS: &str = r#"{"event": "foo", "level": "info", "ts": 72057597, "device": "rpi", "city": "tokio"}
{"event": "bar", "level": "error", "ts": 72057598, "device": "rpi", "city": "paris"}
{"event": "baz", "level": "warning", "ts": 72057604, "device": "fbit", "city": "london"}
{"event": "buz", "level": "debug", "ts": 72057607, "device": "rpi", "city": "paris"}
{"event": "biz", "level": "info", "ts": 72057608, "device": "fbit", "city": "paris"}"#;

const WIKI_JSON_DOCS: &str = r#"{"body": "foo", "title": "shimroy", "url": "https://wiki.com?id=10"}
{"body": "bar", "title": "shimray", "url": "https://wiki.com?id=12"}
{"body": "baz", "title": "preshow", "url": "https://wiki.com?id=11"}
{"body": "buz", "title": "frederick", "url": "https://wiki.com?id=48"}
{"body": "biz", "title": "modern", "url": "https://wiki.com?id=13"}
"#;

/// Waits until localhost:port is ready. Returns an error if it takes too long.
pub async fn wait_port_ready(port: u16) -> anyhow::Result<()> {
    let timer_task = tokio::time::sleep(Duration::from_secs(10));
    let port_check_task = async {
        while tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .is_err()
        {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    };
    tokio::select! {
        _ = timer_task => {
            bail!("Port took too much time to be ready.")
        }
        _ = port_check_task => {
            Ok(())
        }
    }
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
    /// The metastore URI.
    pub metastore_uri: Uri,
    pub config_uri: Uri,
    pub cluster_endpoint: Url,
    pub index_config_uri: Uri,
    /// The index ID.
    pub index_id: String,
    pub index_uri: Uri,
    pub rest_listen_port: u16,
    pub storage: Arc<dyn Storage>,
}

impl TestEnv {
    // For cache reason, it's safer to always create an instance and then make your assertions.
    pub async fn metastore(&self) -> MetastoreResult<FileBackedMetastore> {
        FileBackedMetastore::try_new(self.storage.clone(), None).await
    }

    pub fn index_config_without_uri(&self) -> String {
        self.resource_files["index_config_without_uri"]
            .display()
            .to_string()
    }

    pub async fn index_metadata(&self) -> anyhow::Result<IndexMetadata> {
        let index_metadata = self
            .metastore()
            .await?
            .index_metadata(&self.index_id)
            .await?;
        Ok(index_metadata)
    }

    pub async fn start_server(&self) -> anyhow::Result<()> {
        let run_command = RunCliCommand {
            config_uri: self.config_uri.clone(),
            services: None,
        };
        tokio::spawn(async move {
            if let Err(error) = run_command.execute().await {
                error!(err=?error, "Failed to start a quickwit server");
            }
        });
        wait_for_server_ready(([127, 0, 0, 1], self.rest_listen_port).into()).await?;
        Ok(())
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
    let indexes_dir_path = data_dir_path.join("indexes");
    let resources_dir_path = tempdir.path().join("resources");

    for dir_path in [&data_dir_path, &indexes_dir_path, &resources_dir_path] {
        fs::create_dir(dir_path)?;
    }

    // TODO: refactor when we have a singleton storage resolver.
    let (metastore_uri, storage) = match storage_type {
        TestStorageType::LocalFileSystem => {
            let metastore_uri =
                Uri::from_well_formed(format!("file://{}", indexes_dir_path.display()));
            let storage: Arc<dyn Storage> = Arc::new(LocalFileStorage::from_uri(&metastore_uri)?);
            (metastore_uri, storage)
        }
        TestStorageType::S3 => {
            let metastore_uri =
                Uri::from_well_formed("s3://quickwit-integration-tests/indexes".to_string());
            let storage: Arc<dyn Storage> =
                Arc::new(S3CompatibleObjectStorage::from_uri(&metastore_uri)?);
            (metastore_uri, storage)
        }
    };
    let index_uri = metastore_uri.join(&index_id).unwrap();
    let index_config_path = resources_dir_path.join("index_config.yaml");
    fs::write(
        &index_config_path,
        DEFAULT_INDEX_CONFIG
            .replace("#index_id", &index_id)
            .replace("#index_uri", index_uri.as_str()),
    )?;
    let index_config_without_uri_path = resources_dir_path.join("index_config_without_uri.yaml");
    fs::write(
        &index_config_without_uri_path,
        DEFAULT_INDEX_CONFIG
            .replace("#index_id", &index_id)
            .replace("index_uri: #index_uri\n", ""),
    )?;
    let quickwit_config_path = resources_dir_path.join("config.yaml");
    let rest_listen_port = find_available_tcp_port()?;
    let grpc_listen_port = find_available_tcp_port()?;
    fs::write(
        &quickwit_config_path,
        // A poor's man templating engine reloaded...
        DEFAULT_QUICKWIT_CONFIG
            .replace("#metastore_uri", metastore_uri.as_str())
            .replace("#data_dir", data_dir_path.to_str().unwrap())
            .replace("#rest_listen_port", &rest_listen_port.to_string())
            .replace("#grpc_listen_port", &grpc_listen_port.to_string()),
    )?;
    let log_docs_path = resources_dir_path.join("logs.json");
    fs::write(&log_docs_path, LOGS_JSON_DOCS)?;
    let wikipedia_docs_path = resources_dir_path.join("wikis.json");
    fs::write(&wikipedia_docs_path, WIKI_JSON_DOCS)?;

    let mut resource_files = HashMap::new();
    resource_files.insert("config", quickwit_config_path);
    resource_files.insert("index_config", index_config_path);
    resource_files.insert("index_config_without_uri", index_config_without_uri_path);
    resource_files.insert("logs", log_docs_path);
    resource_files.insert("wiki", wikipedia_docs_path);

    let config_uri =
        Uri::from_well_formed(format!("file://{}", resource_files["config"].display()));
    let index_config_uri = Uri::from_well_formed(format!(
        "file://{}",
        resource_files["index_config"].display()
    ));
    let cluster_endpoint = Url::parse(&format!("http://localhost:{rest_listen_port}"))
        .context("Failed to parse cluster endpoint.")?;

    Ok(TestEnv {
        _tempdir: tempdir,
        data_dir_path,
        indexes_dir_path,
        resource_files,
        metastore_uri,
        config_uri,
        cluster_endpoint,
        index_config_uri,
        index_id,
        index_uri,
        rest_listen_port,
        storage,
    })
}
