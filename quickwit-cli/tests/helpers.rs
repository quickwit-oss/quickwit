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

use assert_cmd::Command;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use tempfile::tempdir;
use tempfile::TempDir;

const DEFAULT_DOC_MAPPER: &str = r#"{
    "store_source": true,
    "default_search_fields": ["event"],
    "timestamp_field": "ts",
    "field_mappings": [
        {
            "name": "event",
            "type": "text"
        },
        {
            "name": "level",
            "type": "text"
        },
        {
            "name": "ts",
            "type": "i64",
            "fast": true
        }
    ]
}"#;

const LOGS_JSON_DOCS: &str = r#"{"event": "foo", "level": "info", "ts": 2}
{"event": "bar", "level": "error", "ts": 3}
{"event": "baz", "level": "warning", "ts": 9}
{"event": "buz", "level": "debug", "ts": 12}
{"event": "biz", "level": "info", "ts": 13}"#;

const WIKI_JSON_DOCS: &str = r#"{"body": "foo", "title": "shimray", "url": "https://wiki.com?id=10"}
{"body": "bar", "title": "shimray", "url": "https://wiki.com?id=12"}
{"body": "baz", "title": "preshow", "url": "https://wiki.com?id=11"}
{"body": "buz", "title": "frederick", "url": "https://wiki.com?id=48"}
{"body": "biz", "title": "modern", "url": "https://wiki.com?id=13"}
"#;

/// Creates a quickwit-cli command with provided list of arguments.
pub fn make_command(argument: &str) -> Command {
    let mut cmd = Command::cargo_bin("quickwit-cli").unwrap();
    for arg in argument.split_whitespace() {
        cmd.arg(arg);
    }
    cmd
}

/// A struct to hold few info about the test environement.
pub struct TestEnv {
    /// The temporary directory of the test.
    pub local_directory: TempDir,
    /// Path of the temporary directory of the test.
    pub local_directory_path: PathBuf,
    /// The index uri.
    pub index_uri: String,
    /// Resource files needed for the test.
    pub resource_files: HashMap<&'static str, PathBuf>,
}

pub enum TestStorageType {
    S3ViaLocalStaorage(PathBuf),
    LocalFileSystem,
}

/// Creates all necessary artifacts in a test environement.
pub fn create_test_env(storage_type: TestStorageType) -> anyhow::Result<TestEnv> {
    let local_directory = tempdir()?;
    let (local_directory_path, index_uri) = match storage_type {
        TestStorageType::LocalFileSystem => {
            let local_path =
                PathBuf::from(format!("{}/indices/data", local_directory.path().display()));
            let uri = format!("file://{}", local_path.display());
            (local_path, uri)
        }
        TestStorageType::S3ViaLocalStaorage(s3_path) => {
            let uri = format!("s3+localstack://{}", s3_path.display());
            (s3_path, uri)
        }
    };

    let config_path = local_directory.path().join("config.json");
    fs::write(&config_path, DEFAULT_DOC_MAPPER)?;
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
        index_uri,
        resource_files,
    })
}
