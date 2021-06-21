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
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
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
            "type": "i64"
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

/// Creates a file with a content
pub fn create_file(path: &Path, data: &str) {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .unwrap();

    file.write_all(data.as_bytes()).unwrap();
    file.flush().unwrap();
    file.sync_all().unwrap();
}

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
    pub dir: TempDir,
    pub path: PathBuf,
    pub uri: String,
    pub config_path: PathBuf,
    pub log_data_path: PathBuf,
    pub wiki_data_path: PathBuf,
}

/// Creates all necessary artifacts in a test environement.
pub fn create_test_env() -> anyhow::Result<TestEnv> {
    let dir = tempdir()?;
    let path = PathBuf::from(format!("{}/indices/data", dir.path().display()));
    let uri = format!("file://{}", path.display());
    let config_path = dir.path().join("config.json");
    create_file(&config_path, DEFAULT_DOC_MAPPER);
    let log_data_path = dir.path().join("logs.json");
    create_file(&log_data_path, LOGS_JSON_DOCS);
    let wiki_data_path = dir.path().join("wikis.json");
    create_file(&wiki_data_path, WIKI_JSON_DOCS);

    Ok(TestEnv {
        dir,
        path,
        uri,
        config_path,
        log_data_path,
        wiki_data_path,
    })
}
