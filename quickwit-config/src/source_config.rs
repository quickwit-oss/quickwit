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

use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceConfig {
    pub source_id: String,
    #[serde(flatten)]
    pub source_type: SourceType,
}

impl SourceConfig {
    pub fn source_type(&self) -> &str {
        match self.source_type {
            SourceType::File(_) => "file",
            SourceType::Kafka(_) => "kafka",
            SourceType::Kinesis(_) => "kinesis",
            SourceType::Vec(_) => "vec",
            SourceType::Void(_) => "void",
        }
    }

    // TODO: Remove after source factory refactor.
    pub fn params(&self) -> serde_json::Value {
        let params = match &self.source_type {
            SourceType::File(params) => serde_json::to_value(params),
            SourceType::Kafka(params) => serde_json::to_value(params),
            SourceType::Kinesis(params) => serde_json::to_value(params),
            SourceType::Vec(params) => serde_json::to_value(params),
            SourceType::Void(params) => serde_json::to_value(params),
        }
        .unwrap();
        params
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "source_type", content = "params")]
pub enum SourceType {
    #[serde(rename = "file")]
    File(FileSourceParams),
    #[serde(rename = "kafka")]
    Kafka(KafkaSourceParams),
    #[doc(hidden)]
    #[serde(rename = "kinesis")]
    Kinesis(KinesisSourceParams),
    #[serde(rename = "vec")]
    Vec(VecSourceParams),
    #[serde(rename = "void")]
    Void(VoidSourceParams),
}

impl SourceType {
    pub fn file<P: AsRef<Path>>(filepath: P) -> Self {
        Self::File(FileSourceParams::file(filepath))
    }

    pub fn stdin() -> Self {
        Self::File(FileSourceParams::stdin())
    }

    pub fn void() -> Self {
        Self::Void(VoidSourceParams)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FileSourceParams {
    /// Path of the file to read. Assume stdin if None.
    pub filepath: Option<PathBuf>, //< If None read from stdin.
}

impl FileSourceParams {
    pub fn file<P: AsRef<Path>>(filepath: P) -> Self {
        FileSourceParams {
            filepath: Some(filepath.as_ref().to_path_buf()),
        }
    }

    pub fn stdin() -> Self {
        FileSourceParams { filepath: None }
    }

    pub fn canonical_filepath(&self) -> io::Result<Option<PathBuf>> {
        match &self.filepath {
            Some(filepath) => {
                let canonical_filepath = filepath.canonicalize()?;
                Ok(Some(canonical_filepath))
            }
            None => Ok(None),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KafkaSourceParams {
    /// Name of the topic that the source consumes.
    pub topic: String,
    /// Kafka client log level. Possible values are `debug`, `info`, `warn`, and `error`.
    pub client_log_level: Option<String>,
    /// Kafka client configuration parameters.
    pub client_params: serde_json::Value,
}

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KinesisSourceParams {
    stream_name: String
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct VecSourceParams {
    pub items: Vec<String>,
    pub batch_num_docs: usize,
    #[serde(default)]
    pub partition: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VoidSourceParams;
