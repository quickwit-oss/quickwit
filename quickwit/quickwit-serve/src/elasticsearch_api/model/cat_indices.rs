// Copyright (C) 2024 Quickwit, Inc.
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

use std::collections::HashSet;
use std::ops::AddAssign;

use http_serde::http::StatusCode;
use quickwit_metastore::{IndexMetadata, SplitMetadata};
use serde::{Deserialize, Serialize, Serializer};

use super::ElasticsearchError;
use crate::simple_list::{from_simple_list, to_simple_list};

#[serde_with::skip_serializing_none]
#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatIndexQueryParams {
    #[serde(default)]
    /// Only JSON supported for now.
    pub format: Option<String>,
    /// Comma-separated list of column names to display.
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    pub h: Option<Vec<String>>,
    #[serde(default)]
    /// Filter for health: green, yellow, or red
    pub health: Option<Health>,
    /// Unit used to display byte values.
    /// Unsupported for now.
    #[serde(default)]
    pub bytes: Option<String>,
    /// Comma-separated list of column names or column aliases used to sort the response.
    /// Unsupported for now.
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    pub s: Option<Vec<String>>,
    /// If true, the response includes column headings. Defaults to false.
    /// Unsupported for now.
    #[serde(default)]
    pub v: Option<bool>,
}
impl CatIndexQueryParams {
    pub fn validate(&self) -> Result<(), ElasticsearchError> {
        if let Some(format) = &self.format {
            if format.to_lowercase() != "json" {
                return Err(ElasticsearchError::new(
                    StatusCode::BAD_REQUEST,
                    format!(
                        "Format {:?} is not supported. Only format=json is supported.",
                        format
                    ),
                    None,
                ));
            }
        } else {
            return Err(ElasticsearchError::new(
                StatusCode::BAD_REQUEST,
                "Only format=json is supported.".to_string(),
                None,
            ));
        }
        let unsupported_parameter_error = |field: &str| {
            ElasticsearchError::new(
                StatusCode::BAD_REQUEST,
                format!("Parameter {:?} is not supported.", field),
                None,
            )
        };
        if self.bytes.is_some() {
            return Err(unsupported_parameter_error("bytes"));
        }
        if self.v.is_some() {
            return Err(unsupported_parameter_error("v"));
        }
        if self.s.is_some() {
            return Err(unsupported_parameter_error("s"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ElasticsearchCatIndexResponse {
    pub health: Health,
    status: Status,
    pub index: String,
    uuid: String,
    pri: String,
    rep: String,
    #[serde(rename = "docs.count", serialize_with = "serialize_u64_as_string")]
    docs_count: u64,
    #[serde(rename = "docs.deleted", serialize_with = "serialize_u64_as_string")]
    docs_deleted: u64,
    #[serde(rename = "store.size", serialize_with = "ser_es_format")]
    store_size: u64,
    #[serde(rename = "pri.store.size", serialize_with = "ser_es_format")]
    pri_store_size: u64,
    #[serde(rename = "dataset.size", serialize_with = "ser_es_format")]
    dataset_size: u64,
}

impl ElasticsearchCatIndexResponse {
    pub fn serialize_filtered(
        &self,
        fields: &Option<Vec<String>>,
    ) -> serde_json::Result<serde_json::Value> {
        let mut value = serde_json::to_value(self)?;

        if let Some(fields) = fields {
            let fields: HashSet<String> = fields.iter().cloned().collect();
            // If fields are specified, retain only those fields
            if let serde_json::Value::Object(ref mut map) = value {
                map.retain(|key, _| fields.contains(key));
            }
        }

        Ok(value)
    }
}
impl AddAssign for ElasticsearchCatIndexResponse {
    fn add_assign(&mut self, rhs: Self) {
        self.health += rhs.health;
        self.status += rhs.status;
        self.docs_count += rhs.docs_count;
        self.docs_deleted += rhs.docs_deleted;
        self.store_size += rhs.store_size;
        self.pri_store_size += rhs.pri_store_size;
        self.dataset_size += rhs.dataset_size;
    }
}

impl From<IndexMetadata> for ElasticsearchCatIndexResponse {
    fn from(index_metadata: IndexMetadata) -> Self {
        ElasticsearchCatIndexResponse {
            uuid: index_metadata.index_uid.to_string(),
            index: index_metadata.index_config.index_id.to_string(),
            pri: "1".to_string(),
            rep: "1".to_string(),
            ..Default::default()
        }
    }
}

impl From<SplitMetadata> for ElasticsearchCatIndexResponse {
    fn from(split_metadata: SplitMetadata) -> Self {
        ElasticsearchCatIndexResponse {
            store_size: split_metadata.as_split_info().file_size_bytes.as_u64(),
            pri_store_size: split_metadata.as_split_info().file_size_bytes.as_u64(),
            dataset_size: split_metadata
                .as_split_info()
                .uncompressed_docs_size_bytes
                .as_u64(),
            uuid: split_metadata.index_uid.to_string(),
            pri: "1".to_string(),
            rep: "1".to_string(),
            docs_count: split_metadata.as_split_info().num_docs as u64,
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ElasticsearchResolveIndexResponse {
    pub indices: Vec<ElasticsearchResolveIndexEntryResponse>,
    // Unused for the moment.
    pub aliases: Vec<serde_json::Value>,
    pub data_streams: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ElasticsearchResolveIndexEntryResponse {
    pub name: String,
    pub attributes: Vec<Status>,
}

impl From<IndexMetadata> for ElasticsearchResolveIndexEntryResponse {
    fn from(index_metadata: IndexMetadata) -> Self {
        ElasticsearchResolveIndexEntryResponse {
            name: index_metadata.index_config.index_id.to_string(),
            attributes: vec![Status::Open],
        }
    }
}

fn serialize_u64_as_string<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where S: Serializer {
    serializer.serialize_str(&value.to_string())
}

fn ser_es_format<S>(bytes: &u64, serializer: S) -> Result<S::Ok, S::Error>
where S: Serializer {
    serializer.serialize_str(&format_byte_size(*bytes))
}

fn format_byte_size(bytes: u64) -> String {
    const KILOBYTE: u64 = 1024;
    const MEGABYTE: u64 = KILOBYTE * 1024;
    const GIGABYTE: u64 = MEGABYTE * 1024;
    const TERABYTE: u64 = GIGABYTE * 1024;
    if bytes < KILOBYTE {
        format!("{}b", bytes)
    } else if bytes < MEGABYTE {
        format!("{:.1}kb", bytes as f64 / KILOBYTE as f64)
    } else if bytes < GIGABYTE {
        format!("{:.1}mb", bytes as f64 / MEGABYTE as f64)
    } else if bytes < TERABYTE {
        format!("{:.1}gb", bytes as f64 / GIGABYTE as f64)
    } else {
        format!("{:.1}tb", bytes as f64 / TERABYTE as f64)
    }
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Health {
    #[default]
    Green = 1,
    Yellow = 2,
    Red = 3,
}
impl AddAssign for Health {
    fn add_assign(&mut self, other: Self) {
        *self = match std::cmp::max(*self as u8, other as u8) {
            1 => Health::Green,
            2 => Health::Yellow,
            _ => Health::Red,
        };
    }
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    #[default]
    Open = 1,
}
impl AddAssign for Status {
    fn add_assign(&mut self, other: Self) {
        *self = match std::cmp::max(*self as u8, other as u8) {
            1 => Status::Open,
            _ => Status::Open,
        };
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_serialize_filtered() {
        let response = ElasticsearchCatIndexResponse {
            health: Health::Green,
            status: Status::Open,
            index: "test_index".to_string(),
            uuid: "test_uuid".to_string(),
            pri: "1".to_string(),
            rep: "2".to_string(),
            docs_count: 100,
            docs_deleted: 10,
            store_size: 1000,
            pri_store_size: 500,
            dataset_size: 1500,
        };

        // Test serialization with all fields
        let all_fields = response.serialize_filtered(&None).unwrap();
        let expected_all_fields = json!({
            "health": "green",
            "status": "open",
            "index": "test_index",
            "uuid": "test_uuid",
            "pri": "1",
            "rep": "2",
            "docs.count": "100",
            "docs.deleted": "10",
            "store.size": "1000b",  // Assuming ser_es_format formats size to kb
            "pri.store.size": "500b", // Example format
            "dataset.size": "1.5kb", // Example format
        });
        assert_eq!(all_fields, expected_all_fields);

        // Test serialization with selected fields
        let selected_fields = response
            .serialize_filtered(&Some(vec!["index".to_string(), "uuid".to_string()]))
            .unwrap();
        let expected_selected_fields = json!({
            "index": "test_index",
            "uuid": "test_uuid"
        });
        assert_eq!(selected_fields, expected_selected_fields);

        // Add more test cases as needed
    }
}
