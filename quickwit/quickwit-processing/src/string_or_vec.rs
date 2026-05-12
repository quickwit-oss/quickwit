// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum StringOrVec {
    String(String),
    Vec(Vec<String>),
}

impl StringOrVec {
    pub fn contains(&self, value: &str) -> bool {
        match self {
            StringOrVec::String(val) => val == value,
            StringOrVec::Vec(vec) => vec.iter().any(|elem| *elem == value),
        }
    }
}

impl From<StringOrVec> for serde_json::Value {
    fn from(value: StringOrVec) -> Self {
        match value {
            StringOrVec::String(val) => serde_json::Value::String(val),
            StringOrVec::Vec(vec) => {
                serde_json::Value::Array(vec.into_iter().map(serde_json::Value::String).collect())
            }
        }
    }
}

impl From<&str> for StringOrVec {
    fn from(value: &str) -> Self {
        StringOrVec::String(value.to_string())
    }
}

// TryFrom since not all serde_json::Value should be converted.
impl TryFrom<serde_json::Value> for StringOrVec {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        match value {
            serde_json::Value::String(val) => Ok(StringOrVec::String(val)),
            serde_json::Value::Array(vec) => {
                let strings: Vec<String> = vec
                    .into_iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect();
                Ok(StringOrVec::Vec(strings))
            }
            serde_json::Value::Null => Err("Null value".to_string()),
            serde_json::Value::Bool(val) => Ok(StringOrVec::String(val.to_string())),
            serde_json::Value::Number(val) => Ok(StringOrVec::String(val.to_string())),
            serde_json::Value::Object(_) => Err("Object value".to_string()),
        }
    }
}
