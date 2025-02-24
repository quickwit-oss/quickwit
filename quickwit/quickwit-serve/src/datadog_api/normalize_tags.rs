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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum StringOrVec {
    String(String),
    Vec(Vec<String>),
}

#[allow(dead_code)]
impl StringOrVec {
    pub fn contains(&self, value: &str) -> bool {
        match self {
            StringOrVec::String(val) => val == value,
            StringOrVec::Vec(vec) => vec.contains(&value.to_string()),
        }
    }
}

pub fn convert_tags(orig: &[String]) -> HashMap<String, StringOrVec> {
    let mut object_map: HashMap<String, StringOrVec> = HashMap::new();

    for tag in orig {
        if let Some(pos) = tag.find(':') {
            let key = &tag[..pos];
            let value = &tag[pos + 1..];
            object_map
                .entry(key.to_string())
                .and_modify(|e| match e {
                    StringOrVec::String(existing_val) => {
                        // If the key already exists, use an array to store the values
                        let vec = vec![std::mem::take(existing_val), value.to_string()];
                        *e = StringOrVec::Vec(vec);
                    }
                    StringOrVec::Vec(vec) => vec.push(value.to_string()),
                })
                .or_insert(StringOrVec::String(value.to_string()));
        }
    }

    object_map
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::datadog_api::rest_handler::{DatadogLogMsg, ProcessedLog};

    /// Test the `StringOrVec` serde_json serialization
    #[test]
    fn test_string_or_vec_serde() {
        let sov = StringOrVec::String("hello".to_string());
        let json = serde_json::to_string(&sov).unwrap();
        assert_eq!(json, r#""hello""#);

        let sov = StringOrVec::Vec(vec!["hello".to_string(), "world".to_string()]);
        let json = serde_json::to_string(&sov).unwrap();
        assert_eq!(json, r#"["hello","world"]"#);
    }

    /// A simple test to ensure `ProcessedLog::from_datadog_log_msg` copies and transforms fields.
    #[test]
    fn test_processed_log_basic() {
        let msg = DatadogLogMsg {
            message: "".to_string(),
            status: Some("".to_string()),
            timestamp: chrono::Utc::now(),
            hostname: "".to_string(),
            service: "".to_string(),
            ddsource: "".to_string(),
            ddtags: Some(vec![
                "env:dev".into(),
                "region:us-east".into(),
                "region:east".into(),
            ]),
        };
        let processed = ProcessedLog::from_datadog_log_msg(msg.clone());

        let expected_map = HashMap::from_iter(vec![
            ("env".to_string(), StringOrVec::String("dev".to_string())),
            (
                "region".to_string(),
                StringOrVec::Vec(vec!["us-east".to_string(), "east".to_string()]),
            ),
        ]);
        assert_eq!(processed.tag, expected_map);
    }
}
