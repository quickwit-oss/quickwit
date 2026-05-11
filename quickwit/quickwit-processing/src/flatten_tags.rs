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

use crate::string_or_vec::StringOrVec;

pub fn convert_tags(orig: &[String]) -> HashMap<String, StringOrVec> {
    let mut object_map: HashMap<String, StringOrVec> = HashMap::new();

    for tag in orig {
        if let Some(tag) = TagKV::parse_tag(tag) {
            object_map
                .entry(tag.key.to_string())
                .and_modify(|entry| match entry {
                    StringOrVec::String(existing_val) => {
                        let tag_values = vec![std::mem::take(existing_val), tag.value.to_string()];
                        *entry = StringOrVec::Vec(tag_values);
                    }
                    StringOrVec::Vec(vec) => vec.push(tag.value.to_string()),
                })
                .or_insert(StringOrVec::String(tag.value.to_string()));
        }
    }

    object_map
}

#[derive(Debug, Clone)]
/// Datadog tags are key-value pairs, separated by a colon, e.g. "env:prod"
pub struct TagKV<'a> {
    pub key: &'a str,
    pub value: &'a str,
}
impl TagKV<'_> {
    pub fn parse_tag(tag: &str) -> Option<TagKV<'_>> {
        tag.split_once(':').map(|(key, value)| TagKV { key, value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_value() {
        let input = vec!["color:blue".to_string(), "size:medium".to_string()];

        let result = convert_tags(&input);

        assert_eq!(
            result.get("color"),
            Some(&StringOrVec::String("blue".to_string()))
        );
        assert_eq!(
            result.get("size"),
            Some(&StringOrVec::String("medium".to_string()))
        );

        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_multi_value() {
        let input = vec![
            "color:blue".to_string(),
            "color:red".to_string(),
            "size:medium".to_string(),
        ];

        let result = convert_tags(&input);

        // Multi-value check: same key "color" appears twice
        // => values get converted into a Vec
        assert_eq!(
            result.get("color"),
            Some(&StringOrVec::Vec(vec![
                "blue".to_string(),
                "red".to_string(),
            ]))
        );
        assert_eq!(
            result.get("size"),
            Some(&StringOrVec::String("medium".to_string()))
        );

        assert_eq!(result.len(), 2);
    }
}
