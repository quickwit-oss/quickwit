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

use crate::StringOrVec;

pub fn convert_tags(orig: &Vec<String>) -> HashMap<String, StringOrVec> {
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

    #[test]
    fn test_single_value() {
        let input = vec!["color:blue".to_string(), "size:medium".to_string()];

        let result = convert_tags(&input);

        // Single-value checks
        assert_eq!(
            result.get("color"),
            Some(&StringOrVec::String("blue".to_string()))
        );
        assert_eq!(
            result.get("size"),
            Some(&StringOrVec::String("medium".to_string()))
        );

        // Make sure we've inserted only these two entries
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
        // The "size" key remains a single value
        assert_eq!(
            result.get("size"),
            Some(&StringOrVec::String("medium".to_string()))
        );

        // Make sure we've inserted only these two keys
        assert_eq!(result.len(), 2);
    }
}
