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

/// Query parameters for `_mapping(s)`. Unknown params are silently ignored.
///
/// Timestamps (`start_timestamp`, `end_timestamp`) are epoch seconds,
/// half-open `[start, end)`, forwarded to `ListFieldsRequest` to prune splits.
/// `field_patterns` is a comma-separated list mirroring
/// `ListFieldsRequest.field_patterns`, pushed down to the leaves for
/// dynamic-field filtering.
#[serde_with::skip_serializing_none]
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct IndexMappingQueryParams {
    #[serde(default)]
    pub start_timestamp: Option<i64>,
    #[serde(default)]
    pub end_timestamp: Option<i64>,
    #[serde(default)]
    pub field_patterns: Option<String>,
}

impl IndexMappingQueryParams {
    /// Splits `field_patterns` on commas, trims, and drops empties.
    /// Returns an empty `Vec` when the parameter is absent or blank.
    pub fn field_patterns(&self) -> Vec<String> {
        self.field_patterns
            .as_deref()
            .unwrap_or_default()
            .split(',')
            .filter_map(|pattern| {
                let trimmed = pattern.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::IndexMappingQueryParams;

    #[test]
    fn empty_query_string_yields_none() {
        let params: IndexMappingQueryParams = serde_qs::from_str("").unwrap();
        assert!(params.start_timestamp.is_none());
        assert!(params.end_timestamp.is_none());
        assert!(params.field_patterns.is_none());
    }

    #[test]
    fn both_params_present_yield_some() {
        let qs = "start_timestamp=1712160204&end_timestamp=1712764984";
        let params: IndexMappingQueryParams = serde_qs::from_str(qs).unwrap();
        assert_eq!(params.start_timestamp, Some(1712160204));
        assert_eq!(params.end_timestamp, Some(1712764984));
        assert!(params.field_patterns.is_none());
    }

    #[test]
    fn only_start_timestamp_present() {
        let qs = "start_timestamp=1712160204";
        let params: IndexMappingQueryParams = serde_qs::from_str(qs).unwrap();
        assert_eq!(params.start_timestamp, Some(1712160204));
        assert!(params.end_timestamp.is_none());
    }

    #[test]
    fn only_end_timestamp_present() {
        let qs = "end_timestamp=1712764984";
        let params: IndexMappingQueryParams = serde_qs::from_str(qs).unwrap();
        assert!(params.start_timestamp.is_none());
        assert_eq!(params.end_timestamp, Some(1712764984));
    }

    #[test]
    fn unknown_field_is_ignored() {
        let qs = "start_timestamp=1&pretty=true&ignore_unavailable=true";
        let params: IndexMappingQueryParams = serde_qs::from_str(qs).unwrap();
        assert_eq!(params.start_timestamp, Some(1));
        assert!(params.end_timestamp.is_none());
        assert!(params.field_patterns.is_none());
    }

    #[test]
    fn field_patterns_param_present() {
        let qs = "field_patterns=host,message,status";
        let params: IndexMappingQueryParams = serde_qs::from_str(qs).unwrap();
        assert_eq!(
            params.field_patterns.as_deref(),
            Some("host,message,status")
        );
    }

    #[test]
    fn field_patterns_combined_with_timestamps() {
        let qs = "start_timestamp=1&end_timestamp=2&field_patterns=host";
        let params: IndexMappingQueryParams = serde_qs::from_str(qs).unwrap();
        assert_eq!(params.start_timestamp, Some(1));
        assert_eq!(params.end_timestamp, Some(2));
        assert_eq!(params.field_patterns.as_deref(), Some("host"));
    }

    #[test]
    fn empty_field_patterns_value() {
        // `serde_qs` collapses an empty value to `None`.
        let qs = "field_patterns=";
        let params: IndexMappingQueryParams = serde_qs::from_str(qs).unwrap();
        assert!(params.field_patterns.is_none());
    }
}
