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

/// Query parameters accepted by `GET /_elastic/{index}/_mapping` and
/// `/_mappings`.
///
/// All fields are optional and absent by default. Unknown query
/// parameters are silently ignored to stay compatible with standard
/// Elasticsearch clients that pass extras like `pretty`,
/// `ignore_unavailable`, or `allow_no_indices`.
///
/// `start_timestamp` and `end_timestamp` are forwarded into
/// [`quickwit_proto::search::ListFieldsRequest`] verbatim, where they prune
/// the set of splits considered for field discovery. Unit is **epoch
/// seconds**, interval is half-open `[start_timestamp, end_timestamp)` —
/// matching the `ListFieldsRequest` proto contract exactly.
///
/// `fields` is an optional comma-separated list of column names. When
/// every requested name is a flat literal declared in some index's
/// `doc_mapping`, the handler filters the mapping response to those
/// columns and skips the per-split `list_fields` lookup entirely. Any
/// other shape (wildcards, dotted paths, or names not in `doc_mapping`)
/// falls back to listing all fields from the splits in the time range
/// and returning the full mapping — `fields=` then acts as a request
/// hint, not a strict filter. An empty value (`fields=`) is treated the
/// same as the parameter being absent.
#[serde_with::skip_serializing_none]
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct IndexMappingQueryParams {
    #[serde(default)]
    pub start_timestamp: Option<i64>,
    #[serde(default)]
    pub end_timestamp: Option<i64>,
    #[serde(default)]
    pub fields: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::IndexMappingQueryParams;

    #[test]
    fn empty_query_string_yields_none() {
        let params: IndexMappingQueryParams = serde_urlencoded::from_str("").unwrap();
        assert!(params.start_timestamp.is_none());
        assert!(params.end_timestamp.is_none());
        assert!(params.fields.is_none());
    }

    #[test]
    fn both_params_present_yield_some() {
        let qs = "start_timestamp=1712160204&end_timestamp=1712764984";
        let params: IndexMappingQueryParams = serde_urlencoded::from_str(qs).unwrap();
        assert_eq!(params.start_timestamp, Some(1712160204));
        assert_eq!(params.end_timestamp, Some(1712764984));
        assert!(params.fields.is_none());
    }

    #[test]
    fn only_start_timestamp_present() {
        let qs = "start_timestamp=1712160204";
        let params: IndexMappingQueryParams = serde_urlencoded::from_str(qs).unwrap();
        assert_eq!(params.start_timestamp, Some(1712160204));
        assert!(params.end_timestamp.is_none());
    }

    #[test]
    fn only_end_timestamp_present() {
        let qs = "end_timestamp=1712764984";
        let params: IndexMappingQueryParams = serde_urlencoded::from_str(qs).unwrap();
        assert!(params.start_timestamp.is_none());
        assert_eq!(params.end_timestamp, Some(1712764984));
    }

    #[test]
    fn unknown_field_is_ignored() {
        // ES clients commonly pass `pretty`, `ignore_unavailable`,
        // `allow_no_indices`, etc. on `_mapping`. We ignore them rather
        // than reject the request, matching the pre-column-hints route
        // which did not parse the query string at all.
        let qs = "start_timestamp=1&pretty=true&ignore_unavailable=true";
        let params: IndexMappingQueryParams = serde_urlencoded::from_str(qs).unwrap();
        assert_eq!(params.start_timestamp, Some(1));
        assert!(params.end_timestamp.is_none());
        assert!(params.fields.is_none());
    }

    #[test]
    fn fields_param_present() {
        let qs = "fields=host,message,status";
        let params: IndexMappingQueryParams = serde_urlencoded::from_str(qs).unwrap();
        assert_eq!(params.fields.as_deref(), Some("host,message,status"));
    }

    #[test]
    fn fields_combined_with_timestamps() {
        let qs = "start_timestamp=1&end_timestamp=2&fields=host";
        let params: IndexMappingQueryParams = serde_urlencoded::from_str(qs).unwrap();
        assert_eq!(params.start_timestamp, Some(1));
        assert_eq!(params.end_timestamp, Some(2));
        assert_eq!(params.fields.as_deref(), Some("host"));
    }

    #[test]
    fn empty_fields_value() {
        let qs = "fields=";
        let params: IndexMappingQueryParams = serde_urlencoded::from_str(qs).unwrap();
        // Empty string is parsed as Some(""); the handler treats it as
        // "absent" (legacy full-mapping behavior). See
        // `parse_field_hints` in `rest_handler.rs`.
        assert_eq!(params.fields.as_deref(), Some(""));
    }
}
