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
/// Both fields are optional and absent by default. When present, they are
/// forwarded into [`quickwit_proto::search::ListFieldsRequest`] verbatim,
/// where they prune the set of splits considered for field discovery. Unit
/// is **epoch seconds**, interval is half-open `[start_timestamp,
/// end_timestamp)` — matching the `ListFieldsRequest` proto contract
/// exactly.
#[serde_with::skip_serializing_none]
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexMappingQueryParams {
    #[serde(default)]
    pub start_timestamp: Option<i64>,
    #[serde(default)]
    pub end_timestamp: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::IndexMappingQueryParams;

    #[test]
    fn empty_query_string_yields_none() {
        let params: IndexMappingQueryParams = serde_urlencoded::from_str("").unwrap();
        assert!(params.start_timestamp.is_none());
        assert!(params.end_timestamp.is_none());
    }

    #[test]
    fn both_params_present_yield_some() {
        let qs = "start_timestamp=1712160204&end_timestamp=1712764984";
        let params: IndexMappingQueryParams = serde_urlencoded::from_str(qs).unwrap();
        assert_eq!(params.start_timestamp, Some(1712160204));
        assert_eq!(params.end_timestamp, Some(1712764984));
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
    fn unknown_field_is_rejected() {
        let qs = "start_timestamp=1&unexpected=foo";
        let result: Result<IndexMappingQueryParams, _> = serde_urlencoded::from_str(qs);
        assert!(result.is_err());
    }
}
