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

use std::collections::HashSet;

use quickwit_proto::search::{LeafSearchRequest, LeafSearchResponse};

use super::RetryPolicy;
use crate::SearchError;

/// Retry policy for LeafSearchRequest.
/// A retry is made either on an error or if there are some failing splits.
/// In the last case, a retry request is built on failing splits only.
pub struct LeafSearchRetryPolicy {}

impl RetryPolicy<LeafSearchRequest, LeafSearchResponse, SearchError> for LeafSearchRetryPolicy {
    // Build a retry request on failing split ids only.
    fn retry_request(
        &self,
        mut request: LeafSearchRequest,
        response_res: &Result<LeafSearchResponse, SearchError>,
    ) -> Option<LeafSearchRequest> {
        match response_res {
            Ok(response) => {
                if response.failed_splits.is_empty() {
                    return None;
                }
                let failed_splits_hash_set: HashSet<&str> = response
                    .failed_splits
                    .iter()
                    .map(|failed_split| failed_split.split_id.as_str())
                    .collect();
                for request in request.leaf_requests.iter_mut() {
                    // Keep only failed splits
                    request.split_offsets.retain(|split_metadata| {
                        failed_splits_hash_set.contains(split_metadata.split_id.as_str())
                    });
                }
                // Remove requests with empty split_offsets
                request
                    .leaf_requests
                    .retain(|request| !request.split_offsets.is_empty());
                Some(request)
            }
            Err(SearchError::Timeout(_)) => None, // Don't retry on timeout
            Err(_) => Some(request),
        }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::search::{
        LeafRequestRef, LeafSearchRequest, LeafSearchResponse, SearchRequest,
        SplitIdAndFooterOffsets, SplitSearchError,
    };
    use quickwit_query::query_ast::qast_json_helper;

    use crate::SearchError;
    use crate::retry::RetryPolicy;
    use crate::retry::search::LeafSearchRetryPolicy;

    fn mock_leaf_search_request() -> LeafSearchRequest {
        let search_request = SearchRequest {
            index_id_patterns: vec!["test-idx".to_string()],
            query_ast: qast_json_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        LeafSearchRequest {
            search_request: Some(search_request),
            doc_mappers: vec!["doc_mapper".to_string()],
            index_uris: vec!["uri".to_string()],
            leaf_requests: vec![LeafRequestRef {
                index_uri_ord: 0,
                doc_mapper_ord: 0,
                split_offsets: vec![
                    SplitIdAndFooterOffsets {
                        split_id: "split_1".to_string(),
                        split_footer_start: 0,
                        split_footer_end: 100,
                        timestamp_start: None,
                        timestamp_end: None,
                        num_docs: 0,
                    },
                    SplitIdAndFooterOffsets {
                        split_id: "split_2".to_string(),
                        split_footer_start: 0,
                        split_footer_end: 100,
                        timestamp_start: None,
                        timestamp_end: None,
                        num_docs: 0,
                    },
                ],
            }],
        }
    }

    #[test]
    fn test_should_retry_on_error() {
        let retry_policy = LeafSearchRetryPolicy {};
        let request = mock_leaf_search_request();
        let response_res = Result::<LeafSearchResponse, SearchError>::Err(SearchError::Internal(
            "test".to_string(),
        ));
        retry_policy.retry_request(request, &response_res).unwrap();
    }

    #[test]
    fn test_should_not_retry_if_result_is_ok_and_no_failing_splits() {
        let retry_policy = LeafSearchRetryPolicy {};
        let request = mock_leaf_search_request();
        let response_res = Ok(LeafSearchResponse {
            num_hits: 0,
            partial_hits: Vec::new(),
            failed_splits: Vec::new(),
            num_attempted_splits: 1,
            ..Default::default()
        });
        assert!(retry_policy.retry_request(request, &response_res).is_none())
    }

    #[test]
    fn test_should_retry_on_failed_splits() {
        let retry_policy = LeafSearchRetryPolicy {};
        let request = mock_leaf_search_request();
        let mut expected_retry_request = request.clone();
        expected_retry_request.leaf_requests[0]
            .split_offsets
            .remove(0);
        let split_error = SplitSearchError {
            error: "error".to_string(),
            split_id: "split_2".to_string(),
            retryable_error: true,
        };
        let response_res = Ok(LeafSearchResponse {
            num_hits: 0,
            partial_hits: Vec::new(),
            failed_splits: vec![split_error],
            num_attempted_splits: 1,
            ..Default::default()
        });
        let retry_request = retry_policy.retry_request(request, &response_res).unwrap();
        assert_eq!(retry_request, expected_retry_request);
    }
}
