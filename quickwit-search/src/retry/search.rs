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

use quickwit_proto::{LeafSearchRequest, LeafSearchResult};

use super::RetryPolicy;
use crate::SearchError;

/// Retry policy for LeafSearchRequest.
/// A retry is made either on an error or if there are some failing splits.
/// In the last case, a retry request is built on failing splits only.
pub struct LeafSearchRetryPolicy {}

impl RetryPolicy<LeafSearchRequest, LeafSearchResult, SearchError> for LeafSearchRetryPolicy {
    // Retry either on error or if there are failing split ids AND if attempts > 0.
    fn should_retry(
        &self,
        _request: &LeafSearchRequest,
        result: Result<&LeafSearchResult, &SearchError>,
    ) -> bool {
        match result {
            Ok(response) => !response.failed_splits.is_empty(),
            Err(_) => true,
        }
    }

    // Build a retry request on failing split ids only.
    fn retry_request(
        &self,
        request: &LeafSearchRequest,
        result: Result<&LeafSearchResult, &SearchError>,
    ) -> LeafSearchRequest {
        let mut request_clone = request.clone();
        match result {
            Ok(response) => {
                if !response.failed_splits.is_empty() {
                    request_clone.split_metadata.retain(|split_metadata| {
                        response
                            .failed_splits
                            .iter()
                            .any(|failed_split| failed_split.split_id == split_metadata.split_id)
                    });
                }
                request_clone
            }
            Err(_) => request_clone,
        }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::{
        LeafSearchRequest, LeafSearchResult, SearchRequest, SplitIdAndFooterOffsets,
        SplitSearchError,
    };

    use crate::retry::search::LeafSearchRetryPolicy;
    use crate::retry::RetryPolicy;
    use crate::SearchError;

    fn mock_leaf_search_request() -> LeafSearchRequest {
        LeafSearchRequest {
            search_request: Some(SearchRequest {
                index_id: "test-idx".to_string(),
                query: "test".to_string(),
                search_fields: vec!["body".to_string()],
                start_timestamp: None,
                end_timestamp: None,
                max_hits: 10,
                start_offset: 0,
                tags: vec![],
            }),
            index_config: "index_config".to_string(),
            index_uri: "uri".to_string(),
            split_metadata: vec![
                SplitIdAndFooterOffsets {
                    split_id: "split_1".to_string(),
                    split_footer_end: 100,
                    split_footer_start: 0,
                },
                SplitIdAndFooterOffsets {
                    split_id: "split_2".to_string(),
                    split_footer_end: 100,
                    split_footer_start: 0,
                },
            ],
        }
    }

    #[test]
    fn test_should_retry_on_error() -> anyhow::Result<()> {
        let retry_policy = LeafSearchRetryPolicy {};
        let request = mock_leaf_search_request();
        let result = Result::<LeafSearchResult, SearchError>::Err(SearchError::InternalError(
            "test".to_string(),
        ));
        let retry = retry_policy.should_retry(&request, result.as_ref());
        assert!(retry);
        Ok(())
    }

    #[test]
    fn test_should_not_retry_if_result_is_ok_and_no_failing_splits() -> anyhow::Result<()> {
        let retry_policy = LeafSearchRetryPolicy {};
        let request = mock_leaf_search_request();
        let leaf_response = LeafSearchResult {
            num_hits: 0,
            partial_hits: vec![],
            failed_splits: vec![],
            num_attempted_splits: 1,
        };
        let result = Result::<LeafSearchResult, SearchError>::Ok(leaf_response);
        let retry = retry_policy.should_retry(&request, result.as_ref());
        assert!(!retry);
        Ok(())
    }

    #[test]
    fn test_should_retry_on_failed_splits() -> anyhow::Result<()> {
        let retry_policy = LeafSearchRetryPolicy {};
        let request = mock_leaf_search_request();
        let mut expected_retry_request = request.clone();
        expected_retry_request.split_metadata.remove(0);
        let split_error = SplitSearchError {
            error: "error".to_string(),
            split_id: "split_2".to_string(),
            retryable_error: true,
        };
        let leaf_response = LeafSearchResult {
            num_hits: 0,
            partial_hits: vec![],
            failed_splits: vec![split_error],
            num_attempted_splits: 1,
        };
        let result = Result::<LeafSearchResult, SearchError>::Ok(leaf_response);
        let retry = retry_policy.should_retry(&request, result.as_ref());
        let retry_request = retry_policy.retry_request(&request, result.as_ref());
        assert!(retry);
        assert_eq!(retry_request, expected_retry_request);
        Ok(())
    }
}
