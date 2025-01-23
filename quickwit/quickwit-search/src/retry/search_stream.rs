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

use quickwit_proto::search::{LeafSearchStreamRequest, LeafSearchStreamResponse};
use tokio::sync::mpsc::error::SendError;
use tracing::warn;

use super::RetryPolicy;

pub struct SuccessfulSplitIds(pub Vec<String>);

/// Retry policy for consuming the result stream of a LeafSearchStreamRequest.
/// A retry is only made if there are some missing splits.
/// As errors only come from a closed receiver, we ignore them.
pub struct LeafSearchStreamRetryPolicy {}

impl
    RetryPolicy<
        LeafSearchStreamRequest,
        SuccessfulSplitIds,
        SendError<crate::Result<LeafSearchStreamResponse>>,
    > for LeafSearchStreamRetryPolicy
{
    // Returns a retry request that is either:
    // - a clone of the initial request on error
    // - or a request on failing split ids only.
    fn retry_request(
        &self,
        mut request: LeafSearchStreamRequest,
        response_res: &Result<
            SuccessfulSplitIds,
            SendError<crate::Result<LeafSearchStreamResponse>>,
        >,
    ) -> Option<LeafSearchStreamRequest> {
        match response_res {
            Ok(SuccessfulSplitIds(successful_split_ids)) => {
                if successful_split_ids.len() == request.split_offsets.len() {
                    // All splits were successful!
                    return None;
                }
                // We retry the failed splits.
                request.split_offsets.retain(|split_metadata| {
                    !successful_split_ids.contains(&split_metadata.split_id)
                });
                Some(request)
            }
            Err(SendError(_)) => {
                // The receiver channel was dropped.
                // There is no need to retry.
                warn!(
                    "Receiver channel closed during stream search request. The client probably \
                     closed the connection?"
                );
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::search::{
        LeafSearchStreamRequest, LeafSearchStreamResponse, SplitIdAndFooterOffsets,
    };
    use tokio::sync::mpsc::error::SendError;

    use crate::retry::search_stream::{LeafSearchStreamRetryPolicy, SuccessfulSplitIds};
    use crate::retry::RetryPolicy;

    #[tokio::test]
    async fn test_retry_policy_search_stream_should_not_retry_on_send_error() {
        let retry_policy = LeafSearchStreamRetryPolicy {};
        let request = LeafSearchStreamRequest::default();
        let response = LeafSearchStreamResponse::default();
        let response_res = Err(SendError(Ok(response)));
        let retry_req_opt = retry_policy.retry_request(request, &response_res);
        assert!(retry_req_opt.is_none());
    }

    #[tokio::test]
    async fn test_retry_policy_search_stream_should_not_retry_on_successful_response() {
        let retry_policy = LeafSearchStreamRetryPolicy {};
        let request = LeafSearchStreamRequest::default();
        let response_res = Ok(SuccessfulSplitIds(Vec::new()));
        let retry_req_opt = retry_policy.retry_request(request, &response_res);
        assert!(retry_req_opt.is_none());
    }

    #[tokio::test]
    async fn test_retry_policy_search_stream_should_retry_on_failed_splits() {
        let split_1 = SplitIdAndFooterOffsets {
            split_id: "split_1".to_string(),
            split_footer_end: 100,
            split_footer_start: 0,
            timestamp_start: None,
            timestamp_end: None,
            num_docs: 0,
        };
        let split_2 = SplitIdAndFooterOffsets {
            split_id: "split_2".to_string(),
            split_footer_end: 100,
            split_footer_start: 0,
            timestamp_start: None,
            timestamp_end: None,
            num_docs: 0,
        };
        let retry_policy = LeafSearchStreamRetryPolicy {};
        let request = LeafSearchStreamRequest {
            split_offsets: vec![split_1, split_2],
            ..Default::default()
        };
        let response_res = Ok(SuccessfulSplitIds(vec!["split_1".to_string()]));
        let retry_req = retry_policy.retry_request(request, &response_res).unwrap();
        assert_eq!(retry_req.split_offsets.len(), 1);
        assert_eq!(retry_req.split_offsets[0].split_id, "split_2");
    }
}
