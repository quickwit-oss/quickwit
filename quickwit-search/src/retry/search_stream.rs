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

use quickwit_proto::{LeafSearchStreamRequest, LeafSearchStreamResponse};
use tokio::sync::mpsc::error::SendError;
use tracing::warn;

use super::RetryPolicy;

pub struct SuccessfullSplitIds(pub Vec<String>);

/// Retry policy for consuming the result stream of a LeafSearchStreamRequest.
/// A retry is only made if there are some missing splits.
/// As errors only come from a closed receiver, we ignore them.
pub struct LeafSearchStreamRetryPolicy {}

impl
    RetryPolicy<
        LeafSearchStreamRequest,
        SuccessfullSplitIds,
        SendError<crate::Result<LeafSearchStreamResponse>>,
    > for LeafSearchStreamRetryPolicy
{
    // Returns a retry request that is either:
    // - a clone of the initial request on error
    // - or a request on failing split ids only.
    fn retry_request(
        &self,
        mut request: LeafSearchStreamRequest,
        result: Result<&SuccessfullSplitIds, &SendError<crate::Result<LeafSearchStreamResponse>>>,
    ) -> Option<LeafSearchStreamRequest> {
        match result {
            Ok(SuccessfullSplitIds(successful_split_ids)) => {
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
    use quickwit_proto::{
        LeafSearchStreamRequest, LeafSearchStreamResponse, SplitIdAndFooterOffsets,
    };
    use tokio::sync::mpsc::error::SendError;

    use crate::retry::search_stream::{LeafSearchStreamRetryPolicy, SuccessfullSplitIds};
    use crate::retry::RetryPolicy;

    #[tokio::test]
    async fn test_retry_policy_search_stream_should_not_retry_on_send_error() {
        let retry_policy = LeafSearchStreamRetryPolicy {};
        let leaf_search_stream_req = LeafSearchStreamRequest::default();
        let leaf_search_stream_res = LeafSearchStreamResponse::default();
        let leaf_search_stream_send_error = Err(SendError(Ok(leaf_search_stream_res)));
        let retry_req_opt = retry_policy.retry_request(
            leaf_search_stream_req,
            leaf_search_stream_send_error.as_ref(),
        );
        assert!(retry_req_opt.is_none());
    }

    #[tokio::test]
    async fn test_retry_policy_search_stream_should_not_retry_on_successful_response() {
        let retry_policy = LeafSearchStreamRetryPolicy {};
        let leaf_search_stream_req = LeafSearchStreamRequest::default();
        let successful_split_ids: Vec<String> = Vec::new();
        let retry_req_opt = retry_policy.retry_request(
            leaf_search_stream_req,
            Ok(SuccessfullSplitIds(successful_split_ids)).as_ref(),
        );
        assert!(retry_req_opt.is_none());
    }

    #[tokio::test]
    async fn test_retry_policy_search_stream_should_retry_on_failed_splits() {
        let splits = vec![
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
        ];

        let retry_policy = LeafSearchStreamRetryPolicy {};
        let mut leaf_search_stream_req = LeafSearchStreamRequest::default();
        leaf_search_stream_req.split_offsets.push(splits[0].clone());
        leaf_search_stream_req.split_offsets.push(splits[1].clone());
        let successful_split_ids: Vec<String> = vec![splits[0].split_id.clone()];
        let retry_req_opt = retry_policy.retry_request(
            leaf_search_stream_req,
            Ok(SuccessfullSplitIds(successful_split_ids)).as_ref(),
        );
        assert!(retry_req_opt.is_some());
        assert!(retry_req_opt.as_ref().unwrap().split_offsets.len() == 1);
        assert!(retry_req_opt.as_ref().unwrap().split_offsets[0].split_id == "split_2".to_string());
    }
}
