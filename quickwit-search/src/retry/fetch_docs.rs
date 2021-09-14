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



#[cfg(test)]
mod tests {
    use quickwit_proto::{FetchDocsRequest, FetchDocsResult, SplitIdAndFooterOffsets};
    use super::FetchDocsRetryPolicy;
    use crate::retry::RetryPolicy;
    use crate::{MockSearchService, SearchError};

    fn mock_doc_request() -> FetchDocsRequest {
        FetchDocsRequest {
            partial_hits: vec![],
            index_id: "id".to_string(),
            index_uri: "uri".to_string(),
            split_metadata: vec![SplitIdAndFooterOffsets {
                split_id: "split_1".to_string(),
                split_footer_end: 100,
                split_footer_start: 0,
            }],
        }
    }

    #[tokio::test]
    async fn test_should_retry_and_attempts_should_decrease() -> anyhow::Result<()> {
        let retry_policy = FetchDocsRetryPolicy::new(1);
        let request = mock_doc_request();
        let result = Err(SearchError::InternalError("test".to_string()));
        let retry = retry_policy.retry(&request, result.as_ref()).await;
        assert!(retry.is_some());
        assert_eq!(retry.unwrap().attempts, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_should_not_retry_if_attempts_is_0() -> anyhow::Result<()> {
        let retry_policy = FetchDocsRetryPolicy::new(0);
        let request = mock_doc_request();
        let result = Err(SearchError::InternalError("test".to_string()));
        let retry = retry_policy.retry(&request, result.as_ref()).await;
        assert!(retry.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_should_not_retry_if_result_is_ok() -> anyhow::Result<()> {
        let retry_policy = FetchDocsRetryPolicy::new(1);
        let request = mock_doc_request();
        let result = Ok(FetchDocsResult { hits: vec![] });
        let retry = retry_policy.retry(&request, result.as_ref()).await;
        assert!(retry.is_none());
        Ok(())
    }
}
