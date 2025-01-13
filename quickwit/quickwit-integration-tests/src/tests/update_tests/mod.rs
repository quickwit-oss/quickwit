// Copyright (C) 2024 Quickwit, Inc.
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

use quickwit_config::service::QuickwitService;
use quickwit_serve::SearchRequestQueryString;
use serde_json::Value;

use crate::test_utils::ClusterSandbox;

/// Checks that the result of the given query matches the expected values
async fn assert_hits_unordered(
    sandbox: &ClusterSandbox,
    index_id: &str,
    query: &str,
    expected_result: Result<&[Value], ()>,
) {
    let search_res = sandbox
        .rest_client(QuickwitService::Searcher)
        .search(
            index_id,
            SearchRequestQueryString {
                query: query.to_string(),
                max_hits: expected_result.map(|hits| hits.len() as u64).unwrap_or(1),
                ..Default::default()
            },
        )
        .await;
    if let Ok(expected_hits) = expected_result {
        let resp = search_res.unwrap_or_else(|err| panic!("query: {}, error: {}", query, err));
        assert_eq!(resp.errors.len(), 0, "query: {}", query);
        assert_eq!(
            resp.num_hits,
            expected_hits.len() as u64,
            "query: {}",
            query
        );
        for expected_hit in expected_hits {
            assert!(
                resp.hits.contains(expected_hit),
                "query: {} -> expected hits: {:?}, got: {:?}",
                query,
                expected_hits,
                resp.hits
            );
        }
    } else if let Ok(search_response) = search_res {
        assert!(!search_response.errors.is_empty(), "query: {}", query);
    }
}

mod doc_mapping_tests;
mod restart_indexer_tests;
mod search_settings_tests;
