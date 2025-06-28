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
        let resp = search_res.unwrap_or_else(|err| panic!("query: {query}, error: {err}"));
        assert_eq!(resp.errors.len(), 0, "query: {query}");
        assert_eq!(resp.num_hits, expected_hits.len() as u64, "query: {query}");
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
        assert!(!search_response.errors.is_empty(), "query: {query}");
    }
}

mod create_on_update;
mod doc_mapping_tests;
mod restart_indexer_tests;
mod search_settings_tests;
