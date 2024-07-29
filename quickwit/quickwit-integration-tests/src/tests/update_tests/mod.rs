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

use quickwit_serve::SearchRequestQueryString;

use crate::test_utils::ClusterSandbox;

async fn assert_hit_count(
    sandbox: &ClusterSandbox,
    index_id: &str,
    query: &str,
    expected_hits: Result<u64, ()>,
) {
    let search_res = sandbox
        .searcher_rest_client
        .search(
            index_id,
            SearchRequestQueryString {
                query: query.to_string(),
                ..Default::default()
            },
        )
        .await;
    if let Ok(expected_hit_count) = expected_hits {
        let resp = search_res.unwrap_or_else(|_| panic!("query: {}", query));
        assert_eq!(resp.errors.len(), 0, "query: {}", query);
        assert_eq!(resp.num_hits, expected_hit_count, "query: {}", query);
    } else if let Ok(search_response) = search_res {
        assert!(!search_response.errors.is_empty(), "query: {}", query);
    }
}

mod doc_mapping_tests;
mod search_settings_tests;
