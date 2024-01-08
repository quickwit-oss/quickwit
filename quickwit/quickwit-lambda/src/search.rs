// Copyright (C) 2023 Quickwit, Inc.
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

use quickwit_proto::search::SearchResponse;
use quickwit_search::{single_node_search, SearchResponseRest};
use quickwit_serve::{
    search_request_from_api_request, BodyFormat, SearchRequestQueryString, SortBy,
};
use tracing::debug;

use crate::utils::load_node_config;

const CONFIGURATION_TEMPLATE: &str = "version: 0.6
node_id: lambda-searcher
metastore_uri: s3://${METASTORE_BUCKET}
default_index_root_uri: s3://${INDEX_BUCKET}
data_dir: /tmp
";

#[derive(Debug, Eq, PartialEq)]
pub struct SearchArgs {
    pub index_id: String,
    pub query: String,
    pub aggregation: Option<String>,
    pub max_hits: usize,
    pub start_offset: usize,
    pub search_fields: Option<Vec<String>>,
    pub snippet_fields: Option<Vec<String>>,
    pub start_timestamp: Option<i64>,
    pub end_timestamp: Option<i64>,
    pub sort_by_field: Option<String>,
}

pub async fn search(args: SearchArgs) -> anyhow::Result<SearchResponseRest> {
    debug!(args=?args, "lambda-search");
    let (_, storage_resolver, metastore) = load_node_config(CONFIGURATION_TEMPLATE).await?;
    let aggs = args
        .aggregation
        .map(|agg_string| serde_json::from_str(&agg_string))
        .transpose()?;
    let sort_by: SortBy = args.sort_by_field.map(SortBy::from).unwrap_or_default();
    let search_request_query_string = SearchRequestQueryString {
        query: args.query,
        start_offset: args.start_offset as u64,
        max_hits: args.max_hits as u64,
        search_fields: args.search_fields,
        snippet_fields: args.snippet_fields,
        start_timestamp: args.start_timestamp,
        end_timestamp: args.end_timestamp,
        aggs,
        format: BodyFormat::Json,
        sort_by,
    };
    let search_request =
        search_request_from_api_request(vec![args.index_id], search_request_query_string)?;
    debug!(search_request=?search_request, "search-request");
    let search_response: SearchResponse =
        single_node_search(search_request, metastore, storage_resolver).await?;
    let search_response_rest = SearchResponseRest::try_from(search_response)?;
    Ok(search_response_rest)
}
