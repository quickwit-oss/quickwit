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
use quickwit_serve::{search_request_from_api_request, SearchRequestQueryString};
use tracing::debug;

use crate::utils::load_node_config;

const CONFIGURATION_TEMPLATE: &str = "version: 0.6
node_id: lambda-searcher
metastore_uri: s3://${QW_LAMBDA_METASTORE_BUCKET}/index
default_index_root_uri: s3://${QW_LAMBDA_INDEX_BUCKET}/index
data_dir: /tmp
";

#[derive(Debug, Eq, PartialEq)]
pub struct SearchArgs {
    pub index_id: String,
    pub query: SearchRequestQueryString,
}

pub async fn search(args: SearchArgs) -> anyhow::Result<SearchResponseRest> {
    debug!(args=?args, "lambda-search");
    let (_, storage_resolver, metastore) = load_node_config(CONFIGURATION_TEMPLATE).await?;
    let search_request = search_request_from_api_request(vec![args.index_id], args.query)?;
    debug!(search_request=?search_request, "search-request");
    let search_response: SearchResponse =
        single_node_search(search_request, metastore, storage_resolver).await?;
    let search_response_rest = SearchResponseRest::try_from(search_response)?;
    Ok(search_response_rest)
}
