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

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use quickwit_config::SearcherConfig;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_proto::search::{SearchRequest, SearchResponse};
use quickwit_search::{
    root_search, ClusterClient, Result as SearchResult, SearchJobPlacer, SearchResponseRest,
    SearchServiceClient, SearchServiceImpl, SearcherContext, SearcherPool,
};
use quickwit_serve::{search_request_from_api_request, SearchRequestQueryString};
use quickwit_storage::StorageResolver;
use tokio::sync::OnceCell;
use tracing::debug;

use super::environment::{CONFIGURATION_TEMPLATE, ENABLE_SEARCH_CACHE, INDEX_ID};
use crate::utils::load_node_config;

static LAMBDA_SEARCH_CACHE: OnceCell<LambdaSearchCtx> = OnceCell::const_new();

#[derive(Clone)]
struct LambdaSearchCtx {
    pub searcher_context: Arc<SearcherContext>,
    pub cluster_client: ClusterClient,
}

impl LambdaSearchCtx {
    async fn instantiate(
        metastore: MetastoreServiceClient,
        storage_resolver: StorageResolver,
    ) -> Self {
        let socket_addr = SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 7280u16);
        let searcher_pool = SearcherPool::default();
        let search_job_placer = SearchJobPlacer::new(searcher_pool.clone());
        let cluster_client = ClusterClient::new(search_job_placer);
        let searcher_config = SearcherConfig::default();
        let searcher_context = Arc::new(SearcherContext::new(searcher_config, None));
        let search_service = Arc::new(SearchServiceImpl::new(
            metastore,
            storage_resolver,
            cluster_client.clone(),
            searcher_context.clone(),
        ));
        let search_service_client =
            SearchServiceClient::from_service(search_service.clone(), socket_addr);
        searcher_pool.insert(socket_addr, search_service_client);
        Self {
            searcher_context,
            cluster_client,
        }
    }
}

async fn single_node_search(
    search_request: SearchRequest,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
) -> SearchResult<SearchResponse> {
    let lambda_search_ctx = if *ENABLE_SEARCH_CACHE {
        let cached_ctx = LAMBDA_SEARCH_CACHE
            .get_or_init(|| LambdaSearchCtx::instantiate(metastore.clone(), storage_resolver))
            .await;
        LambdaSearchCtx::clone(cached_ctx)
    } else {
        LambdaSearchCtx::instantiate(metastore.clone(), storage_resolver).await
    };
    root_search(
        &lambda_search_ctx.searcher_context,
        search_request,
        metastore,
        &lambda_search_ctx.cluster_client,
    )
    .await
}

#[derive(Debug, Eq, PartialEq)]
pub struct SearchArgs {
    pub query: SearchRequestQueryString,
}

pub async fn search(args: SearchArgs) -> anyhow::Result<SearchResponseRest> {
    debug!(args=?args, "lambda-search");
    let (_, storage_resolver, metastore) = load_node_config(CONFIGURATION_TEMPLATE).await?;
    let search_request = search_request_from_api_request(vec![INDEX_ID.clone()], args.query)?;
    debug!(search_request=?search_request, "search-request");
    let search_response: SearchResponse =
        single_node_search(search_request, metastore, storage_resolver).await?;
    let search_response_rest = SearchResponseRest::try_from(search_response)?;
    Ok(search_response_rest)
}
