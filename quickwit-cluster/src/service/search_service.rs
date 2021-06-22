//  Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use quickwit_proto::search_service_server::SearchService;

use quickwit_proto::{
    FetchDocsRequest, FetchDocsResult, LeafSearchRequest, LeafSearchResult, SearchRequest,
    SearchResult,
};

use crate::client_pool::search_client_pool::SearchClientPool;
use crate::cluster::Cluster;

/// Search service implementation.
#[allow(dead_code)]
pub struct SearchServiceImpl {
    /// Search client pool.
    client_pool: SearchClientPool,
}

impl SearchServiceImpl {
    /// Create a search service given a cluster.
    pub async fn new(cluster: Arc<Cluster>) -> anyhow::Result<Self> {
        let client_pool = SearchClientPool::new(cluster).await?;

        Ok(SearchServiceImpl { client_pool })
    }
}

#[tonic::async_trait]
impl SearchService for SearchServiceImpl {
    async fn root_search(
        &self,
        _request: Request<SearchRequest>,
    ) -> Result<Response<SearchResult>, Status> {
        unimplemented!();
    }

    async fn leaf_search(
        &self,
        _request: Request<LeafSearchRequest>,
    ) -> Result<Response<LeafSearchResult>, Status> {
        unimplemented!();
    }

    async fn fetch_docs(
        &self,
        _request: Request<FetchDocsRequest>,
    ) -> Result<Response<FetchDocsResult>, Status> {
        unimplemented!();
    }
}
