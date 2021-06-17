/*
 * Copyright (C) 2021 Quickwit Inc.
 *
 * Quickwit is offered under the AGPL v3.0 and as commercial software.
 * For commercial licensing, contact us at hello@quickwit.io.
 *
 * AGPL:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

use std::sync::Arc;

use async_trait::async_trait;
use quickwit_proto::search_service_server as grpc;
use quickwit_proto::FetchDocsRequest;
use quickwit_proto::FetchDocsResult;
use quickwit_proto::LeafSearchRequest;
use quickwit_proto::LeafSearchResult;
use quickwit_proto::SearchRequest;
use quickwit_proto::SearchResult;
use quickwit_search::SearchError;
use quickwit_search::SearchService;

#[derive(Clone)]
pub struct GrpcAdapter(Arc<dyn SearchService>);

impl From<Arc<dyn SearchService>> for GrpcAdapter {
    fn from(search_service_arc: Arc<dyn SearchService>) -> Self {
        GrpcAdapter(search_service_arc)
    }
}

#[async_trait]
impl grpc::SearchService for GrpcAdapter {
    async fn root_search(
        &self,
        request: tonic::Request<SearchRequest>,
    ) -> Result<tonic::Response<SearchResult>, tonic::Status> {
        let search_request = request.into_inner();
        let search_result = self
            .0
            .root_search(search_request)
            .await
            .map_err(convert_error_to_tonic_status)?;
        Ok(tonic::Response::new(search_result))
    }

    async fn leaf_search(
        &self,
        _request: tonic::Request<LeafSearchRequest>,
    ) -> Result<tonic::Response<LeafSearchResult>, tonic::Status> {
        todo!()
    }

    async fn fetch_docs(
        &self,
        _request: tonic::Request<FetchDocsRequest>,
    ) -> Result<tonic::Response<FetchDocsResult>, tonic::Status> {
        todo!()
    }
}

fn convert_error_to_tonic_status(search_error: SearchError) -> tonic::Status {
    match search_error {
        SearchError::IndexDoesNotExist { index_id } => tonic::Status::new(
            tonic::Code::NotFound,
            format!("Index not found {}", index_id),
        ),
        SearchError::InternalError(error) => tonic::Status::new(
            tonic::Code::Internal,
            format!("Internal error: {:?}", error),
        ),
        SearchError::StorageResolverError(storage_resolver_error) => tonic::Status::new(
            tonic::Code::Internal,
            format!("Failed to resolve storage uri {:?}", storage_resolver_error),
        ),
        SearchError::InvalidQuery(query_error) => tonic::Status::new(
            tonic::Code::InvalidArgument,
            format!("Invalid query: {:?}", query_error),
        ),
    }
}
