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
use quickwit_search::{SearchError, SearchService, SearchServiceImpl};

/// gRPC adapter that wraped SearchService.
#[derive(Clone)]
pub struct GrpcAdapter(Arc<dyn SearchService>);

impl From<Arc<SearchServiceImpl>> for GrpcAdapter {
    fn from(search_service_arc: Arc<SearchServiceImpl>) -> Self {
        GrpcAdapter(search_service_arc)
    }
}

#[async_trait]
impl grpc::SearchService for GrpcAdapter {
    async fn root_search(
        &self,
        request: tonic::Request<quickwit_proto::SearchRequest>,
    ) -> Result<tonic::Response<quickwit_proto::SearchResult>, tonic::Status> {
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
        request: tonic::Request<quickwit_proto::LeafSearchRequest>,
    ) -> Result<tonic::Response<quickwit_proto::LeafSearchResult>, tonic::Status> {
        let leaf_search_request = request.into_inner();
        let leaf_search_result = self
            .0
            .leaf_search(leaf_search_request)
            .await
            .map_err(convert_error_to_tonic_status)?;
        Ok(tonic::Response::new(leaf_search_result))
    }

    async fn fetch_docs(
        &self,
        request: tonic::Request<quickwit_proto::FetchDocsRequest>,
    ) -> Result<tonic::Response<quickwit_proto::FetchDocsResult>, tonic::Status> {
        let fetch_docs_request = request.into_inner();
        let fetch_docs_result = self
            .0
            .fetch_docs(fetch_docs_request)
            .await
            .map_err(convert_error_to_tonic_status)?;
        Ok(tonic::Response::new(fetch_docs_result))
    }
}

fn status_code_for_search_error(search_error: &SearchError) -> tonic::Code {
    match search_error {
        SearchError::IndexDoesNotExist { .. } => tonic::Code::NotFound,
        SearchError::InternalError(_) => tonic::Code::Internal,
        SearchError::StorageResolverError(_) => tonic::Code::Internal,
        SearchError::InvalidQuery(_) => tonic::Code::InvalidArgument,
    }
}

/// Convert quickwit search error to tonic status.
fn convert_error_to_tonic_status(search_error: SearchError) -> tonic::Status {
    let error_json = serde_json::to_string_pretty(&search_error)
        .unwrap_or_else(|_| "Failed to serialize error".to_string());
    let code = status_code_for_search_error(&search_error);
    tonic::Status::new(code, error_json)
}
