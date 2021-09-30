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

use std::sync::Arc;

use async_trait::async_trait;
use futures::TryStreamExt;
use opentelemetry::global;
use opentelemetry::propagation::Extractor;
use quickwit_proto::{
    search_service_server as grpc, LeafSearchStreamRequest, LeafSearchStreamResult,
};
use quickwit_search::{SearchService, SearchServiceImpl};
use tracing::{instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

struct MetadataMap<'a>(&'a tonic::metadata::MetadataMap);

impl<'a> Extractor for MetadataMap<'a> {
    /// Gets a value for a key from the MetadataMap.  If the value can't be converted to &str,
    /// returns None
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|metadata| metadata.to_str().ok())
    }

    /// Collect all the keys from the MetadataMap.
    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|key| match key {
                tonic::metadata::KeyRef::Ascii(v) => v.as_str(),
                tonic::metadata::KeyRef::Binary(v) => v.as_str(),
            })
            .collect::<Vec<_>>()
    }
}

#[derive(Clone)]
pub struct GrpcSearchAdapter(Arc<dyn SearchService>);

impl GrpcSearchAdapter {
    #[cfg(test)]
    pub fn from_mock(mock_search_service_arc: Arc<dyn SearchService>) -> Self {
        GrpcSearchAdapter(mock_search_service_arc)
    }
}

impl From<Arc<SearchServiceImpl>> for GrpcSearchAdapter {
    fn from(search_service_arc: Arc<SearchServiceImpl>) -> Self {
        GrpcSearchAdapter(search_service_arc)
    }
}

#[async_trait]
impl grpc::SearchService for GrpcSearchAdapter {
    #[instrument(skip(self, request))]
    async fn root_search(
        &self,
        request: tonic::Request<quickwit_proto::SearchRequest>,
    ) -> Result<tonic::Response<quickwit_proto::SearchResponse>, tonic::Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        let search_request = request.into_inner();
        let search_response = self
            .0
            .root_search(search_request)
            .await
            .map_err(Into::<tonic::Status>::into)?;
        Ok(tonic::Response::new(search_response))
    }

    #[instrument(skip(self, request))]
    async fn leaf_search(
        &self,
        request: tonic::Request<quickwit_proto::LeafSearchRequest>,
    ) -> Result<tonic::Response<quickwit_proto::LeafSearchResponse>, tonic::Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        let leaf_search_request = request.into_inner();
        let leaf_search_response = self
            .0
            .leaf_search(leaf_search_request)
            .await
            .map_err(Into::<tonic::Status>::into)?;
        Ok(tonic::Response::new(leaf_search_response))
    }

    #[instrument(skip(self, request))]
    async fn fetch_docs(
        &self,
        request: tonic::Request<quickwit_proto::FetchDocsRequest>,
    ) -> Result<tonic::Response<quickwit_proto::FetchDocsResponse>, tonic::Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        let fetch_docs_request = request.into_inner();
        let fetch_docs_response = self
            .0
            .fetch_docs(fetch_docs_request)
            .await
            .map_err(Into::<tonic::Status>::into)?;
        Ok(tonic::Response::new(fetch_docs_response))
    }

    type LeafSearchStreamStream = std::pin::Pin<
        Box<
            dyn futures::Stream<Item = Result<LeafSearchStreamResult, tonic::Status>> + Send + Sync,
        >,
    >;
    #[instrument(name = "search_adapter:leaf_search_stream", skip(self, request))]
    async fn leaf_search_stream(
        &self,
        request: tonic::Request<LeafSearchStreamRequest>,
    ) -> Result<tonic::Response<Self::LeafSearchStreamStream>, tonic::Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        let leaf_search_request = request.into_inner();
        let leaf_search_result = self
            .0
            .leaf_search_stream(leaf_search_request)
            .await
            .map_err(Into::<tonic::Status>::into)?
            .map_err(Into::<tonic::Status>::into);
        Ok(tonic::Response::new(Box::pin(leaf_search_result)))
    }
}
