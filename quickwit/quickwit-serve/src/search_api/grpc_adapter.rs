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

use std::sync::Arc;

use async_trait::async_trait;
use quickwit_proto::error::convert_to_grpc_result;
use quickwit_proto::search::{
    GetKvRequest, GetKvResponse, LeafListFieldsRequest, ListFieldsRequest, ListFieldsResponse,
    ReportSplitsRequest, ReportSplitsResponse, search_service_server as grpc,
};
use quickwit_proto::{set_parent_span_from_request_metadata, tonic};
use quickwit_search::SearchService;
use tracing::instrument;

#[derive(Clone)]
pub struct GrpcSearchAdapter(Arc<dyn SearchService>);

impl From<Arc<dyn SearchService>> for GrpcSearchAdapter {
    fn from(search_service_arc: Arc<dyn SearchService>) -> Self {
        GrpcSearchAdapter(search_service_arc)
    }
}

#[async_trait]
impl grpc::SearchService for GrpcSearchAdapter {
    #[instrument(skip(self, request))]
    async fn root_search(
        &self,
        request: tonic::Request<quickwit_proto::search::SearchRequest>,
    ) -> Result<tonic::Response<quickwit_proto::search::SearchResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let search_request = request.into_inner();
        let search_result = self.0.root_search(search_request).await;
        convert_to_grpc_result(search_result)
    }

    #[instrument(skip(self, request))]
    async fn leaf_search(
        &self,
        request: tonic::Request<quickwit_proto::search::LeafSearchRequest>,
    ) -> Result<tonic::Response<quickwit_proto::search::LeafSearchResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let leaf_search_request = request.into_inner();
        let leaf_search_result = self.0.leaf_search(leaf_search_request).await;
        convert_to_grpc_result(leaf_search_result)
    }

    #[instrument(skip(self, request))]
    async fn fetch_docs(
        &self,
        request: tonic::Request<quickwit_proto::search::FetchDocsRequest>,
    ) -> Result<tonic::Response<quickwit_proto::search::FetchDocsResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let fetch_docs_request = request.into_inner();
        let fetch_docs_result = self.0.fetch_docs(fetch_docs_request).await;
        convert_to_grpc_result(fetch_docs_result)
    }

    #[instrument(skip(self, request))]
    async fn root_list_terms(
        &self,
        request: tonic::Request<quickwit_proto::search::ListTermsRequest>,
    ) -> Result<tonic::Response<quickwit_proto::search::ListTermsResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let search_request = request.into_inner();
        let search_result = self.0.root_list_terms(search_request).await;
        convert_to_grpc_result(search_result)
    }

    #[instrument(skip(self, request))]
    async fn leaf_list_terms(
        &self,
        request: tonic::Request<quickwit_proto::search::LeafListTermsRequest>,
    ) -> Result<tonic::Response<quickwit_proto::search::LeafListTermsResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let leaf_search_request = request.into_inner();
        let leaf_search_result = self.0.leaf_list_terms(leaf_search_request).await;
        convert_to_grpc_result(leaf_search_result)
    }

    async fn scroll(
        &self,
        request: tonic::Request<quickwit_proto::search::ScrollRequest>,
    ) -> Result<tonic::Response<quickwit_proto::search::SearchResponse>, tonic::Status> {
        let scroll_request = request.into_inner();
        let scroll_result = self.0.scroll(scroll_request).await;
        convert_to_grpc_result(scroll_result)
    }

    #[instrument(skip(self, request))]
    async fn put_kv(
        &self,
        request: tonic::Request<quickwit_proto::search::PutKvRequest>,
    ) -> Result<tonic::Response<quickwit_proto::search::PutKvResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let put_request = request.into_inner();
        self.0.put_kv(put_request).await;
        Ok(tonic::Response::new(
            quickwit_proto::search::PutKvResponse {},
        ))
    }

    #[instrument(skip(self, request))]
    async fn get_kv(
        &self,
        request: tonic::Request<GetKvRequest>,
    ) -> Result<tonic::Response<GetKvResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let get_search_after_context_request = request.into_inner();
        let payload = self.0.get_kv(get_search_after_context_request).await;
        let get_response = GetKvResponse { payload };
        Ok(tonic::Response::new(get_response))
    }

    #[instrument(skip(self, request))]
    async fn report_splits(
        &self,
        request: tonic::Request<ReportSplitsRequest>,
    ) -> Result<tonic::Response<ReportSplitsResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let get_search_after_context_request = request.into_inner();
        self.0.report_splits(get_search_after_context_request).await;
        Ok(tonic::Response::new(ReportSplitsResponse {}))
    }

    #[instrument(skip(self, request))]
    async fn list_fields(
        &self,
        request: tonic::Request<ListFieldsRequest>,
    ) -> Result<tonic::Response<ListFieldsResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let resp = self.0.root_list_fields(request.into_inner()).await;
        convert_to_grpc_result(resp)
    }
    #[instrument(skip(self, request))]
    async fn leaf_list_fields(
        &self,
        request: tonic::Request<LeafListFieldsRequest>,
    ) -> Result<tonic::Response<ListFieldsResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let resp = self.0.leaf_list_fields(request.into_inner()).await;
        convert_to_grpc_result(resp)
    }

    #[instrument(skip(self, request))]
    async fn search_plan(
        &self,
        request: tonic::Request<quickwit_proto::search::SearchRequest>,
    ) -> Result<tonic::Response<quickwit_proto::search::SearchPlanResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let search_request = request.into_inner();
        let search_result = self.0.search_plan(search_request).await;
        convert_to_grpc_result(search_result)
    }
}
