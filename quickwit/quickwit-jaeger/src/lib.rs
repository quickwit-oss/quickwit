// Copyright (C) 2022 Quickwit, Inc.
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
use quickwit_proto::jaeger::storage::v1::span_reader_plugin_server::SpanReaderPlugin;
use quickwit_proto::jaeger::storage::v1::{
    FindTraceIDsRequest, FindTraceIDsResponse, FindTracesRequest, GetOperationsRequest,
    GetOperationsResponse, GetServicesRequest, GetServicesResponse, GetTraceRequest,
    SpansResponseChunk, Operation,
};
use quickwit_proto::SearchRequest;
use quickwit_search::SearchService;
use serde_json::Value as JsonValue;
use time::OffsetDateTime;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

const TRACE_INDEX_ID: &str = "otel-trace";

pub struct JaegerService {
    search_service: Arc<dyn SearchService>,
}

impl JaegerService {
    pub fn new(search_service: Arc<dyn SearchService>) -> Self {
        Self { search_service }
    }
}

type SpanStream = ReceiverStream<Result<SpansResponseChunk, Status>>;

#[async_trait]
impl SpanReaderPlugin for JaegerService {
    type GetTraceStream = SpanStream;

    type FindTracesStream = SpanStream;

    async fn get_services(
        &self,
        _request: Request<GetServicesRequest>,
    ) -> Result<Response<GetServicesResponse>, Status> {
        let search_request = SearchRequest {
            index_id: TRACE_INDEX_ID.to_string(),
            query: "status.code:0".to_string(),
            search_fields: Vec::new(),
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 1_000,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            aggregation_request: None,
            snippet_fields: Vec::new(),
        };
        let search_response = self
            .search_service
            .root_search(search_request)
            .await
            .unwrap();
        let mut services: Vec<String> = search_response
            .hits
            .into_iter()
            .filter_map(|hit| {
                match serde_json::from_str::<JsonValue>(&hit.json)
                    .ok()
                    .map(|mut value| value["service_name"].take()) {
                        Some(JsonValue::String(service_name)) => Some(service_name),
                        _ => None,
                    }
            })
            .collect();
        services.sort();
        services.dedup();
        let response = GetServicesResponse { services };
        Ok(Response::new(response))
    }

    async fn get_operations(
        &self,
        request: Request<GetOperationsRequest>,
    ) -> Result<Response<GetOperationsResponse>, Status> {
        let request = request.into_inner();
        print!("GET OPERATIONS: {:?}", request);
        let query = if request.span_kind.is_empty() {
            format!("service_name:{}", request.service) }
        else {
            format!("service_name:{} span_kind.name:{}", request.service, request.span_kind)
        };
        let search_request = SearchRequest {
            index_id: TRACE_INDEX_ID.to_string(),
            query,
            search_fields: Vec::new(),
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 1_000,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            aggregation_request: None,
            snippet_fields: Vec::new(),
        };
        let search_response = self
            .search_service
            .root_search(search_request)
            .await
            .unwrap();
        let mut operation_names: Vec<String> = search_response
            .hits
            .into_iter()
            .filter_map(|hit| {
                match serde_json::from_str::<JsonValue>(&hit.json)
                    .ok()
                    .map(|mut value| value["span_name"].take()) {
                        Some(JsonValue::String(service_name)) => Some(service_name),
                        _ => None,
                    }
            })
            .collect();
        operation_names.sort();
        operation_names.dedup();
        let operations = vec![Operation {
            name: "all".to_string(),
            span_kind: "span_kind".to_string(),
        }];
        let response = GetOperationsResponse { operation_names, operations };
        Ok(Response::new(response))
    }

    async fn get_trace(
        &self,
        _request: Request<GetTraceRequest>,
    ) -> Result<Response<Self::GetTraceStream>, Status> {
        unimplemented!()
    }

    async fn find_trace_i_ds(
        &self,
        _request: Request<FindTraceIDsRequest>,
    ) -> Result<Response<FindTraceIDsResponse>, Status> {
        unimplemented!()
    }

    async fn find_traces(
        &self,
        _request: Request<FindTracesRequest>,
    ) -> Result<Response<Self::FindTracesStream>, Status> {
        unimplemented!()
    }
}
