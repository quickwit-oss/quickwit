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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use quickwit_proto::jaeger::storage::v1::span_reader_plugin_server::SpanReaderPlugin;
use quickwit_proto::jaeger::storage::v1::{
    FindTraceIDsRequest, FindTraceIDsResponse, FindTracesRequest, GetOperationsRequest,
    GetOperationsResponse, GetServicesRequest, GetServicesResponse, GetTraceRequest, Operation,
    SpansResponseChunk,
};
use quickwit_proto::{SearchRequest, SearchResponse};
use quickwit_search::SearchService;
use serde_json::Value as JsonValue;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::debug;

// OpenTelemetry to Jaeger Transformation
// <https://opentelemetry.io/docs/reference/specification/trace/sdk_exporters/jaeger/>

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
        request: Request<GetServicesRequest>,
    ) -> Result<Response<GetServicesResponse>, Status> {
        let request = request.into_inner();
        debug!(request=?request, "`get_services` request");

        let search_request = request.into_search_req();
        let search_response = self
            .search_service
            .root_search(search_request)
            .await
            .unwrap();
        let response = search_response.into_jaeger_resp();
        debug!(response=?response, "`get_services` response");
        Ok(Response::new(response))
    }

    async fn get_operations(
        &self,
        request: Request<GetOperationsRequest>,
    ) -> Result<Response<GetOperationsResponse>, Status> {
        let request = request.into_inner();
        debug!(request=?request, "`get_operations` request");

        let search_request = request.into_search_req();
        let search_response = self
            .search_service
            .root_search(search_request)
            .await
            .unwrap();
        let response = search_response.into_jaeger_resp();
        debug!(response=?response, "`get_operations` response");
        Ok(Response::new(response))
    }

    async fn find_trace_i_ds(
        &self,
        request: Request<FindTraceIDsRequest>,
    ) -> Result<Response<FindTraceIDsResponse>, Status> {
        let request = request.into_inner();
        debug!(request=?request, "`find_trace_ids` request");

        let search_request = request.into_search_req();
        let search_response = self
            .search_service
            .root_search(search_request)
            .await
            .unwrap();
        let response = search_response.into_jaeger_resp();
        debug!(response=?response, "`find_trace_ids` response");
        Ok(Response::new(response))
    }

    async fn find_traces(
        &self,
        request: Request<FindTracesRequest>,
    ) -> Result<Response<Self::FindTracesStream>, Status> {
        let request = request.into_inner();
        debug!(request=?request, "`find_traces` request");

        // let search_request = request.into_search_req();
        // let search_response = self
        //     .search_service
        //     .root_search(search_request)
        //     .await
        //     .unwrap();
        unimplemented!()
        // let response = search_response.into_jaeger_resp();
        // debug!(response=?response, "`find_traces` response");
        // Ok(Response::new(response))
    }

    async fn get_trace(
        &self,
        request: Request<GetTraceRequest>,
    ) -> Result<Response<Self::GetTraceStream>, Status> {
        let request = request.into_inner();
        debug!(request=?request, "`get_trace` request");

        // let search_request = request.into();
        // let search_response = self
        //     .search_service
        //     .root_search(search_request)
        //     .await
        //     .unwrap();
        // let response = search_response.into();
        // Ok(Response::new(response))

        unimplemented!()
    }
}
trait IntoSearchRequest {
    fn into_search_req(self) -> SearchRequest;
}

trait FromSearchResponse {
    fn from_search_resp(search_response: SearchResponse) -> Self;
}

trait IntoJaegerResponse<T> {
    fn into_jaeger_resp(self) -> T;
}

impl<T> IntoJaegerResponse<T> for SearchResponse
where T: FromSearchResponse
{
    fn into_jaeger_resp(self) -> T {
        T::from_search_resp(self)
    }
}

// GetServices
impl IntoSearchRequest for GetServicesRequest {
    fn into_search_req(self) -> SearchRequest {
        SearchRequest {
            index_id: TRACE_INDEX_ID.to_string(),
            query: build_query("", "", "", HashMap::new()),
            search_fields: Vec::new(),
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 1_000,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            aggregation_request: None,
            snippet_fields: Vec::new(),
        }
    }
}

impl FromSearchResponse for GetServicesResponse {
    fn from_search_resp(search_response: SearchResponse) -> Self {
        let services: Vec<String> = search_response
            .hits
            .into_iter()
            .map(|hit| {
                serde_json::from_str::<JsonValue>(&hit.json)
                    .expect("Failed to deserialize hit. This should never happen!")
            })
            .filter_map(extract_service_name)
            .sorted()
            .dedup()
            .collect();
        Self { services }
    }
}

fn extract_service_name(mut doc: JsonValue) -> Option<String> {
    match doc["service_name"].take() {
        JsonValue::String(service_name) => Some(service_name),
        _ => None,
    }
}

// GetOperations
impl IntoSearchRequest for GetOperationsRequest {
    fn into_search_req(self) -> SearchRequest {
        SearchRequest {
            index_id: TRACE_INDEX_ID.to_string(),
            query: build_query(&self.service, &self.span_kind, "", HashMap::new()),
            search_fields: Vec::new(),
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 1_000,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            aggregation_request: None,
            snippet_fields: Vec::new(),
        }
    }
}

impl FromSearchResponse for GetOperationsResponse {
    fn from_search_resp(search_response: SearchResponse) -> Self {
        let operations: Vec<Operation> = search_response
            .hits
            .into_iter()
            .map(|hit| {
                serde_json::from_str::<JsonValue>(&hit.json)
                    .expect("Failed to deserialize hit. This should never happen!")
            })
            .filter_map(extract_operation)
            .sorted()
            .dedup()
            .collect();
        Self {
            operations,
            operation_names: Vec::new(), // `operation_names` is deprecated.
        }
    }
}

fn extract_operation(mut doc: JsonValue) -> Option<Operation> {
    match (doc["span_name"].take(), doc["span_kind"]["name"].take()) {
        (JsonValue::String(span_name), JsonValue::String(span_kind_name)) => Some(Operation {
            name: span_name,
            span_kind: span_kind_name,
        }),
        _ => None,
    }
}

// FindTraceIDs
impl IntoSearchRequest for FindTraceIDsRequest {
    fn into_search_req(self) -> SearchRequest {
        let query = self.query.unwrap();
        let start_timestamp = query.start_time_min.map(|ts| ts.seconds);
        let end_timestamp = query.start_time_max.map(|ts| ts.seconds);
        let max_hits = query.num_traces as u64;
        SearchRequest {
            index_id: TRACE_INDEX_ID.to_string(),
            query: build_query(&query.service_name, "", &query.operation_name, query.tags),
            search_fields: Vec::new(),
            start_timestamp,
            end_timestamp,
            max_hits,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            aggregation_request: None,
            snippet_fields: Vec::new(),
        }
    }
}

impl FromSearchResponse for FindTraceIDsResponse {
    fn from_search_resp(search_response: SearchResponse) -> Self {
        let trace_ids: Vec<Vec<u8>> = search_response
            .hits
            .into_iter()
            .map(|hit| {
                serde_json::from_str::<JsonValue>(&hit.json)
                    .expect("Failed to deserialize hit. This should never happen.")
            })
            .filter_map(extract_trace_id)
            .sorted()
            .dedup()
            .map(|trace_id| {
                base64::decode(&trace_id)
                    .expect("Failed to decode trace ID. This should never happen!")
            })
            .collect();
        Self { trace_ids }
    }
}

fn extract_trace_id(mut doc: JsonValue) -> Option<String> {
    match doc["trace_id"].take() {
        JsonValue::String(trace_id) => Some(trace_id),
        _ => None,
    }
}

// FindTraces
impl IntoSearchRequest for FindTracesRequest {
    fn into_search_req(self) -> SearchRequest {
        let query = self.query.unwrap();
        let start_timestamp = query.start_time_min.map(|ts| ts.seconds);
        let end_timestamp = query.start_time_max.map(|ts| ts.seconds);
        let max_hits = query.num_traces as u64;
        SearchRequest {
            index_id: TRACE_INDEX_ID.to_string(),
            query: build_query(&query.service_name, "", &query.operation_name, query.tags),
            search_fields: Vec::new(),
            start_timestamp,
            end_timestamp,
            max_hits,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            aggregation_request: None,
            snippet_fields: Vec::new(),
        }
    }
}

// impl FromSearchResponse for FindTracesResponse {
//     fn from_search_resp(search_response: SearchResponse) -> Self {
//         let traces: Vec<Trace> = search_response
//             .hits
//             .into_iter()
//             .map(|hit| {
//                 serde_json::from_str::<JsonValue>(&hit.json)
//                     .expect("Failed to deserialize hit. This should never happen.")
//             })
//             .filter_map(extract_trace)
//             .collect();
//         Self { traces }
//     }
// }

fn build_query(
    service_name: &str,
    span_kind: &str,
    span_name: &str,
    mut tags: HashMap<String, String>,
) -> String {
    if let Some(qw_query) = tags.remove("_qw_query") {
        return qw_query;
    }
    let mut query = String::new();

    if !service_name.is_empty() {
        query.push_str("service_name:");
        query.push_str(service_name);
    }
    if !span_kind.is_empty() {
        if !query.is_empty() {
            query.push_str(" AND ");
        }
        query.push_str("span_kind.name:");
        query.push_str(span_kind);
    }
    if !span_name.is_empty() {
        if !query.is_empty() {
            query.push_str(" AND ");
        }
        query.push_str("span_name:");
        query.push_str(span_name);
    }
    if !tags.is_empty() {
        if !query.is_empty() {
            query.push_str(" AND ");
        }
        let mut tags_iter = tags.into_iter();

        if let Some((key, value)) = tags_iter.next() {
            query.push_str("attributes.");
            query.push_str(&key);
            query.push_str(":");
            query.push_str(&value);
        }
        while let Some((key, value)) = tags_iter.next() {
            query.push_str(" AND ");
            query.push_str("attributes.");
            query.push_str(&key);
            query.push_str(":");
            query.push_str(&value);
        }
    }
    // FIXME
    if query.is_empty() {
        query.push_str("status.code:0");
    }
    debug!(query=%query, "Search query");
    query
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_query() {
        {
            let service_name = "quickwit";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::new();
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                "service_name:quickwit"
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::from_iter([("_qw_query".to_string(), "query".to_string())]);
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                "query"
            );
        }
        {
            let service_name = "";
            let span_kind = "client";
            let span_name = "";
            let tags = HashMap::new();
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                "span_kind.name:client"
            );
        }
        {
            let service_name = "";
            let span_kind = "";
            let span_name = "leaf_search";
            let tags = HashMap::new();
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                "span_name:leaf_search"
            );
        }
        {
            let service_name = "";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::from_iter([("foo".to_string(), "bar".to_string())]);
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                "attributes.foo:bar"
            );
        }
        {
            let service_name = "";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::from_iter([
                ("foo".to_string(), "bar".to_string()),
                ("baz".to_string(), "qux".to_string()),
            ]);
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                "attributes.foo:bar AND attributes.baz:qux"
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::from_iter([
                ("foo".to_string(), "bar".to_string()),
                ("baz".to_string(), "qux".to_string()),
            ]);
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                "service_name:quickwit AND attributes.foo:bar AND attributes.baz:qux"
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = "client";
            let span_name = "";
            let tags = HashMap::from_iter([
                ("foo".to_string(), "bar".to_string()),
                ("baz".to_string(), "qux".to_string()),
            ]);
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                "service_name:quickwit AND span_kind.name:client AND attributes.foo:bar AND \
                 attributes.baz:qux"
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = "client";
            let span_name = "leaf_search";
            let tags = HashMap::from_iter([
                ("foo".to_string(), "bar".to_string()),
                ("baz".to_string(), "qux".to_string()),
            ]);
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                "service_name:quickwit AND span_kind.name:client AND span_name:leat_search AND \
                 attributes.foo:bar AND attributes.baz:qux"
            );
        }
    }
}
