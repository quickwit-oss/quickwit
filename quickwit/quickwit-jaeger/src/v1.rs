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

//! Jaeger v1 API implementation (SpanReaderPlugin)

use std::time::Instant;

use async_trait::async_trait;
use quickwit_opentelemetry::otlp::{
    OTEL_TRACES_INDEX_ID, extract_otel_traces_index_id_patterns_from_metadata,
};
use quickwit_proto::jaeger::storage::v1::span_reader_plugin_server::SpanReaderPlugin;
use quickwit_proto::jaeger::storage::v1::{
    FindTraceIDsRequest, FindTraceIDsResponse, FindTracesRequest, GetOperationsRequest,
    GetOperationsResponse, GetServicesRequest, GetServicesResponse, GetTraceRequest,
};
use tonic::{Request, Response, Status};

use crate::metrics::JAEGER_SERVICE_METRICS;
use crate::{JaegerService, SpanStream};

macro_rules! metrics {
    ($expr:expr, [$operation:ident, $($label:expr),*]) => {
        let start = std::time::Instant::now();
        let labels = [stringify!($operation), $($label,)*];
        JAEGER_SERVICE_METRICS.requests_total.with_label_values(labels).inc();
        let (res, is_error) = match $expr {
            ok @ Ok(_) => {
                (ok, "false")
            },
            err @ Err(_) => {
                JAEGER_SERVICE_METRICS.request_errors_total.with_label_values(labels).inc();
                (err, "true")
            },
        };
        let elapsed = start.elapsed().as_secs_f64();
        let labels = [stringify!($operation), $($label,)* is_error];
        JAEGER_SERVICE_METRICS.request_duration_seconds.with_label_values(labels).observe(elapsed);

        return res.map(Response::new);
    };
}

#[async_trait]
impl SpanReaderPlugin for JaegerService {
    type GetTraceStream = SpanStream;

    type FindTracesStream = SpanStream;

    async fn get_services(
        &self,
        request: Request<GetServicesRequest>,
    ) -> Result<Response<GetServicesResponse>, Status> {
        let index_id_patterns =
            extract_otel_traces_index_id_patterns_from_metadata(request.metadata())?;
        metrics!(
            self.get_services_for_indexes(request.into_inner(), index_id_patterns)
                .await,
            [get_services, OTEL_TRACES_INDEX_ID]
        );
    }

    async fn get_operations(
        &self,
        request: Request<GetOperationsRequest>,
    ) -> Result<Response<GetOperationsResponse>, Status> {
        let index_id_patterns =
            extract_otel_traces_index_id_patterns_from_metadata(request.metadata())?;
        metrics!(
            self.get_operations_for_indexes(request.into_inner(), index_id_patterns)
                .await,
            [get_operations, OTEL_TRACES_INDEX_ID]
        );
    }

    async fn find_trace_i_ds(
        &self,
        request: Request<FindTraceIDsRequest>,
    ) -> Result<Response<FindTraceIDsResponse>, Status> {
        let index_id_patterns =
            extract_otel_traces_index_id_patterns_from_metadata(request.metadata())?;
        metrics!(
            self.find_trace_ids_for_indexes(request.into_inner(), index_id_patterns)
                .await,
            [find_trace_ids, OTEL_TRACES_INDEX_ID]
        );
    }

    async fn find_traces(
        &self,
        request: Request<FindTracesRequest>,
    ) -> Result<Response<Self::FindTracesStream>, Status> {
        let index_id_patterns =
            extract_otel_traces_index_id_patterns_from_metadata(request.metadata())?;
        self.find_traces_for_indexes(
            request.into_inner(),
            "find_traces",
            Instant::now(),
            index_id_patterns,
            false, /* if we use true, Jaeger will display "1 Span", and display an empty trace
                    * when clicking on the ui (but display the full trace after reloading the
                    * page) */
        )
        .await
        .map(Response::new)
    }

    async fn get_trace(
        &self,
        request: Request<GetTraceRequest>,
    ) -> Result<Response<Self::GetTraceStream>, Status> {
        let index_id_patterns =
            extract_otel_traces_index_id_patterns_from_metadata(request.metadata())?;
        self.get_trace_for_indexes(
            request.into_inner(),
            "get_trace",
            Instant::now(),
            index_id_patterns,
        )
        .await
        .map(Response::new)
    }
}
