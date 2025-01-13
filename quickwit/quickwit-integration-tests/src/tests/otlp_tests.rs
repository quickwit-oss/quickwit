// Copyright (C) 2024 Quickwit, Inc.
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

use futures_util::StreamExt;
use quickwit_config::service::QuickwitService;
use quickwit_metastore::SplitState;
use quickwit_opentelemetry::otlp::{
    make_resource_spans_for_test, OTEL_LOGS_INDEX_ID, OTEL_TRACES_INDEX_ID,
};
use quickwit_proto::jaeger::storage::v1::{
    FindTraceIDsRequest, GetOperationsRequest, GetServicesRequest, GetTraceRequest, Operation,
    SpansResponseChunk, TraceQueryParameters,
};
use quickwit_proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
use quickwit_proto::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;
use quickwit_proto::opentelemetry::proto::common::v1::any_value::Value;
use quickwit_proto::opentelemetry::proto::common::v1::AnyValue;
use quickwit_proto::opentelemetry::proto::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use quickwit_proto::opentelemetry::proto::trace::v1::{ResourceSpans, ScopeSpans, Span};
use tonic::codec::CompressionEncoding;

use crate::test_utils::ClusterSandboxBuilder;

fn initialize_tests() {
    quickwit_common::setup_logging_for_tests();
    std::env::set_var("QW_ENABLE_INGEST_V2", "true");
}

#[tokio::test]
async fn test_ingest_traces_with_otlp_grpc_api() {
    initialize_tests();
    let mut sandbox = ClusterSandboxBuilder::default()
        .add_node([QuickwitService::Searcher])
        .add_node([QuickwitService::Metastore])
        .add_node_with_otlp([QuickwitService::Indexer])
        .add_node([QuickwitService::ControlPlane])
        .add_node([QuickwitService::Janitor])
        .build_and_start()
        .await;
    // Wait for the pipelines to start (one for logs and one for traces)
    sandbox.wait_for_indexing_pipelines(2).await.unwrap();

    fn build_span(span_name: String) -> Vec<ResourceSpans> {
        let scope_spans = vec![ScopeSpans {
            spans: vec![Span {
                name: span_name,
                trace_id: vec![1; 16],
                span_id: vec![2; 8],
                start_time_unix_nano: 1724060143000000001,
                end_time_unix_nano: 1724060144000000000,
                ..Default::default()
            }],
            ..Default::default()
        }];
        vec![ResourceSpans {
            scope_spans,
            ..Default::default()
        }]
    }

    // Send the spans on the default index
    let tested_clients = vec![
        sandbox.trace_client().clone(),
        sandbox
            .trace_client()
            .clone()
            .send_compressed(CompressionEncoding::Gzip),
    ];
    for (idx, mut tested_client) in tested_clients.into_iter().enumerate() {
        let body = format!("hello{}", idx);
        let request = ExportTraceServiceRequest {
            resource_spans: build_span(body.clone()),
        };
        let response = tested_client.export(request).await.unwrap();
        assert_eq!(
            response
                .into_inner()
                .partial_success
                .unwrap()
                .rejected_spans,
            0
        );
        sandbox
            .wait_for_splits(
                OTEL_TRACES_INDEX_ID,
                Some(vec![SplitState::Published]),
                idx + 1,
            )
            .await
            .unwrap();
        sandbox
            .assert_hit_count(OTEL_TRACES_INDEX_ID, &format!("span_name:{}", body), 1)
            .await;
    }

    // Send the spans on a non existing index, should return an error.
    {
        let request = ExportTraceServiceRequest {
            resource_spans: build_span("hello".to_string()),
        };
        let mut tonic_request = tonic::Request::new(request);
        tonic_request.metadata_mut().insert(
            "qw-otel-traces-index",
            tonic::metadata::MetadataValue::try_from("non-existing-index").unwrap(),
        );
        let status = sandbox
            .trace_client()
            .clone()
            .export(tonic_request)
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    sandbox
        .shutdown_services([QuickwitService::Indexer])
        .await
        .unwrap();
    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_ingest_logs_with_otlp_grpc_api() {
    initialize_tests();
    let mut sandbox = ClusterSandboxBuilder::default()
        .add_node([QuickwitService::Searcher])
        .add_node([QuickwitService::Metastore])
        .add_node_with_otlp([QuickwitService::Indexer])
        .add_node([QuickwitService::ControlPlane])
        .add_node([QuickwitService::Janitor])
        .build_and_start()
        .await;
    // Wait fo the pipelines to start (one for logs and one for traces)
    sandbox.wait_for_indexing_pipelines(2).await.unwrap();

    fn build_log(body: String) -> Vec<ResourceLogs> {
        let log_record = LogRecord {
            time_unix_nano: 1724060143000000001,
            body: Some(AnyValue {
                value: Some(Value::StringValue(body)),
            }),
            ..Default::default()
        };
        let scope_logs = ScopeLogs {
            log_records: vec![log_record],
            ..Default::default()
        };
        vec![ResourceLogs {
            scope_logs: vec![scope_logs],
            ..Default::default()
        }]
    }

    // Send the logs on the default index
    let tested_clients = vec![
        sandbox.logs_client().clone(),
        sandbox
            .logs_client()
            .clone()
            .send_compressed(CompressionEncoding::Gzip),
    ];
    for (idx, mut tested_client) in tested_clients.into_iter().enumerate() {
        let body: String = format!("hello{}", idx);
        let request = ExportLogsServiceRequest {
            resource_logs: build_log(body.clone()),
        };
        let response = tested_client.export(request).await.unwrap();
        assert_eq!(
            response
                .into_inner()
                .partial_success
                .unwrap()
                .rejected_log_records,
            0
        );
        sandbox
            .wait_for_splits(
                OTEL_LOGS_INDEX_ID,
                Some(vec![SplitState::Published]),
                idx + 1,
            )
            .await
            .unwrap();
        sandbox
            .assert_hit_count(OTEL_LOGS_INDEX_ID, &format!("body.message:{}", body), 1)
            .await;
    }

    sandbox
        .shutdown_services([QuickwitService::Indexer])
        .await
        .unwrap();
    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_jaeger_api() {
    initialize_tests();
    let mut sandbox = ClusterSandboxBuilder::default()
        .add_node([QuickwitService::Searcher])
        .add_node([QuickwitService::Metastore])
        .add_node_with_otlp([QuickwitService::Indexer])
        .add_node([QuickwitService::ControlPlane])
        .add_node([QuickwitService::Janitor])
        .build_and_start()
        .await;
    // Wait fo the pipelines to start (one for logs and one for traces)
    sandbox.wait_for_indexing_pipelines(2).await.unwrap();

    let export_trace_request = ExportTraceServiceRequest {
        resource_spans: make_resource_spans_for_test(),
    };
    sandbox
        .trace_client()
        .export(export_trace_request)
        .await
        .unwrap();

    sandbox
        .wait_for_splits(OTEL_TRACES_INDEX_ID, Some(vec![SplitState::Published]), 1)
        .await
        .unwrap();

    sandbox
        .shutdown_services([QuickwitService::Indexer])
        .await
        .unwrap();

    {
        // Test `GetServices`
        let get_services_request = GetServicesRequest {};
        let get_services_response = sandbox
            .jaeger_client()
            .get_services(tonic::Request::new(get_services_request))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(get_services_response.services, &["quickwit"]);
    }
    {
        // Test `GetOperations`
        let get_operations_request = GetOperationsRequest {
            service: "quickwit".to_string(),
            span_kind: "".to_string(),
        };
        let get_operations_response = sandbox
            .jaeger_client()
            .get_operations(tonic::Request::new(get_operations_request))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(get_operations_response.operations.len(), 4);
        assert_eq!(
            get_operations_response.operations,
            vec![
                Operation {
                    name: "delete_splits".to_string(),
                    span_kind: "client".to_string(),
                },
                Operation {
                    name: "list_splits".to_string(),
                    span_kind: "client".to_string(),
                },
                Operation {
                    name: "publish_splits".to_string(),
                    span_kind: "server".to_string(),
                },
                Operation {
                    name: "stage_splits".to_string(),
                    span_kind: "internal".to_string(),
                }
            ]
        );

        let get_operations_request = GetOperationsRequest {
            service: "quickwit".to_string(),
            span_kind: "server".to_string(),
        };
        let get_operations_response = sandbox
            .jaeger_client()
            .get_operations(tonic::Request::new(get_operations_request))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(get_operations_response.operations.len(), 1);
        assert_eq!(
            get_operations_response.operations,
            vec![Operation {
                name: "publish_splits".to_string(),
                span_kind: "server".to_string(),
            },]
        );
    }
    {
        // Test `FindTraceIds`
        // TODO: Increase comprehensiveness of this test.
        // Search by service and operation name.
        let query = TraceQueryParameters {
            service_name: "quickwit".to_string(),
            operation_name: "stage_splits".to_string(),
            tags: HashMap::new(),
            start_time_min: None,
            start_time_max: None,
            duration_min: None,
            duration_max: None,
            num_traces: 10,
        };
        let find_trace_ids_request = FindTraceIDsRequest { query: Some(query) };
        let find_trace_ids_response = sandbox
            .jaeger_client()
            .find_trace_i_ds(tonic::Request::new(find_trace_ids_request))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(find_trace_ids_response.trace_ids.len(), 1);
        assert_eq!(find_trace_ids_response.trace_ids[0], [1; 16]);

        // Search by service name, operation name, and span attribute.
        let query = TraceQueryParameters {
            service_name: "quickwit".to_string(),
            operation_name: "list_splits".to_string(),
            tags: HashMap::from([("span_key".to_string(), "span_value".to_string())]),
            start_time_min: None,
            start_time_max: None,
            duration_min: None,
            duration_max: None,
            num_traces: 10,
        };
        let find_trace_ids_request = FindTraceIDsRequest { query: Some(query) };
        let find_trace_ids_response = sandbox
            .jaeger_client()
            .find_trace_i_ds(tonic::Request::new(find_trace_ids_request))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(find_trace_ids_response.trace_ids.len(), 1);
        assert_eq!(find_trace_ids_response.trace_ids[0], [3; 16]);

        // Search by service name, operation name, and event attribute.
        let query = TraceQueryParameters {
            service_name: "quickwit".to_string(),
            operation_name: "delete_splits".to_string(),
            tags: HashMap::from([("event_key".to_string(), "event_value".to_string())]),
            start_time_min: None,
            start_time_max: None,
            duration_min: None,
            duration_max: None,
            num_traces: 10,
        };
        let find_trace_ids_request = FindTraceIDsRequest { query: Some(query) };
        let find_trace_ids_response = sandbox
            .jaeger_client()
            .find_trace_i_ds(tonic::Request::new(find_trace_ids_request))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(find_trace_ids_response.trace_ids.len(), 1);
        assert_eq!(find_trace_ids_response.trace_ids[0], [5; 16]);

        // Search traces with an error.
        let query = TraceQueryParameters {
            service_name: "quickwit".to_string(),
            operation_name: "list_splits".to_string(),
            tags: HashMap::from([("error".to_string(), "true".to_string())]),
            start_time_min: None,
            start_time_max: None,
            duration_min: None,
            duration_max: None,
            num_traces: 10,
        };
        let find_trace_ids_request = FindTraceIDsRequest { query: Some(query) };
        let find_trace_ids_response = sandbox
            .jaeger_client()
            .find_trace_i_ds(tonic::Request::new(find_trace_ids_request))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(find_trace_ids_response.trace_ids.len(), 1);
        assert_eq!(find_trace_ids_response.trace_ids[0], [4; 16]);

        // Search traces without an error.
        let query = TraceQueryParameters {
            service_name: "quickwit".to_string(),
            operation_name: "list_splits".to_string(),
            tags: HashMap::from([("error".to_string(), "false".to_string())]),
            start_time_min: None,
            start_time_max: None,
            duration_min: None,
            duration_max: None,
            num_traces: 10,
        };
        let find_trace_ids_request = FindTraceIDsRequest { query: Some(query) };
        let find_trace_ids_response = sandbox
            .jaeger_client()
            .find_trace_i_ds(tonic::Request::new(find_trace_ids_request))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(find_trace_ids_response.trace_ids.len(), 1);
        assert_eq!(find_trace_ids_response.trace_ids[0], [3; 16]);
    }
    {
        // Test `GetTrace`
        let get_trace_request = GetTraceRequest {
            trace_id: [1; 16].to_vec(),
        };
        let mut span_stream = sandbox
            .jaeger_client()
            .get_trace(tonic::Request::new(get_trace_request))
            .await
            .unwrap()
            .into_inner();
        let SpansResponseChunk { spans } = span_stream.next().await.unwrap().unwrap();
        assert_eq!(spans.len(), 1);

        let span: &quickwit_proto::jaeger::api_v2::Span = &spans[0];
        assert_eq!(span.operation_name, "stage_splits");

        let process = span.process.as_ref().unwrap();
        assert_eq!(process.tags.len(), 1);
        assert_eq!(process.tags[0].key, "tags");
        assert_eq!(process.tags[0].v_str, r#"["foo"]"#);
    }
    sandbox.shutdown().await.unwrap();
}
