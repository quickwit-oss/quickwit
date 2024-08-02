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

use std::collections::HashSet;

use quickwit_config::service::QuickwitService;
use quickwit_metastore::SplitState;
use quickwit_opentelemetry::otlp::{OTEL_LOGS_INDEX_ID, OTEL_TRACES_INDEX_ID};
use quickwit_proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
use quickwit_proto::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;
use quickwit_proto::opentelemetry::proto::common::v1::any_value::Value;
use quickwit_proto::opentelemetry::proto::common::v1::AnyValue;
use quickwit_proto::opentelemetry::proto::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use quickwit_proto::opentelemetry::proto::trace::v1::{ResourceSpans, ScopeSpans, Span};
use tonic::codec::CompressionEncoding;

use crate::test_utils::ClusterSandbox;

fn initialize_tests() {
    quickwit_common::setup_logging_for_tests();
    std::env::set_var("QW_ENABLE_INGEST_V2", "true");
}

#[tokio::test]
async fn test_ingest_traces_with_otlp_grpc_api() {
    initialize_tests();
    let nodes_services = vec![
        HashSet::from_iter([QuickwitService::Searcher]),
        HashSet::from_iter([QuickwitService::Metastore]),
        HashSet::from_iter([QuickwitService::Indexer]),
        HashSet::from_iter([QuickwitService::ControlPlane]),
        HashSet::from_iter([QuickwitService::Janitor]),
    ];
    let mut sandbox = ClusterSandbox::start_cluster_with_otlp_service(&nodes_services)
        .await
        .unwrap();
    // Wait for the pipelines to start (one for logs and one for traces)
    sandbox.wait_for_indexing_pipelines(2).await.unwrap();

    fn build_span(span_name: String) -> Vec<ResourceSpans> {
        let scope_spans = vec![ScopeSpans {
            spans: vec![Span {
                name: span_name,
                trace_id: vec![1; 16],
                span_id: vec![2; 8],
                start_time_unix_nano: 1_000_000_001,
                end_time_unix_nano: 1_000_000_002,
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
        sandbox.trace_client.clone(),
        sandbox
            .trace_client
            .clone()
            .send_compressed(CompressionEncoding::Gzip),
    ];
    for (idx, mut tested_client) in tested_clients.into_iter().enumerate() {
        let body = format!("hello{}", idx);
        let request = ExportTraceServiceRequest {
            resource_spans: build_span(body.clone()),
        };
        let response = tested_client.export(request.clone()).await.unwrap();
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
            .trace_client
            .clone()
            .export(tonic_request)
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    sandbox
        .shutdown_services(&HashSet::from_iter([QuickwitService::Indexer]))
        .await
        .unwrap();
    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_ingest_logs_with_otlp_grpc_api() {
    initialize_tests();
    let nodes_services = vec![
        HashSet::from_iter([QuickwitService::Searcher]),
        HashSet::from_iter([QuickwitService::Metastore]),
        HashSet::from_iter([QuickwitService::Indexer]),
        HashSet::from_iter([QuickwitService::ControlPlane]),
        HashSet::from_iter([QuickwitService::Janitor]),
    ];
    let mut sandbox = ClusterSandbox::start_cluster_with_otlp_service(&nodes_services)
        .await
        .unwrap();
    // Wait fo the pipelines to start (one for logs and one for traces)
    sandbox.wait_for_indexing_pipelines(2).await.unwrap();

    fn build_log(body: String) -> Vec<ResourceLogs> {
        let log_record = LogRecord {
            time_unix_nano: 1_000_000_001,
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
        sandbox.logs_client.clone(),
        sandbox
            .logs_client
            .clone()
            .send_compressed(CompressionEncoding::Gzip),
    ];
    for (idx, mut tested_client) in tested_clients.into_iter().enumerate() {
        let body: String = format!("hello{}", idx);
        let request = ExportLogsServiceRequest {
            resource_logs: build_log(body.clone()),
        };
        let response = tested_client.export(request.clone()).await.unwrap();
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
        .shutdown_services(&HashSet::from_iter([QuickwitService::Indexer]))
        .await
        .unwrap();
    sandbox.shutdown().await.unwrap();
}
