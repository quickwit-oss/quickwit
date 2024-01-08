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
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use quickwit_actors::{ActorHandle, Mailbox, Universe};
use quickwit_cluster::{create_cluster_for_test, ChannelTransport, Cluster};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::uri::Uri;
use quickwit_config::{IndexerConfig, IngestApiConfig, JaegerConfig, SearcherConfig, SourceConfig};
use quickwit_indexing::models::SpawnPipeline;
use quickwit_indexing::IndexingService;
use quickwit_ingest::{
    init_ingest_api, CommitType, CreateQueueRequest, IngestApiService, IngestServiceClient,
    IngesterPool, QUEUES_DIR_NAME,
};
use quickwit_metastore::{AddSourceRequestExt, CreateIndexRequestExt, FileBackedMetastore};
use quickwit_opentelemetry::otlp::{make_resource_spans_for_test, OtlpGrpcTracesService};
use quickwit_proto::jaeger::storage::v1::span_reader_plugin_server::SpanReaderPlugin;
use quickwit_proto::jaeger::storage::v1::{
    FindTraceIDsRequest, GetOperationsRequest, GetServicesRequest, GetTraceRequest, Operation,
    SpansResponseChunk, TraceQueryParameters,
};
use quickwit_proto::metastore::{
    AddSourceRequest, CreateIndexRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::opentelemetry::proto::collector::trace::v1::trace_service_server::TraceService;
use quickwit_proto::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;
use quickwit_proto::types::{IndexUid, PipelineUid};
use quickwit_search::{
    start_searcher_service, SearchJobPlacer, SearchService, SearchServiceClient, SearcherContext,
    SearcherPool,
};
use quickwit_storage::StorageResolver;
use tempfile::TempDir;
use tokio_stream::StreamExt;

use crate::JaegerService;

#[tokio::test]
async fn test_otel_jaeger_integration() {
    let cluster = cluster_for_test().await;
    let universe = Universe::with_accelerated_time();
    let temp_dir = tempfile::tempdir().unwrap();

    let (ingester_service, ingester_client) = ingester_for_test(&universe, temp_dir.path()).await;
    let ingester_pool = IngesterPool::default();
    let traces_service = OtlpGrpcTracesService::new(ingester_client, Some(CommitType::Force));

    let storage_resolver = StorageResolver::unconfigured();
    let metastore = metastore_for_test(&storage_resolver).await;
    let (indexer_service, _indexer_handle) = indexer_for_test(
        &universe,
        temp_dir.path(),
        cluster.clone(),
        metastore.clone(),
        storage_resolver.clone(),
        ingester_service.clone(),
        ingester_pool.clone(),
    )
    .await;

    setup_traces_index(
        &temp_dir,
        metastore.clone(),
        &ingester_service,
        &indexer_service,
    )
    .await;

    let search_service =
        searcher_for_test(&cluster, metastore.clone(), storage_resolver.clone()).await;
    let jaeger_service = JaegerService::new(JaegerConfig::default(), search_service);

    cluster
        .wait_for_ready_members(|members| members.len() == 1, Duration::from_secs(5))
        .await
        .unwrap();

    {
        // Export traces.
        let export_trace_request = ExportTraceServiceRequest {
            resource_spans: make_resource_spans_for_test(),
        };
        traces_service
            .export(tonic::Request::new(export_trace_request))
            .await
            .unwrap();
    }
    {
        // Test `GetServices`
        let get_services_request = GetServicesRequest {};
        let get_services_response = jaeger_service
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
        let get_operations_response = jaeger_service
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
        let get_operations_response = jaeger_service
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
        let find_trace_ids_response = jaeger_service
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
        let find_trace_ids_response = jaeger_service
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
        let find_trace_ids_response = jaeger_service
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
        let find_trace_ids_response = jaeger_service
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
        let find_trace_ids_response = jaeger_service
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
        let mut span_stream = jaeger_service
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
    _indexer_handle.quit().await;
    universe.assert_quit().await;
}

async fn cluster_for_test() -> Cluster {
    let transport = ChannelTransport::default();
    create_cluster_for_test(
        Vec::new(),
        &["metastore", "indexer", "searcher"],
        &transport,
        true,
    )
    .await
    .unwrap()
}

async fn ingester_for_test(
    universe: &Universe,
    data_dir_path: &Path,
) -> (Mailbox<IngestApiService>, IngestServiceClient) {
    let queues_dir_path = data_dir_path.join(QUEUES_DIR_NAME);
    let ingester_service = init_ingest_api(universe, &queues_dir_path, &IngestApiConfig::default())
        .await
        .unwrap();
    let ingester_client = IngestServiceClient::from_mailbox(ingester_service.clone());
    (ingester_service, ingester_client)
}

async fn metastore_for_test(storage_resolver: &StorageResolver) -> MetastoreServiceClient {
    let storage = storage_resolver
        .resolve(&Uri::for_test("ram:///metastore"))
        .await
        .unwrap();
    MetastoreServiceClient::new(FileBackedMetastore::for_test(storage))
}

async fn indexer_for_test(
    universe: &Universe,
    data_dir_path: &Path,
    cluster: Cluster,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
    ingester_service: Mailbox<IngestApiService>,
    ingester_pool: IngesterPool,
) -> (Mailbox<IndexingService>, ActorHandle<IndexingService>) {
    let indexer_config = IndexerConfig::for_test().unwrap();
    let indexing_service = IndexingService::new(
        "test-node".to_string(),
        data_dir_path.to_path_buf(),
        indexer_config,
        1,
        cluster,
        metastore,
        Some(ingester_service),
        ingester_pool,
        storage_resolver,
        EventBroker::default(),
    )
    .await
    .unwrap();
    universe.spawn_builder().spawn(indexing_service)
}

async fn searcher_for_test(
    cluster: &Cluster,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
) -> Arc<dyn SearchService> {
    let searcher_config = SearcherConfig::default();
    let searcher_pool = SearcherPool::default();
    let search_job_placer = SearchJobPlacer::new(searcher_pool.clone());
    let searcher_context = Arc::new(SearcherContext::new(searcher_config, None));
    let searcher_service = start_searcher_service(
        metastore,
        storage_resolver,
        search_job_placer,
        searcher_context,
    )
    .await
    .unwrap();
    let grpc_advertise_addr = cluster
        .ready_members()
        .await
        .get(0)
        .unwrap()
        .grpc_advertise_addr;
    let searcher_client =
        SearchServiceClient::from_service(searcher_service.clone(), grpc_advertise_addr);
    searcher_pool.insert(grpc_advertise_addr, searcher_client);
    searcher_service
}

async fn setup_traces_index(
    temp_dir: &TempDir,
    mut metastore: MetastoreServiceClient,
    ingester_service: &Mailbox<IngestApiService>,
    indexer_service: &Mailbox<IndexingService>,
) {
    let index_root_uri: Uri = format!("{}", temp_dir.path().join("indexes").display())
        .parse()
        .unwrap();
    let index_config = OtlpGrpcTracesService::index_config(&index_root_uri).unwrap();
    let index_id = index_config.index_id.clone();
    let create_index_request = CreateIndexRequest::try_from_index_config(index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid
        .into();
    let source_config = SourceConfig::ingest_api_default();
    let add_source_request =
        AddSourceRequest::try_from_source_config(index_uid.clone(), source_config.clone()).unwrap();
    metastore.add_source(add_source_request).await.unwrap();

    let create_queue_request = CreateQueueRequest {
        queue_id: index_id.clone(),
    };
    ingester_service
        .ask_for_res(create_queue_request)
        .await
        .unwrap();
    let spawn_pipeline_request = SpawnPipeline {
        index_id: index_id.clone(),
        source_config,
        pipeline_uid: PipelineUid::default(),
    };
    indexer_service
        .ask_for_res(spawn_pipeline_request)
        .await
        .unwrap();
}
