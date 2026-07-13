use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::Stream;
use futures::stream::FuturesUnordered;
use itertools::Itertools;
use prost::Message as _;
use prost_types::Any;
use quickwit_cluster::{Cluster, ClusterNode};
use quickwit_common::ServiceStream;
use quickwit_config::service::QuickwitService;
use quickwit_config::{NodeConfig, RetentionPolicy, SourceConfig, validate_identifier};
#[cfg(feature = "datafusion")]
use quickwit_datafusion::{
    DataFusionService as QuickwitDataFusionService, DataFusionSessionBuilder,
};
use quickwit_metastore::{
    CreateIndexResponseExt, IndexMetadataResponseExt, ListIndexesMetadataResponseExt,
};
use quickwit_proto::ServiceError;
#[cfg(feature = "datafusion")]
use quickwit_proto::cloudprem::cloudprem_substrait_request;
use quickwit_proto::cloudprem::index::{
    IndexConfig as IndexConfigProto, IndexMetadata as IndexMetadataProto,
    RetentionPolicy as RetentionPolicyProto,
};
use quickwit_proto::cloudprem::metrics::{Label, MetricFamily, metric};
use quickwit_proto::cloudprem::{
    AggregationRequest, AggregationResponse, CloudPremError, CloudPremResult, CloudPremService,
    CloudPremServiceStream, CloudpremSubstraitRequest, CloudpremSubstraitResponse,
    CreateIndexRequest, CreateIndexResponse, DeleteIndexRequest, DeleteIndexResponse,
    EsHttpRequest, EsHttpResponse, Event, EventTracker, FetchOneRequest, FetchOneResponse,
    GetClusterDiagnosticsRequest, GetClusterDiagnosticsResponse, GetIndexRoutingTableRequest,
    GetIndexRoutingTableResponse, GetIndexesRequest, GetIndexesResponse, ListRequest, ListResponse,
    ListSplitsRequest, ListSplitsResponse, NodeDiagnostics, NodeMetrics, PingRequest, PingResponse,
    PullClusterMetricsResponse, ScalarType, SearchSplitRequest, SearchSplitResponse,
    SetIndexRoutingTableRequest, SetIndexRoutingTableResponse, SetLogLevelRequest,
    SetLogLevelResponse, SplitToken, Statistics, UpdateIndexRequest, UpdateIndexResponse,
    column_type,
};
use quickwit_proto::developer::{
    DeveloperService as _, DeveloperServiceClient, GetNodeDiagnosticsRequest, PullMetricsRequest,
    PullMetricsResponse, SetNodeLogLevelRequest,
};
use quickwit_proto::metastore::{
    CreateIndexRequest as MetastoreCreateIndexRequest,
    DeleteIndexRequest as MetastoreDeleteIndexRequest, ListIndexesMetadataRequest,
    MetastoreService, MetastoreServiceClient, UpdateIndexRequest as MetastoreUpdateIndexRequest,
    serde_utils,
};
use quickwit_proto::search::{
    CountHits, Hit, ListFieldsRequest, ListFieldsResponse, ListTermsRequest, ListTermsResponse,
    PartialHit, SearchRequest, SearchResponse, SortField, SortOrder,
};
use quickwit_proto::tonic::codec::CompressionEncoding;
use quickwit_query::MatchAllOrNone;
use quickwit_query::cloudprem::{apply_trace_id_rewrite, sanitize_metric_id_aggregations};
use quickwit_query::query_ast::{
    BoolQuery, FullTextMode, FullTextParams, FullTextQuery, QueryAst, TermQuery,
};
use quickwit_search::{
    BatchSize, ColumnRequest, ColumnarSplitPlanRequest, SearchError, SearchService,
    SearchSplitColumnarRequest,
};
use serde_json::Value as JsonValue;
use tokio::time::timeout;
use tokio_stream::StreamExt as _;
use tracing::{debug, error, info, warn};

use crate::developer_api::DeveloperApiServer;

pub(crate) const CLOUDPREM_INDEX_ID_PATTERN: &str = "datadog*";

/// Returns the index patterns to search. Falls back to `"datadog*"` when the
/// caller doesn't specify any, for backward compatibility.
pub(super) fn resolve_index_patterns(index_id_patterns: &[String]) -> Vec<String> {
    if index_id_patterns.is_empty() {
        return vec![CLOUDPREM_INDEX_ID_PATTERN.to_string()];
    }
    index_id_patterns.to_vec()
}

/// Normalizes the wire filter to a `QueryAst`. An absent `query_node` means
/// "match everything". Any time window is expected to already be encoded as
/// a range query over the timestamp field within the filter itself,
/// consistent with `List`/`FetchOne`/`Aggregate`.
fn query_node_to_query_ast(query_node: Option<Any>) -> CloudPremResult<QueryAst> {
    let Some(query_node) = query_node else {
        return Ok(QueryAst::MatchAll);
    };
    let evp_ast = quickwit_query::cloudprem::parse_query(query_node)
        .map_err(|error| CloudPremError::InvalidQuery(format!("failed to parse query: {error}")))?;
    let query_ast = quickwit_query::cloudprem::to_quickwit_query(evp_ast)?;
    Ok(apply_trace_id_rewrite(query_ast))
}

fn scalar_type_to_arrow(scalar_type: ScalarType) -> Option<arrow::datatypes::DataType> {
    use arrow::datatypes::TimeUnit;
    match scalar_type {
        ScalarType::Unspecified => None,
        ScalarType::String => Some(arrow::datatypes::DataType::Utf8),
        ScalarType::Int64 => Some(arrow::datatypes::DataType::Int64),
        ScalarType::Uint64 => Some(arrow::datatypes::DataType::UInt64),
        ScalarType::Float64 => Some(arrow::datatypes::DataType::Float64),
        ScalarType::Bool => Some(arrow::datatypes::DataType::Boolean),
        ScalarType::TimestampNanos => Some(arrow::datatypes::DataType::Timestamp(
            TimeUnit::Microsecond,
            None,
        )),
        ScalarType::Bytes => Some(arrow::datatypes::DataType::Binary),
        ScalarType::Ip => Some(arrow::datatypes::DataType::Utf8),
    }
}

const PULL_METRICS_TIMEOUT: Duration = Duration::from_secs(1);
const GET_NODE_DIAGNOSTICS_TIMEOUT: Duration = Duration::from_secs(5);
const SET_NODE_LOG_LEVEL_TIMEOUT: Duration = Duration::from_secs(1);

/// Capacity of the bounded channel buffering already-encoded `SearchSplit`
/// responses ahead of the gRPC transport.
///
/// This is the one point in the columnar read path where we decide how much
/// output to hold in memory while waiting on a slow network consumer: the
/// scan (`quickwit-search`) and the Arrow-IPC framing (`columnar.rs`) are
/// both plain, unbuffered streams, so nothing runs ahead of the consumer
/// until it reaches this buffer.
const SEARCH_SPLIT_RESPONSE_BUFFER_CAPACITY: usize = 4;

/// Buffers a lazily-produced response stream into a `ServiceStream` backed
/// by a bounded channel. The channel decouples the network consumer from
/// the scan/encode pipeline feeding it and applies backpressure once full;
/// if the consumer drops the stream, sends fail and the producer task stops
/// — no `JoinHandle::abort`, no `tokio::sync::Mutex`.
fn buffer_response_stream(
    response_stream: impl Stream<Item = CloudPremResult<SearchSplitResponse>> + Send + 'static,
) -> CloudPremServiceStream<SearchSplitResponse> {
    let (sender, stream) = ServiceStream::new_bounded(SEARCH_SPLIT_RESPONSE_BUFFER_CAPACITY);
    tokio::spawn(async move {
        let mut response_stream = Box::pin(response_stream);
        while let Some(item) = response_stream.next().await {
            if sender.send(item).await.is_err() {
                // Consumer dropped: stop producing.
                return;
            }
        }
    });
    stream
}

#[cfg(feature = "datafusion")]
fn encode_record_batches_as_arrow_ipc(
    batches: &[arrow::array::RecordBatch],
) -> CloudPremResult<Vec<u8>> {
    let mut ipc_buf = Vec::new();
    if let Some(batch) = batches.first() {
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(&mut ipc_buf, &batch.schema())
                .map_err(|error| CloudPremError::Internal(format!("ipc error: {error}")))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|error| CloudPremError::Internal(format!("ipc error: {error}")))?;
        }
        writer
            .finish()
            .map_err(|error| CloudPremError::Internal(format!("ipc error: {error}")))?;
    }
    Ok(ipc_buf)
}

#[cfg(feature = "datafusion")]
async fn collect_stream_as_arrow_ipc(
    stream: quickwit_datafusion::SendableRecordBatchStream,
) -> CloudPremResult<Vec<u8>> {
    let batches = datafusion::physical_plan::common::collect(stream)
        .await
        .map_err(|error| CloudPremError::Internal(format!("execution error: {error}")))?;
    encode_record_batches_as_arrow_ipc(&batches)
}

#[allow(dead_code)]
pub struct CloudPremServiceImpl {
    search_service: Arc<dyn SearchService>,
    metastore_client: MetastoreServiceClient,
    cluster: Cluster,
    node_config: Arc<NodeConfig>,
    #[cfg(feature = "datafusion")]
    datafusion_session_builder: Option<Arc<DataFusionSessionBuilder>>,
}

impl fmt::Debug for CloudPremServiceImpl {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CloudPremServiceImpl")
    }
}

impl CloudPremServiceImpl {
    pub fn new(
        search_service: Arc<dyn SearchService>,
        metastore_client: MetastoreServiceClient,
        cluster: Cluster,
        node_config: Arc<NodeConfig>,
        #[cfg(feature = "datafusion")] datafusion_session_builder: Option<
            Arc<DataFusionSessionBuilder>,
        >,
    ) -> Self {
        CloudPremServiceImpl {
            search_service,
            metastore_client,
            cluster,
            node_config,
            #[cfg(feature = "datafusion")]
            datafusion_session_builder,
        }
    }
}

fn doc_id_to_query_ast(doc_id: &str) -> QueryAst {
    let full_text_params = FullTextParams {
        tokenizer: None,
        mode: FullTextMode::Bool {
            operator: quickwit_query::BooleanOperand::And,
        },
        zero_terms_query: MatchAllOrNone::MatchNone,
    };
    // Right now, the id field does not use a raw tokenizer, so we cannot
    // rely on the term query.
    FullTextQuery {
        field: "id".to_string(),
        text: doc_id.to_string(),
        params: full_text_params,
        lenient: false,
    }
    .into()
}

fn timestamp_to_query_ast(timestamp_ms: u64) -> QueryAst {
    TermQuery {
        field: "timestamp".to_string(),
        // quickwit automatically detects this is ms
        value: timestamp_ms.to_string(),
    }
    .into()
}

#[async_trait]
impl CloudPremService for CloudPremServiceImpl {
    async fn ping(&self, _request: PingRequest) -> CloudPremResult<PingResponse> {
        info!("received Ping request");
        Ok(PingResponse {})
    }

    async fn list(&self, request: ListRequest) -> CloudPremResult<ListResponse> {
        // we don't use request.columns, not sure what to do with it rn
        info!("received List request");

        let Some(query) = request.query else {
            return Err(CloudPremError::Internal("missing query".to_string()));
        };
        let query_evp_ast = quickwit_query::cloudprem::parse_query(query)
            .map_err(|err| CloudPremError::InvalidQuery(format!("failed to parse query: {err}")))?;

        debug!("received ast: {query_evp_ast:?}");
        let query_ast = quickwit_query::cloudprem::to_quickwit_query(query_evp_ast)?;
        let query_ast = apply_trace_id_rewrite(query_ast);
        debug!("converted ast: {query_ast:?}");

        let count_hits = if request.should_compute_count {
            CountHits::CountAll
        } else {
            CountHits::Underestimate
        };

        let search_after = request
            .search_after
            .map(|after| DEFAULT_HIT_MAPPER.event_tracker_to_partial_hit(after));

        let search_request = SearchRequest {
            index_id_patterns: resolve_index_patterns(&request.index_id_patterns),
            query_ast: serde_json::to_string(&query_ast)
                .map_err(|error| CloudPremError::Internal(error.to_string()))?,
            start_timestamp: None,
            end_timestamp: None,
            max_hits: request.num_events_to_fetch.into(),
            start_offset: 0,
            aggregation_request: None,
            snippet_fields: Vec::new(),
            sort_fields: request
                .sort
                .into_iter()
                .map(|sort_kv| SortField {
                    field_name: sort_kv.path, // or should it be .name ?
                    sort_order: if sort_kv.ascending {
                        SortOrder::Asc
                    } else {
                        SortOrder::Desc
                    }
                    .into(),
                    sort_datetime_format: None,
                })
                .collect(),
            scroll_ttl_secs: None,
            search_after,
            count_hits: count_hits.into(),
            ignore_missing_indexes: false,
            skip_aggregation_finalization: false,
            enable_request_batching: request.enable_request_batching,
        };

        let response = self
            .search_service
            .root_search(search_request)
            .await
            .inspect_err(|e| warn!("list root search failed: {e}"))?;

        if let Some(search_error) = SearchError::from_split_errors(&response.failed_splits) {
            return Err(CloudPremError::Internal(search_error.to_string()));
        }

        let events = response
            .hits
            .into_iter()
            .map(|hit| DEFAULT_HIT_MAPPER.hit_to_event(hit))
            .collect::<Result<_, _>>()
            .inspect_err(|e| warn!("building hit list failed with: {e}"))?;

        let statistics = Statistics {
            hit_count: response.num_hits,
            scanned_count: 0,
            result_memory_size: 0u64,
            max_result_memory_size: 0u64,
        };

        Ok(ListResponse {
            count: response.num_hits,
            streams: vec![quickwit_proto::cloudprem::Stream { events }],
            statistics: Some(statistics),
        })
    }

    async fn list_splits(&self, request: ListSplitsRequest) -> CloudPremResult<ListSplitsResponse> {
        info!("received ListSplits request");
        let plan_request = ColumnarSplitPlanRequest {
            index_id_patterns: resolve_index_patterns(&request.index_id_patterns),
            query_ast: query_node_to_query_ast(request.query_node)?,
        };
        super::columnar::list_splits(&self.metastore_client, plan_request).await
    }

    async fn search_split(
        &self,
        request: SearchSplitRequest,
    ) -> CloudPremResult<CloudPremServiceStream<SearchSplitResponse>> {
        info!("received SearchSplit request");
        let token = SplitToken::decode(request.split_token.as_slice()).map_err(|error| {
            CloudPremError::InvalidQuery(format!("invalid split token: {error}"))
        })?;
        let Some(split) = token.split else {
            return Err(CloudPremError::InvalidQuery(
                "split token is missing split offsets".to_string(),
            ));
        };
        // Any timestamp window is expected to already be encoded as a range query
        // over the timestamp field within the filter's AST. Phase 1 only coarsely
        // prunes whole splits by it; `run_columnar_search` re-narrows it against
        // this split's own on-disk range and applies it as a per-document filter.
        let query_ast = query_node_to_query_ast(request.query_node)?;

        let mut columns = Vec::with_capacity(request.columns.len());
        for projection in request.columns {
            let scalar_type = match projection.r#type.and_then(|column_type| column_type.kind) {
                Some(column_type::Kind::Scalar(scalar_type)) => {
                    ScalarType::try_from(scalar_type).unwrap_or(ScalarType::Unspecified)
                }
                None => ScalarType::Unspecified,
            };
            let Some(data_type) = scalar_type_to_arrow(scalar_type) else {
                return Err(CloudPremError::InvalidQuery(format!(
                    "unsupported column type for `{}`",
                    projection.name
                )));
            };
            columns.push(ColumnRequest {
                name: projection.name,
                data_type,
            });
        }

        let columnar_request = SearchSplitColumnarRequest {
            index_uri: token.index_uri,
            doc_mapper_str: token.doc_mapper_str,
            split,
            query_ast,
            columns,
            batch_size: match request.batch_size {
                0 => BatchSize::Default,
                batch_size => BatchSize::Value(batch_size),
            },
            limit: (request.limit != 0).then_some(request.limit as usize),
        };

        let response_stream =
            super::columnar::search_split(&self.search_service, columnar_request).await?;
        Ok(buffer_response_stream(response_stream))
    }

    async fn fetch_one(
        &self,
        fetch_one_request: FetchOneRequest,
    ) -> CloudPremResult<FetchOneResponse> {
        let Some(event_tracker) = fetch_one_request.event_tracker.as_ref() else {
            error!("fetchone with missing event tracker");
            return Err(CloudPremError::InvalidQuery(
                "Missing event tracker".to_string(),
            ));
        };

        info!(id=%event_tracker.id, "received FetchOne request");

        let restriction_query = if let Some(query) = fetch_one_request.restriction_query {
            let query_evp_ast = quickwit_query::cloudprem::parse_query(query).map_err(|err| {
                CloudPremError::InvalidQuery(format!("failed to parse query: {err}"))
            })?;

            debug!("received ast: {query_evp_ast:?}");
            let query_ast = quickwit_query::cloudprem::to_quickwit_query(query_evp_ast)?;
            debug!("converted ast: {query_ast:?}");
            query_ast
        } else {
            QueryAst::MatchAll
        };

        let fetch_id_query = doc_id_to_query_ast(&event_tracker.id);
        let ts_filter = timestamp_to_query_ast(event_tracker.epoch_ms);

        let query_ast = QueryAst::Bool(BoolQuery {
            must: vec![fetch_id_query, restriction_query, ts_filter],
            ..BoolQuery::default()
        });

        let query_ast_json = serde_json::to_string(&query_ast)
            .map_err(|error| CloudPremError::Internal(error.to_string()))?;

        debug!(query=%query_ast_json, "query ast for fetch one");

        // TODO optimize fetch one by leveraging the information in the event tracker
        // (last seen split_id, etc.)
        let search_request = SearchRequest {
            index_id_patterns: resolve_index_patterns(&fetch_one_request.index_id_patterns),
            query_ast: query_ast_json,
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 1,
            start_offset: 0,
            aggregation_request: None,
            snippet_fields: Vec::new(),
            sort_fields: Vec::new(),
            scroll_ttl_secs: None,
            search_after: None,
            count_hits: CountHits::Underestimate.into(),
            ignore_missing_indexes: false,
            skip_aggregation_finalization: false,
            enable_request_batching: false,
        };

        let search_response: SearchResponse =
            self.search_service.root_search(search_request).await?;

        if search_response.hits.is_empty() {
            // Only treat split failures as errors if the document wasn't found, if we got a hit,
            // the failures are irrelevant.
            if let Some(search_error) =
                SearchError::from_split_errors(&search_response.failed_splits)
            {
                return Err(CloudPremError::Internal(search_error.to_string()));
            }
            warn!("document not found on fetch one");
            return Ok(FetchOneResponse {
                event: None,
                statistics: None,
            });
        }

        if search_response.hits.len() > 1 {
            warn!("there should be only one document");
        }

        let hit: Hit = search_response.hits.into_iter().next().unwrap();

        debug!(
            doc_id = event_tracker.id.as_str(),
            "fetch one document found"
        );

        let event = DEFAULT_HIT_MAPPER.hit_to_event(hit)?;

        Ok(FetchOneResponse {
            event: Some(event),
            statistics: None,
        })
    }

    async fn aggregate(&self, request: AggregationRequest) -> CloudPremResult<AggregationResponse> {
        info!("received Aggregation request");

        let Some(query) = request.query else {
            return Err(CloudPremError::Internal("missing query".to_string()));
        };
        let query_evp_ast = quickwit_query::cloudprem::parse_query(query)
            .map_err(|err| CloudPremError::InvalidQuery(format!("failed to parse query: {err}")))?;

        debug!("received query ast: {query_evp_ast:?}");
        let query_ast = quickwit_query::cloudprem::to_quickwit_query(query_evp_ast)
            .inspect_err(|e| warn!("failed to query map ast: {e}"))?;
        debug!("converted query ast: {query_ast:?}");

        let Some(mut evp_aggregation_ast) = request.aggregation else {
            return Err(CloudPremError::Internal("missing aggregation".to_string()));
        };

        sanitize_metric_id_aggregations(&mut evp_aggregation_ast);

        // TODO we can use ExtractTimestampRange to get some decent timestamp from the request
        // this is all a hack to somewhat support calendar invervals though
        let start_ts_secs = 1735686000; // 2025-01-01

        debug!("received aggregation ast {evp_aggregation_ast:?}");
        let aggregation_ast = quickwit_query::cloudprem::to_tantivy_aggregation(
            evp_aggregation_ast.clone(),
            start_ts_secs,
        )
        .inspect_err(|e| warn!("failed to aggregation map ast: {e}"))?;
        debug!("converted aggregation ast {aggregation_ast:?}");

        let search_request = SearchRequest {
            index_id_patterns: resolve_index_patterns(&request.index_id_patterns),
            query_ast: serde_json::to_string(&query_ast)
                .map_err(|error| CloudPremError::Internal(error.to_string()))?,
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 0,
            start_offset: 0,
            aggregation_request: Some(
                serde_json::to_string(&aggregation_ast)
                    .map_err(|error| CloudPremError::Internal(error.to_string()))?,
            ),
            snippet_fields: Vec::new(),
            sort_fields: Vec::new(),
            scroll_ttl_secs: None,
            search_after: None,
            // we can't really optimise about count in an aggregation request, and we may need the
            // count if the aggregation was in fact a COUNT(*) (which is omited from
            // aggregation ast)
            count_hits: CountHits::CountAll.into(),
            ignore_missing_indexes: false,
            skip_aggregation_finalization: true,
            enable_request_batching: request.enable_request_batching,
        };

        let response = self
            .search_service
            .root_search(search_request)
            .await
            .inspect_err(|e| warn!("list root search failed: {e}"))?;

        if let Some(search_error) = SearchError::from_split_errors(&response.failed_splits) {
            return Err(CloudPremError::Internal(search_error.to_string()));
        }

        tracing::trace!("request result: {response:?}");
        let aggregation_postcard_bytes: Vec<u8> =
            response.aggregation_postcard.ok_or_else(|| {
                CloudPremError::Internal("request generated no aggregation result".to_string())
            })?;

        let intermediate_results: tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults =
            postcard::from_bytes(&aggregation_postcard_bytes)
                .map_err(|err| {
                    CloudPremError::Internal(format!("failed to deserialize intermediate agg result: {err}"))
                })?;
        let cloudprem_aggregation_result =
            quickwit_query::cloudprem::intermediate_aggregation_result_to_proto(
                intermediate_results,
                &evp_aggregation_ast,
                response.num_hits,
            )?;
        tracing::trace!("aggregation result: {cloudprem_aggregation_result:?}");

        let statistics = Statistics {
            hit_count: response.num_hits,
            scanned_count: 0,
            result_memory_size: 0u64,
            max_result_memory_size: 0u64,
        };

        Ok(AggregationResponse {
            result: cloudprem_aggregation_result,
            statistics: Some(statistics),
        })
    }

    async fn root_search(
        &self,
        mut search_request: SearchRequest,
    ) -> Result<SearchResponse, CloudPremError> {
        // we don't want to ever access customer data here, that has to go through properly audited
        // channels
        filter_safe_indexes(&mut search_request.index_id_patterns)?;
        self.search_service
            .root_search(search_request)
            .await
            .map_err(Into::into)
    }

    async fn root_list_terms(
        &self,
        mut list_terms_request: ListTermsRequest,
    ) -> Result<ListTermsResponse, CloudPremError> {
        // we don't want to ever access customer data here, that has to go through properly audited
        // channels
        filter_safe_indexes(&mut list_terms_request.index_id_patterns)?;
        self.search_service
            .root_list_terms(list_terms_request)
            .await
            .map_err(Into::into)
    }

    async fn root_list_fields(
        &self,
        list_fields_request: ListFieldsRequest,
    ) -> Result<ListFieldsResponse, CloudPremError> {
        self.search_service
            .root_list_fields(list_fields_request)
            .await
            .map_err(Into::into)
    }

    async fn pull_cluster_metrics(
        &self,
        _request: quickwit_proto::cloudprem::PullClusterMetricsRequest,
    ) -> CloudPremResult<quickwit_proto::cloudprem::PullClusterMetricsResponse> {
        let ready_nodes = self.cluster.ready_nodes().await;

        let mut pull_metrics_futures = FuturesUnordered::new();
        let mut node_metrics: Vec<quickwit_proto::cloudprem::NodeMetrics> =
            Vec::with_capacity(ready_nodes.len());

        for ready_node in ready_nodes {
            let pull_metrics_fut = async move { build_node_metric_future(ready_node).await };
            pull_metrics_futures.push(pull_metrics_fut);
        }

        while let Some(single_node_metrics) = pull_metrics_futures.next().await {
            node_metrics.push(single_node_metrics);
        }
        Ok(PullClusterMetricsResponse { node_metrics })
    }

    async fn substrait_search(
        &self,
        request: CloudpremSubstraitRequest,
    ) -> CloudPremResult<CloudpremSubstraitResponse> {
        #[cfg(not(feature = "datafusion"))]
        {
            let _ = request;
            return Err(CloudPremError::Internal(
                "datafusion support is disabled at compile time".to_string(),
            ));
        }

        #[cfg(feature = "datafusion")]
        {
            use datafusion_sql::parser::DFParserBuilder;

            let session_builder = self.datafusion_session_builder.as_ref().ok_or_else(|| {
                CloudPremError::Internal(
                    "datafusion not configured; set QW_ENABLE_DATAFUSION_ENDPOINT=true".to_string(),
                )
            })?;

            let CloudpremSubstraitRequest {
                org_id,
                scope,
                source,
                tags,
                mut settings,
                query,
            } = request;
            let explain = matches!(settings.remove("explain"), Some(value) if value == "true");
            debug!(
                org_id,
                included_indices = ?scope.as_ref().map(|scope| &scope.included_indices),
                query_source = ?source.as_ref().map(|source| (&source.source, &source.query_name, &source.client_id)),
                tag_count = tags.len(),
                explain,
                "executing cloudprem substrait request"
            );

            let datafusion_service = QuickwitDataFusionService::new(Arc::clone(session_builder));
            let sql = match query {
                Some(cloudprem_substrait_request::Query::StringQuery(sql)) => sql,
                Some(cloudprem_substrait_request::Query::SubstraitPlan(plan_bytes)) => {
                    let stream = if explain {
                        datafusion_service
                            .explain_substrait(&plan_bytes, &settings)
                            .await
                            .map_err(|error| {
                                CloudPremError::Internal(format!("plan error: {error}"))
                            })?
                    } else {
                        datafusion_service
                            .execute_substrait(&plan_bytes, &settings)
                            .await
                            .map_err(|error| {
                                CloudPremError::Internal(format!("execution error: {error}"))
                            })?
                    };
                    let ipc_buf = collect_stream_as_arrow_ipc(stream).await?;
                    return Ok(CloudpremSubstraitResponse {
                        arrow_ipc_bytes: ipc_buf,
                    });
                }
                None => return Err(CloudPremError::Internal("missing query".to_string())),
            };

            let ctx = session_builder
                .build_session_with_properties(&settings)
                .map_err(|error| CloudPremError::Internal(format!("session error: {error}")))?;

            let mut statements = DFParserBuilder::new(sql.as_str())
                .build()
                .and_then(|mut parser| parser.parse_statements())
                .map_err(|error| CloudPremError::Internal(format!("parse error: {error}")))?;

            let mut last_df = None;
            while let Some(statement) = statements.pop_front() {
                let plan = ctx
                    .state()
                    .statement_to_plan(statement)
                    .await
                    .map_err(|error| CloudPremError::Internal(format!("plan error: {error}")))?;
                let df = ctx
                    .execute_logical_plan(plan)
                    .await
                    .map_err(|error| CloudPremError::Internal(format!("sql error: {error}")))?;
                last_df = Some(df);
            }
            let df = last_df
                .ok_or_else(|| CloudPremError::Internal("no statements provided".to_string()))?;

            if explain {
                let plan = df
                    .create_physical_plan()
                    .await
                    .map_err(|error| CloudPremError::Internal(format!("plan error: {error}")))?;
                let plan_text = format!(
                    "{}",
                    datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
                );
                let schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
                    "plan",
                    arrow::datatypes::DataType::Utf8,
                    false,
                )]);
                let batch = arrow::array::RecordBatch::try_new(
                    Arc::new(schema),
                    vec![Arc::new(arrow::array::StringArray::from(vec![
                        plan_text.as_str(),
                    ]))],
                )
                .map_err(|error| {
                    CloudPremError::Internal(format!("record batch error: {error}"))
                })?;
                let ipc_buf = encode_record_batches_as_arrow_ipc(&[batch])?;
                return Ok(CloudpremSubstraitResponse {
                    arrow_ipc_bytes: ipc_buf,
                });
            }

            let batches = df
                .collect()
                .await
                .map_err(|error| CloudPremError::Internal(format!("execution error: {error}")))?;
            let ipc_buf = encode_record_batches_as_arrow_ipc(&batches)?;
            Ok(CloudpremSubstraitResponse {
                arrow_ipc_bytes: ipc_buf,
            })
        }
    }

    async fn inverted_request_stream(
        &self,
        _: ServiceStream<quickwit_proto::cloudprem::AnyResponse>,
    ) -> Result<
        ServiceStream<Result<quickwit_proto::cloudprem::AnyRequest, CloudPremError>>,
        CloudPremError,
    > {
        Err(CloudPremError::Unimplemented)
    }

    async fn get_indexes(
        &self,
        _request: GetIndexesRequest,
    ) -> CloudPremResult<GetIndexesResponse> {
        info!("received GetIndexes request");

        let indexes_metadata = self
            .metastore_client
            .clone()
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await?
            .deserialize_indexes_metadata()
            .await?;

        let indexes = indexes_metadata
            .iter()
            .map(index_metadata_to_proto)
            .collect();

        Ok(GetIndexesResponse { indexes })
    }

    async fn create_index(
        &self,
        request: CreateIndexRequest,
    ) -> CloudPremResult<CreateIndexResponse> {
        info!(index_id=%request.index_id, "received CreateIndex request");

        // Validate index_id
        validate_identifier("index ID", &request.index_id)
            .map_err(|error| CloudPremError::InvalidArgument(error.to_string()))?;

        // Build index_uri from default root
        let default_index_root_uri = &self.node_config.default_index_root_uri;
        let index_uri = default_index_root_uri
            .join(&request.index_id)
            .map_err(|error| CloudPremError::Internal(error.to_string()))?;

        // Load the default datadog-logs.yaml config
        let default_config_bytes = include_bytes!("../../../../config/cloudprem/datadog-logs.yaml");
        let mut index_config = quickwit_config::load_index_config_from_user_config(
            quickwit_config::ConfigFormat::Yaml,
            default_config_bytes,
            default_index_root_uri,
        )
        .map_err(|error| CloudPremError::Internal(error.to_string()))?;

        // Override the index_id and index_uri from the request
        index_config.index_id = request.index_id.clone();
        index_config.index_uri = index_uri;

        // Patch with retention policy from request if provided
        if let Some(proto_config) = request.index_config {
            if let Some(proto_rp) = proto_config.retention_policy {
                let retention_policy = RetentionPolicy {
                    retention_period: proto_rp.period,
                    evaluation_schedule: RetentionPolicy::default_schedule(),
                };
                retention_policy.retention_period().map_err(|error| {
                    CloudPremError::InvalidArgument(format!("invalid retention period: {error}"))
                })?;
                index_config.retention_policy_opt = Some(retention_policy);
            } else {
                // If index_config is provided but retention_policy is None, remove it
                index_config.retention_policy_opt = None;
            }
        }

        // Serialize config
        let index_config_json = serde_utils::to_json_str(&index_config)?;

        // Add default sources
        let source_configs_json = vec![
            serde_utils::to_json_str(&SourceConfig::ingest_api_default())?,
            serde_utils::to_json_str(&SourceConfig::ingest_v2())?,
        ];

        let create_response = self
            .metastore_client
            .clone()
            .create_index(MetastoreCreateIndexRequest {
                index_config_json,
                source_configs_json,
            })
            .await?;

        let index_metadata = create_response.deserialize_index_metadata()?;

        let index_metadata_proto = index_metadata_to_proto(&index_metadata);

        Ok(CreateIndexResponse {
            index_metadata: Some(index_metadata_proto),
        })
    }

    async fn update_index(
        &self,
        request: UpdateIndexRequest,
    ) -> CloudPremResult<UpdateIndexResponse> {
        info!(index_id=%request.index_id, "received UpdateIndex request");

        // Get current index metadata
        let index_metadata_request =
            quickwit_proto::metastore::IndexMetadataRequest::for_index_id(request.index_id.clone());
        let current_metadata_response = self
            .metastore_client
            .clone()
            .index_metadata(index_metadata_request)
            .await?;

        let current_metadata = current_metadata_response.deserialize_index_metadata()?;
        let index_uid = current_metadata.index_uid.clone();
        let mut updated_config = current_metadata.into_index_config();

        // Update retention policy if provided
        if let Some(index_config) = request.index_config {
            if let Some(proto_rp) = index_config.retention_policy {
                // Validate that the index has a timestamp field before allowing retention
                // policy
                if updated_config.doc_mapping.timestamp_field.is_none() {
                    return Err(quickwit_proto::metastore::MetastoreError::InvalidArgument {
                        message: "Cannot set retention policy: retention policy requires a \
                                  timestamp field, but doc mapping does not declare one"
                            .to_string(),
                    }
                    .into());
                }
                let retention_policy = RetentionPolicy {
                    retention_period: proto_rp.period,
                    evaluation_schedule: RetentionPolicy::default_schedule(),
                };
                retention_policy.retention_period().map_err(|error| {
                    CloudPremError::InvalidArgument(format!("invalid retention period: {error}"))
                })?;
                updated_config.retention_policy_opt = Some(retention_policy);
            } else {
                // If no retention policy provided, remove it
                updated_config.retention_policy_opt = None;
            }
        }

        // Serialize each part of the config
        let doc_mapping_json = serde_utils::to_json_str(&updated_config.doc_mapping)?;
        let indexing_settings_json = serde_utils::to_json_str(&updated_config.indexing_settings)?;
        let ingest_settings_json = serde_utils::to_json_str(&updated_config.ingest_settings)?;
        let search_settings_json = serde_utils::to_json_str(&updated_config.search_settings)?;
        let retention_policy_json_opt = updated_config
            .retention_policy_opt
            .as_ref()
            .map(serde_utils::to_json_str)
            .transpose()?;

        let update_response = self
            .metastore_client
            .clone()
            .update_index(MetastoreUpdateIndexRequest {
                index_uid: Some(index_uid),
                doc_mapping_json,
                indexing_settings_json,
                ingest_settings_json,
                search_settings_json,
                retention_policy_json_opt,
            })
            .await?;

        let updated_metadata = update_response.deserialize_index_metadata()?;

        let index_metadata_proto = index_metadata_to_proto(&updated_metadata);

        Ok(UpdateIndexResponse {
            index_metadata: Some(index_metadata_proto),
        })
    }

    async fn delete_index(
        &self,
        request: DeleteIndexRequest,
    ) -> CloudPremResult<DeleteIndexResponse> {
        info!(index_id=%request.index_id, "received DeleteIndex request");

        // Get index metadata to obtain the IndexUid
        let index_metadata_request =
            quickwit_proto::metastore::IndexMetadataRequest::for_index_id(request.index_id.clone());
        let index_metadata_response = self
            .metastore_client
            .clone()
            .index_metadata(index_metadata_request)
            .await?;

        let index_metadata = index_metadata_response.deserialize_index_metadata()?;
        let index_uid = index_metadata.index_uid;

        self.metastore_client
            .clone()
            .delete_index(MetastoreDeleteIndexRequest {
                index_uid: Some(index_uid),
            })
            .await?;

        Ok(DeleteIndexResponse {})
    }

    async fn get_index_routing_table(
        &self,
        _request: GetIndexRoutingTableRequest,
    ) -> CloudPremResult<GetIndexRoutingTableResponse> {
        info!("received GetIndexRoutingTable request");

        let rules =
            crate::datadog_api::index_router::get_or_default_routing_rules(&self.metastore_client)
                .await
                .map_err(|error| CloudPremError::Internal(error.to_string()))?;

        let routing_table = rules.into_iter().map(Into::into).collect();

        Ok(GetIndexRoutingTableResponse { routing_table })
    }

    async fn set_index_routing_table(
        &self,
        request: SetIndexRoutingTableRequest,
    ) -> CloudPremResult<SetIndexRoutingTableResponse> {
        info!("received SetIndexRoutingTable request");

        let metastore_request = quickwit_proto::metastore::SetIndexRoutingTableRequest {
            rules: request.routing_table.into_iter().map(Into::into).collect(),
        };
        self.metastore_client
            .clone()
            .set_index_routing_table(metastore_request)
            .await?;

        Ok(SetIndexRoutingTableResponse {})
    }

    async fn get_cluster_diagnostics(
        &self,
        _request: GetClusterDiagnosticsRequest,
    ) -> CloudPremResult<GetClusterDiagnosticsResponse> {
        info!("received GetClusterDiagnostics request");

        let ready_nodes = self.cluster.ready_nodes().await;
        let mut cluster_diagnostics: HashMap<String, NodeDiagnostics> =
            HashMap::with_capacity(ready_nodes.len());

        let mut get_node_diagnostics_futures = FuturesUnordered::new();

        for ready_node in ready_nodes {
            let node_id = ready_node.node_id.clone();
            let client = DeveloperServiceClient::from_channel(
                ready_node.grpc_advertise_addr,
                ready_node.channel(),
                DeveloperApiServer::MAX_GRPC_MESSAGE_SIZE,
                Some(CompressionEncoding::Zstd),
            );
            let get_node_diagnostics_future = async move {
                let get_node_diagnostics_res = timeout(
                    GET_NODE_DIAGNOSTICS_TIMEOUT,
                    client.get_node_diagnostics(GetNodeDiagnosticsRequest {}),
                )
                .await;
                (node_id, get_node_diagnostics_res)
            };
            get_node_diagnostics_futures.push(get_node_diagnostics_future);
        }

        while let Some(future_res) = get_node_diagnostics_futures.next().await {
            let (node_id, get_node_diagnostics_res) = future_res;
            let node_diagnostics: NodeDiagnostics = match get_node_diagnostics_res {
                Ok(Ok(resp)) => NodeDiagnostics {
                    status_code: 200,
                    build_info_json: resp.build_info_json,
                    runtime_info_json: resp.runtime_info_json,
                    node_config_json: resp.node_config_json,
                    env_info_json: resp.env_info_json,
                    deployment_info_json: resp.deployment_info_json,
                },
                Ok(Err(error)) => {
                    error!(%node_id, %error, "failed to get diagnostics from node");
                    NodeDiagnostics {
                        status_code: error.error_code().http_status_code().as_u16() as u32,
                        ..Default::default()
                    }
                }
                Err(_elapsed) => {
                    error!(%node_id, "GetNodeDiagnostics request timed out");
                    NodeDiagnostics {
                        status_code: http::StatusCode::REQUEST_TIMEOUT.as_u16() as u32,
                        ..Default::default()
                    }
                }
            };
            cluster_diagnostics.insert(node_id.to_string(), node_diagnostics);
        }

        Ok(GetClusterDiagnosticsResponse {
            cluster_diagnostics,
        })
    }

    async fn es_query(&self, request: EsHttpRequest) -> CloudPremResult<EsHttpResponse> {
        info!(%request.method, %request.path, "received EsQuery request");
        super::es_query::handle_es_query(
            request,
            self.search_service.clone(),
            self.metastore_client.clone(),
            self.cluster.clone(),
            self.node_config.clone(),
        )
        .await
    }

    async fn set_log_level(
        &self,
        request: SetLogLevelRequest,
    ) -> CloudPremResult<SetLogLevelResponse> {
        info!(filter = request.filter, "received SetLogLevel request");

        let ready_nodes = self.cluster.ready_nodes().await;
        let mut set_log_level_futures = FuturesUnordered::new();

        for ready_node in ready_nodes {
            let node_id = ready_node.node_id.clone();
            let client = DeveloperServiceClient::from_channel(
                ready_node.grpc_advertise_addr,
                ready_node.channel(),
                DeveloperApiServer::MAX_GRPC_MESSAGE_SIZE,
                Some(CompressionEncoding::Zstd),
            );
            let filter = request.filter.clone();
            let set_log_level_future = async move {
                let res = timeout(
                    SET_NODE_LOG_LEVEL_TIMEOUT,
                    client.set_node_log_level(SetNodeLogLevelRequest { filter }),
                )
                .await;
                (node_id, res)
            };
            set_log_level_futures.push(set_log_level_future);
        }

        let mut failed_nodes: Vec<String> = Vec::with_capacity(set_log_level_futures.len());
        while let Some((node_id, res)) = set_log_level_futures.next().await {
            match res {
                Ok(Ok(_)) => {}
                Ok(Err(err)) => {
                    error!(%node_id, %err, "failed to set log level on node");
                    failed_nodes.push(node_id.to_string());
                }
                Err(_elapsed) => {
                    error!(%node_id, "set node log level request timed out");
                    failed_nodes.push(node_id.to_string());
                }
            }
        }

        Ok(SetLogLevelResponse { failed_nodes })
    }
}

/// Converts quickwit IndexMetadata to cloudprem proto format.
fn index_metadata_to_proto(metadata: &quickwit_metastore::IndexMetadata) -> IndexMetadataProto {
    let retention_policy = metadata
        .index_config
        .retention_policy_opt
        .as_ref()
        .map(|rp| RetentionPolicyProto {
            period: rp.retention_period.clone(),
        });

    IndexMetadataProto {
        index_id: metadata.index_config.index_id.clone(),
        index_uri: metadata.index_config.index_uri.to_string(),
        create_timestamp: metadata.create_timestamp,
        index_config: Some(IndexConfigProto { retention_policy }),
    }
}

/// Computes the labels that apply to the entire pod.
/// Right now, we only have the `services` label.
fn node_labels(ready_node: &ClusterNode) -> Vec<Label> {
    // Multivalued labels (several values for one key) are supported by datadog.
    //
    // I suspect they might lead to wrong/confusing metrics so I prefer
    // emitting the set as a comma-separated string.
    let service_label_value: String = ready_node
        .enabled_services
        .iter()
        .map(QuickwitService::as_str)
        .sorted()
        .join(",");
    let services_label = Label {
        name: "services".to_string(),
        value: service_label_value,
    };
    vec![services_label]
}

async fn build_node_metric_future(ready_node: ClusterNode) -> NodeMetrics {
    let node_id = ready_node.node_id.clone();
    let node_labels = node_labels(&ready_node);
    let client = DeveloperServiceClient::from_channel(
        ready_node.grpc_advertise_addr,
        ready_node.channel(),
        DeveloperApiServer::MAX_GRPC_MESSAGE_SIZE,
        Some(CompressionEncoding::Zstd),
    );
    let pull_metrics_result = timeout(
        PULL_METRICS_TIMEOUT,
        client.pull_metrics(PullMetricsRequest {}),
    )
    .await;
    let metric_families_res: Result<Vec<MetricFamily>, http::StatusCode> = match pull_metrics_result
    {
        Ok(Ok(PullMetricsResponse { metric_families })) => Ok(metric_families),
        Err(_) => Err(http::StatusCode::REQUEST_TIMEOUT),
        Ok(Err(err)) => Err(err.error_code().http_status_code()),
    };
    let status_code: http::StatusCode = metric_families_res
        .as_ref()
        .err()
        .cloned()
        .unwrap_or(http::StatusCode::OK);
    let mut metric_families = metric_families_res.unwrap_or_default();
    for metric_family in &mut metric_families {
        metric_family.metrics.retain(|metric| {
            let has_pipeline_uid = metric.labels.iter().any(|l| l.name == "pipeline_uid");
            if !has_pipeline_uid {
                return true;
            }
            match metric.metric_value {
                Some(metric::MetricValue::Counter(v)) => v != 0,
                _ => true,
            }
        });
    }
    NodeMetrics {
        node_id: node_id.to_string(),
        status_code: status_code.as_u16() as u32,
        node_labels,
        metric_families,
    }
}

fn filter_safe_indexes(index_id_patterns: &mut Vec<String>) -> CloudPremResult<()> {
    let safe_pattern_char = |c: char| c.is_ascii_alphanumeric() || "-._*".contains(c);

    index_id_patterns
        .retain(|pattern| pattern.starts_with("otel-") && pattern.chars().all(safe_pattern_char));
    if index_id_patterns.is_empty() {
        Err(CloudPremError::InvalidQuery(
            "no safe index targeted".to_string(),
        ))
    } else {
        Ok(())
    }
}

struct HitMapper {
    id_field: &'static str,
    ts_field: &'static str,
    tiebreaker_field: &'static str,
}

const DEFAULT_HIT_MAPPER: HitMapper = HitMapper {
    id_field: "id",
    ts_field: "timestamp",
    tiebreaker_field: "tiebreaker",
};

impl HitMapper {
    fn hit_to_event(&self, hit: Hit) -> CloudPremResult<Event> {
        // TODO use serde_json_borrowed ?
        let map: serde_json::Map<String, JsonValue> = serde_json::from_str(&hit.json)
            .map_err(|error| CloudPremError::Internal(format!("failed to parse hit: {error}")))?;

        let event_id = if let Some(JsonValue::String(id_str)) = map.get(self.id_field) {
            id_str.clone()
        } else {
            "missing_id".to_string()
        };

        let timestamp = if let Some(JsonValue::String(ts)) = map.get(self.ts_field) {
            quickwit_datetime::parse_date_time_str(
                ts,
                &[quickwit_datetime::DateTimeInputFormat::Rfc3339],
            )
            .map(|ts| ts.into_timestamp_millis())
            .unwrap_or(0) as u64
        } else {
            0
        };

        let tiebreaker = if let Some(JsonValue::Number(tiebreaker)) = map.get(self.tiebreaker_field)
        {
            tiebreaker.as_i64().unwrap_or_default() as i32
        } else {
            0
        };

        Ok(Event {
            tracker: Some(EventTracker {
                id: event_id,
                epoch_ms: timestamp,
                tiebreaker,
                row_number: hit
                    .partial_hit
                    .as_ref()
                    .map(|partial_hit| u64::from(partial_hit.doc_id)),
                fragment_id: hit.partial_hit.map(|partial_hit| partial_hit.split_id),
            }),
            content_json: hit.json,
        })
    }

    fn event_tracker_to_partial_hit(&self, event: EventTracker) -> PartialHit {
        let make_uint_value = |value| {
            Some(quickwit_proto::search::SortByValue {
                sort_value: Some(quickwit_proto::search::sort_by_value::SortValue::U64(value)),
            })
        };
        let make_int_value = |value| {
            Some(quickwit_proto::search::SortByValue {
                sort_value: Some(quickwit_proto::search::sort_by_value::SortValue::I64(value)),
            })
        };
        // this assumes all requests are sorted by timestamp+tiebreaker
        // the timestamps we provide must be ms unless with force ns in doc mapping
        PartialHit {
            sort_value: make_uint_value(event.epoch_ms),
            sort_value2: make_int_value(event.tiebreaker.into()),
            split_id: event.fragment_id.unwrap_or_default(),
            segment_ord: 0,
            doc_id: event.row_number.unwrap_or_default() as u32,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use prost::Message;
    use prost_types::Any;
    use quickwit_cluster::{ChitchatTransport, create_cluster_for_test};
    use quickwit_config::NodeConfig;
    use quickwit_proto::cloudprem::{MatchNoneQueryNode, QueryNode, query_node};
    use quickwit_proto::ingest::ingester::IngesterStatus;
    use quickwit_proto::metastore::{MetastoreServiceClient, MockMetastoreService};
    use quickwit_proto::search::{Hit, SearchResponse};
    use quickwit_search::MockSearchService;

    use super::*;

    async fn make_service_with_mock_search(mock_search: MockSearchService) -> CloudPremServiceImpl {
        let transport = ChitchatTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &[], &transport, true)
            .await
            .unwrap();
        let metastore_client = MetastoreServiceClient::from_mock(MockMetastoreService::new());
        let node_config = Arc::new(NodeConfig::for_test());
        CloudPremServiceImpl::new(
            Arc::new(mock_search),
            metastore_client,
            cluster,
            node_config,
            #[cfg(feature = "datafusion")]
            None,
        )
    }

    fn make_fetch_one_request(doc_id: &str, epoch_ms: u64) -> FetchOneRequest {
        FetchOneRequest {
            event_tracker: Some(EventTracker {
                id: doc_id.to_string(),
                epoch_ms,
                tiebreaker: 0,
                fragment_id: None,
                row_number: None,
            }),
            restriction_query: None,
            org_id: 0,
            scope: None,
            index_id_patterns: Vec::new(),
        }
    }

    #[test]
    fn query_node_absent_matches_everything() {
        assert!(matches!(
            query_node_to_query_ast(None).unwrap(),
            QueryAst::MatchAll
        ));
    }

    #[test]
    fn scalar_type_mapping() {
        use arrow::datatypes::TimeUnit;

        assert_eq!(scalar_type_to_arrow(ScalarType::Unspecified), None);
        assert_eq!(
            scalar_type_to_arrow(ScalarType::String),
            Some(arrow::datatypes::DataType::Utf8)
        );
        assert_eq!(
            scalar_type_to_arrow(ScalarType::Uint64),
            Some(arrow::datatypes::DataType::UInt64)
        );
        assert_eq!(
            scalar_type_to_arrow(ScalarType::TimestampNanos),
            Some(arrow::datatypes::DataType::Timestamp(
                TimeUnit::Microsecond,
                None
            ))
        );
        assert_eq!(
            scalar_type_to_arrow(ScalarType::Ip),
            Some(arrow::datatypes::DataType::Utf8)
        );
    }

    #[tokio::test]
    async fn test_fetch_one() {
        const DOC_ID: &str = "test-doc-abc123";
        let epoch_ms = 1704067200000u64;

        let restriction_query = Any {
            type_url: "type.googleapis.com/queryparser_proto.QueryNode".to_string(),
            value: QueryNode {
                node: Some(query_node::Node::None(MatchNoneQueryNode {})),
            }
            .encode_to_vec(),
        };

        let mut mock_search = MockSearchService::new();
        mock_search
            .expect_root_search()
            .once()
            .returning(move |request| {
                let query_ast: QueryAst = serde_json::from_str(&request.query_ast).unwrap();
                let QueryAst::Bool(bool_query) = query_ast else {
                    panic!("expected a BoolQuery, got {query_ast:?}");
                };
                // doc_id filter
                assert!(
                    bool_query.must.iter().any(|clause| {
                        matches!(clause, QueryAst::FullText(fq) if fq.field == "id" && fq.text == DOC_ID)
                    }),
                    "missing FullTextQuery filter on id={DOC_ID}"
                );
                // timestamp filter
                assert!(
                    bool_query.must.iter().any(|clause| {
                        matches!(clause, QueryAst::Term(tq) if tq.field == "timestamp" && tq.value == epoch_ms.to_string())
                    }),
                    "missing TermQuery filter on timestamp={epoch_ms}"
                );
                // restriction query (MatchNone QueryNode -> QueryAst::MatchNone)
                assert!(
                    bool_query
                        .must
                        .iter()
                        .any(|clause| matches!(clause, QueryAst::MatchNone)),
                    "restriction query was not forwarded into the BoolQuery must clauses"
                );
                Ok(SearchResponse {
                    hits: vec![Hit {
                        json: serde_json::json!({
                            "id": DOC_ID,
                            "timestamp": "2024-01-01T00:00:00Z",
                            "tiebreaker": 42,
                        })
                        .to_string(),
                        partial_hit: None,
                        snippet: None,
                        index_id: "datadog-logs".to_string(),
                    }],
                    num_hits: 1,
                    ..Default::default()
                })
            });

        let service = make_service_with_mock_search(mock_search).await;
        let mut request = make_fetch_one_request(DOC_ID, epoch_ms);
        request.restriction_query = Some(restriction_query);
        let response = service.fetch_one(request).await.unwrap();
        let event = response.event.expect("expected an event in the response");
        let tracker = event.tracker.expect("expected a tracker on the event");
        assert_eq!(tracker.id, DOC_ID);
        assert_eq!(tracker.epoch_ms, epoch_ms);
        assert_eq!(tracker.tiebreaker, 42);
    }

    #[tokio::test]
    async fn test_node_labels() {
        let cluster_node = ClusterNode::for_test(
            "my_node",
            10001,
            true,
            &[
                QuickwitService::Indexer.as_str(),
                QuickwitService::Searcher.as_str(),
            ],
            &[],
            IngesterStatus::Ready,
        )
        .await;
        let node_labels: Vec<Label> = node_labels(&cluster_node);
        assert_eq!(node_labels.len(), 1);
        assert_eq!(
            node_labels[0],
            Label {
                name: "services".to_string(),
                value: "indexer,searcher".to_string()
            }
        );
    }
}
