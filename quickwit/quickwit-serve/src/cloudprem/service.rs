use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use itertools::Itertools;
use quickwit_cluster::{Cluster, ClusterNode};
use quickwit_common::ServiceStream;
use quickwit_config::service::QuickwitService;
use quickwit_proto::ServiceError as _;
use quickwit_proto::cloudprem::metrics::{Label, MetricFamily};
use quickwit_proto::cloudprem::{
    AggregationRequest, AggregationResponse, CloudPremError, CloudPremResult, CloudPremService,
    Event, EventTracker, FetchOneRequest, FetchOneResponse, ListRequest, ListResponse, NodeMetrics,
    PingRequest, PingResponse, PullClusterMetricsResponse, Statistics,
};
use quickwit_proto::developer::{
    DeveloperService as _, DeveloperServiceClient, PullMetricsRequest, PullMetricsResponse,
};
use quickwit_proto::search::{
    CountHits, Hit, ListTermsRequest, ListTermsResponse, PartialHit, SearchRequest, SearchResponse,
    SortField, SortOrder,
};
use quickwit_proto::tonic::codec::CompressionEncoding;
use quickwit_query::MatchAllOrNone;
use quickwit_query::query_ast::{BoolQuery, FullTextMode, FullTextParams, FullTextQuery, QueryAst};
use quickwit_search::SearchService;
use serde_json::Value as JsonValue;
use tokio_stream::StreamExt as _;
use tracing::{debug, error, info, warn};

use crate::developer_api::DeveloperApiServer;

// TODO this should become configurable and sent by EVP
pub const CLOUDPREM_INDEX_ID_PATTERN: &str = "datadog*";

const PULL_METRICS_TIMEOUT: Duration = Duration::from_secs(1);

#[allow(dead_code)]
pub struct CloudPremServiceImpl {
    search_service: Arc<dyn SearchService>,
    cluster: Cluster,
}

impl fmt::Debug for CloudPremServiceImpl {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CloudPremServiceImpl")
    }
}

impl CloudPremServiceImpl {
    pub fn new(search_service: Arc<dyn SearchService>, cluster: Cluster) -> Self {
        CloudPremServiceImpl {
            search_service,
            cluster,
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
    QueryAst::FullText(FullTextQuery {
        field: "id".to_string(),
        text: doc_id.to_string(),
        params: full_text_params,
        lenient: false,
    })
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
            index_id_patterns: vec![CLOUDPREM_INDEX_ID_PATTERN.to_string()],
            query_ast: serde_json::to_string(&query_ast)
                .map_err(|e| CloudPremError::Internal(e.to_string()))?,
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
        };

        let response = self
            .search_service
            .root_search(search_request)
            .await
            .inspect_err(|e| warn!("list root search failed: {e}"))?;

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

        let query_ast = QueryAst::Bool(BoolQuery {
            must: vec![fetch_id_query, restriction_query],
            ..BoolQuery::default()
        });

        let query_ast_json = serde_json::to_string(&query_ast)
            .map_err(|e| CloudPremError::Internal(e.to_string()))?;

        debug!(query=%query_ast_json, "query ast for fetch one");

        // TODO optimize fetch one by leveraging the information in the event tracker
        // (last seen split_id, etc.)
        let search_request = SearchRequest {
            index_id_patterns: vec![CLOUDPREM_INDEX_ID_PATTERN.to_string()],
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
        };

        let search_response: SearchResponse =
            self.search_service.root_search(search_request).await?;

        if search_response.hits.is_empty() {
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

        let Some(evp_aggregation_ast) = request.aggregation else {
            return Err(CloudPremError::Internal("missing aggregation".to_string()));
        };

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
            index_id_patterns: vec![CLOUDPREM_INDEX_ID_PATTERN.to_string()],
            query_ast: serde_json::to_string(&query_ast)
                .map_err(|e| CloudPremError::Internal(e.to_string()))?,
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 0,
            start_offset: 0,
            aggregation_request: Some(
                serde_json::to_string(&aggregation_ast)
                    .map_err(|e| CloudPremError::Internal(e.to_string()))?,
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
        };

        let response = self
            .search_service
            .root_search(search_request)
            .await
            .inspect_err(|e| warn!("list root search failed: {e}"))?;

        tracing::trace!("request result: {response:?}");
        let aggregation_postcard_bytes: Vec<u8> =
            response.aggregation_postcard.ok_or_else(|| {
                CloudPremError::Internal("request generated no aggregation result".to_string())
            })?;
        let quickwit_aggregation_result = postcard::from_bytes(&aggregation_postcard_bytes)
            .map_err(|err| {
                CloudPremError::Internal(format!("failed to deserialize agg result: {err}"))
            })?;
        let cloudprem_aggregation_result = quickwit_query::cloudprem::aggregation_result_to_proto(
            quickwit_aggregation_result,
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
        filter_safe_indexes(&mut search_request.index_id_patterns);
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
        filter_safe_indexes(&mut list_terms_request.index_id_patterns);
        self.search_service
            .root_list_terms(list_terms_request)
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

    async fn inverted_request_stream(
        &self,
        _: ServiceStream<quickwit_proto::cloudprem::AnyResponse>,
    ) -> Result<
        ServiceStream<Result<quickwit_proto::cloudprem::AnyRequest, CloudPremError>>,
        CloudPremError,
    > {
        Err(CloudPremError::Unimplemented)
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
        .enabled_services()
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
    let node_id = ready_node.node_id().to_owned();
    let node_labels = node_labels(&ready_node);
    let client = DeveloperServiceClient::from_channel(
        ready_node.grpc_advertise_addr(),
        ready_node.channel(),
        DeveloperApiServer::MAX_GRPC_MESSAGE_SIZE,
        Some(CompressionEncoding::Zstd),
    );
    let pull_metrics_result = tokio::time::timeout(
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
    let metric_families = metric_families_res.unwrap_or_default();
    NodeMetrics {
        node_id: node_id.to_string(),
        status_code: status_code.as_u16() as u32,
        node_labels,
        metric_families,
    }
}

fn filter_safe_indexes(index_id_patterns: &mut Vec<String>) {
    let safe_pattern_char = |c: char| c.is_ascii_alphanumeric() || "-._*".contains(c);

    index_id_patterns
        .retain(|pattern| pattern.starts_with("otel-") && pattern.chars().all(safe_pattern_char));
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
            .map_err(|e| CloudPremError::Internal(format!("failed to parse hit: {e}")))?;

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
    use super::*;

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
