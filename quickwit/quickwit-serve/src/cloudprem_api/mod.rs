use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_proto::cloudprem::{
    AggregationRequest, AggregationResponse, CloudPremError, CloudPremResult, CloudPremService,
    Event, EventTracker, FetchOneRequest, FetchOneResponse, ListRequest, ListResponse, PingRequest,
    PingResponse, SetClusterAddressRequest, SetClusterAddressResponse, Statistics,
};
use quickwit_proto::search::{
    CountHits, Hit, PartialHit, SearchRequest, SearchResponse, SortField, SortOrder,
};
use quickwit_query::query_ast::{FullTextMode, FullTextParams, FullTextQuery, QueryAst};
use quickwit_query::MatchAllOrNone;
use quickwit_search::SearchService;
use serde_json::Value as JsonValue;
use tracing::{debug, error, info, warn};

// TODO this should become configurable and sent by EVP
const CLOUD_PREM_INDEX_ID_PATTERN: &str = "datadog*";

#[allow(dead_code)]
pub struct CloudPremServiceImpl {
    search_service: Arc<dyn SearchService>,
}

impl fmt::Debug for CloudPremServiceImpl {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CloudPremServiceImpl")
    }
}

impl From<Arc<dyn SearchService>> for CloudPremServiceImpl {
    fn from(search_service: Arc<dyn SearchService>) -> Self {
        CloudPremServiceImpl { search_service }
    }
}

fn query_ast_to_search_doc_id(doc_id: &str) -> QueryAst {
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
            index_id_patterns: vec![CLOUD_PREM_INDEX_ID_PATTERN.to_string()],
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
        let query_ast = query_ast_to_search_doc_id(&event_tracker.id);
        let query_ast_json = serde_json::to_string(&query_ast)
            .map_err(|e| CloudPremError::Internal(e.to_string()))?;

        debug!(query=%query_ast_json, "query ast for fetch one");

        // TODO optimize fetch one by leveraging the information in the event tracker
        // (last seen split_id, etc.)
        let search_request = SearchRequest {
            index_id_patterns: vec![CLOUD_PREM_INDEX_ID_PATTERN.to_string()],
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
        };

        let search_response: SearchResponse =
            self.search_service.root_search(search_request).await?;

        if search_response.hits.is_empty() {
            warn!("document not found on fetch one");
            return Err(CloudPremError::DocumentNotFound {
                id: event_tracker.id.clone(),
                split_id: event_tracker.fragment_id.clone(),
                doc_id: event_tracker.row_number,
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
        let aggregation_ast =
            quickwit_query::cloudprem::to_tantivy_aggregation(evp_aggregation_ast, start_ts_secs)
                .inspect_err(|e| warn!("failed to aggregation map ast: {e}"))?;
        debug!("converted aggregation ast {aggregation_ast:?}");

        let search_request = SearchRequest {
            index_id_patterns: vec![CLOUD_PREM_INDEX_ID_PATTERN.to_string()],
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
            count_hits: CountHits::Underestimate.into(),
        };

        let response = self
            .search_service
            .root_search(search_request)
            .await
            .inspect_err(|e| warn!("list root search failed: {e}"))?;

        tracing::trace!("request result: {response:?}");
        let aggregation_result = quickwit_query::cloudprem::aggregation_result_to_proto(
            &response.aggregation.ok_or_else(|| {
                CloudPremError::Internal("request generated no aggregation result".to_string())
            })?,
        )?;
        tracing::trace!("aggregation result: {aggregation_result:?}");

        let statistics = Statistics {
            hit_count: response.num_hits,
            scanned_count: 0,
            result_memory_size: 0u64,
            max_result_memory_size: 0u64,
        };

        Ok(AggregationResponse {
            result: aggregation_result,
            statistics: Some(statistics),
        })
    }

    async fn set_cluster_address(
        &self,
        _: SetClusterAddressRequest,
    ) -> Result<SetClusterAddressResponse, CloudPremError> {
        Err(CloudPremError::Unimplemented)
    }
}

struct HitMapper {
    id_field: &'static str,
    ts_field: &'static str,
}

const DEFAULT_HIT_MAPPER: HitMapper = HitMapper {
    id_field: "id",
    ts_field: "timestamp",
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
            .map(|ts| ts.into_timestamp_nanos())
            .unwrap_or(0) as u64
        } else {
            0
        };

        Ok(Event {
            tracker: Some(EventTracker {
                id: event_id,
                epoch_ms: timestamp / 1_000_000,
                tiebreaker: (timestamp % 1_000_000) as i32,
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
        PartialHit {
            // internally we always want ns
            sort_value: make_uint_value(event.epoch_ms * 1_000_000 + event.tiebreaker as u64),
            sort_value2: None,
            split_id: event.fragment_id.unwrap_or_default(),
            segment_ord: 0,
            doc_id: event.row_number.unwrap_or_default() as u32,
        }
    }
}
