use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_proto::cloudprem::{
    CloudPremError, CloudPremResult, CloudPremService, Event, EventTracker, FetchOneRequest,
    FetchOneResponse, ListRequest, ListResponse, PingRequest, PingResponse,
};
use quickwit_proto::search::{CountHits, Hit, SearchRequest, SortField, SortOrder};
use quickwit_search::SearchService;
use serde_json::Value as JsonValue;
use tracing::{debug, info};

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
        let search_request = SearchRequest {
            index_id_patterns: vec!["datadog-op-*".to_string()], /* TODO this should become
                                                                  * configurable and sent by EVP */
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
            search_after: None,
            count_hits: count_hits.into(),
        };

        let response = self.search_service.root_search(search_request).await?;

        let hit_mapper = HitMapper {
            id_field: "id".to_string(),
            ts_field: "timestamp".to_string(),
        };
        let events = response
            .hits
            .into_iter()
            .map(|hit| hit_mapper.hit_to_event(hit))
            .collect::<Result<_, _>>()?;

        Ok(ListResponse {
            count: response.num_hits,
            streams: vec![quickwit_proto::cloudprem::Stream { events }],
            statistics: None,
        })
    }

    async fn fetch_one(&self, _request: FetchOneRequest) -> CloudPremResult<FetchOneResponse> {
        info!("received FetchOne request");
        Err(CloudPremError::Unimplemented)
    }
}

struct HitMapper {
    id_field: String,
    ts_field: String,
}

impl HitMapper {
    fn hit_to_event(&self, hit: Hit) -> CloudPremResult<Event> {
        // TODO use serde_json_borrowed ?
        let map: serde_json::Map<String, JsonValue> = serde_json::from_str(&hit.json)
            .map_err(|e| CloudPremError::Internal(format!("failed to parse hit: {e}")))?;

        let event_id = if let Some(id) = map.get(&self.id_field) {
            id.to_string()
        } else {
            "missing_id".to_string()
        };

        let timestamp = if let Some(JsonValue::String(ts)) = map.get(&self.ts_field) {
            quickwit_datetime::parse_date_time_str(
                ts,
                &[quickwit_datetime::DateTimeInputFormat::Rfc3339],
            )
            .map(|ts| ts.into_timestamp_millis())
            .unwrap_or(0)
        } else {
            0
        };

        Ok(Event {
            tracker: Some(EventTracker {
                id: event_id,
                epoch_ms: timestamp as u64,
                tiebreaker: 0, /* TODO get from event? or if we record ingest time with ns, use
                                * sub ms precision? */
                row_number: hit
                    .partial_hit
                    .as_ref()
                    .map(|partial_hit| partial_hit.doc_id as i64),
                fragment_id: hit.partial_hit.map(|partial_hit| partial_hit.split_id),
            }),
            content_json: hit.json,
        })
    }
}
