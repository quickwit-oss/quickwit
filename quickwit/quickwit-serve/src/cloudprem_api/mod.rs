use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_proto::cloudprem::{
    CloudPremError, CloudPremResult, CloudPremService, FetchOneRequest, FetchOneResponse,
    ListRequest, ListResponse, PingRequest, PingResponse,
};
use quickwit_proto::search::{CountHits, SearchRequest, SortField, SortOrder};
use quickwit_search::SearchService;
use tracing::info;

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
        info!("Received Ping request");
        Ok(PingResponse {})
    }

    async fn list(&self, request: ListRequest) -> CloudPremResult<ListResponse> {
        info!("Received List request");

        let Some(query) = request.query else {
            return Err(CloudPremError::Internal("missing query".to_string()));
        };
        let query_evp_ast = quickwit_query::cloudprem::parse_query(query)
            .map_err(|err| CloudPremError::InvalidQuery(format!("failed to parse query: {err}")))?;
        let query_ast = quickwit_query::cloudprem::to_quickwit_query(query_evp_ast)?;

        let count_hits = if request.should_compute_count {
            CountHits::CountAll
        } else {
            CountHits::Underestimate
        };
        let request = SearchRequest {
            index_id_patterns: vec!["cloudprem".to_string()], /* TODO this should become
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

        let response = self.search_service.root_search(request).await?;

        Ok(ListResponse {
            count: response.num_hits,
            streams: response
                .hits
                .into_iter()
                .map(|_| quickwit_proto::cloudprem::Stream {})
                .collect(),
            statistics: None,
        })
    }

    async fn fetch_one(&self, _request: FetchOneRequest) -> CloudPremResult<FetchOneResponse> {
        info!("Received FetchOne request");
        Err(CloudPremError::Unimplemented)
    }
}
