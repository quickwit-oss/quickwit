use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_proto::cloudprem::{
    CloudPremError, CloudPremResult, CloudPremService, FetchOneRequest, FetchOneResponse,
    ListRequest, ListResponse, PingRequest, PingResponse,
};
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

    async fn list(&self, _request: ListRequest) -> CloudPremResult<ListResponse> {
        info!("Received List request");
        Err(CloudPremError::Unimplemented)
    }

    async fn fetch_one(&self, _request: FetchOneRequest) -> CloudPremResult<FetchOneResponse> {
        info!("Received FetchOne request");
        Err(CloudPremError::Unimplemented)
    }
}
