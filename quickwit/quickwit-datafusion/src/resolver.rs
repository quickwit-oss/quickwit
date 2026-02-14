use std::net::SocketAddr;

use datafusion::error::DataFusionError;
use datafusion_distributed::WorkerResolver;
use quickwit_search::SearcherPool;
use url::Url;

/// A [`WorkerResolver`] backed by Quickwit's [`SearcherPool`].
///
/// The searcher pool is populated from Chitchat cluster membership.
/// Every searcher node runs both the Quickwit gRPC `SearchService`
/// and the Arrow Flight service on the same port. This resolver
/// returns those addresses as Flight URLs for df-distributed.
#[derive(Clone)]
pub struct QuickwitWorkerResolver {
    searcher_pool: SearcherPool,
}

impl QuickwitWorkerResolver {
    pub fn new(searcher_pool: SearcherPool) -> Self {
        Self { searcher_pool }
    }
}

impl WorkerResolver for QuickwitWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        let addrs: Vec<SocketAddr> = self.searcher_pool.keys();
        if addrs.is_empty() {
            return Err(DataFusionError::Execution(
                "no searcher nodes available in the cluster".to_string(),
            ));
        }
        addrs
            .into_iter()
            .map(|addr| {
                Url::parse(&format!("http://{addr}")).map_err(|e| {
                    DataFusionError::Internal(format!("bad worker url: {e}"))
                })
            })
            .collect()
    }
}
