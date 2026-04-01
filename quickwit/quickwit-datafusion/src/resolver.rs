// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Generic worker resolver — maps `SearcherPool` → Flight URLs.
//!
//! No data-source-specific code here.

use std::net::SocketAddr;

use datafusion::error::DataFusionError;
use datafusion_distributed::WorkerResolver;
use quickwit_search::SearcherPool;
use url::Url;

/// Resolves worker Flight URLs from the cluster's searcher pool.
///
/// Every searcher node runs both the Quickwit gRPC `SearchService` and the
/// Arrow Flight service on the same port.
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
                Url::parse(&format!("http://{addr}"))
                    .map_err(|e| DataFusionError::Internal(format!("bad worker url: {e}")))
            })
            .collect()
    }
}
