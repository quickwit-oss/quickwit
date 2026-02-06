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

use std::sync::Arc;

use quickwit_config::SearcherConfig;
use quickwit_search::SearcherContext;
use quickwit_storage::StorageResolver;
use tracing::info;

use crate::config::LambdaSearcherConfig;

/// Lambda-specific searcher context that holds resources for search execution.
pub struct LambdaSearcherContext {
    pub searcher_context: Arc<SearcherContext>,
    pub storage_resolver: StorageResolver,
}

impl LambdaSearcherContext {
    /// Create a new Lambda searcher context from environment variables.
    pub fn try_from_env() -> anyhow::Result<Self> {
        info!("initializing Lambda searcher context");

        let config = LambdaSearcherConfig::try_from_env()?;
        let searcher_config = create_searcher_config(&config);
        let searcher_context = Arc::new(SearcherContext::new(searcher_config, None, None));
        let storage_resolver = StorageResolver::configured(&Default::default());

        Ok(Self {
            searcher_context,
            storage_resolver,
        })
    }
}

fn create_searcher_config(config: &LambdaSearcherConfig) -> SearcherConfig {
    let mut searcher_config = SearcherConfig::default();
    searcher_config.max_num_concurrent_split_searches = config.max_concurrent_split_searches;
    searcher_config
}
