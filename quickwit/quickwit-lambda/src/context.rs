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

use std::sync::{Arc, OnceLock};

use quickwit_config::SearcherConfig;
use quickwit_search::SearcherContext;
use quickwit_storage::StorageResolver;
use tracing::info;

use crate::config::LambdaSearcherConfig;
use crate::error::LambdaResult;

/// Lambda-specific searcher context that caches resources across warm invocations.
pub struct LambdaSearcherContext {
    pub searcher_context: Arc<SearcherContext>,
    pub storage_resolver: StorageResolver,
}

impl LambdaSearcherContext {
    pub async fn new(config: LambdaSearcherConfig) -> LambdaResult<Self> {
        info!(
            memory_mb = config.memory_mb,
            "Initializing Lambda searcher context"
        );

        let searcher_config = create_searcher_config(&config);
        let searcher_context = Arc::new(SearcherContext::new(searcher_config, None));
        let storage_resolver = StorageResolver::configured(&Default::default());

        Ok(Self {
            searcher_context,
            storage_resolver,
        })
    }

    pub async fn get_or_init() -> &'static Self {
        static CONTEXT: OnceLock<LambdaSearcherContext> = OnceLock::new();

        if let Some(ctx) = CONTEXT.get() {
            return ctx;
        }

        let config = config_from_env();
        let ctx = Self::new(config)
            .await
            .expect("Failed to initialize Lambda searcher context");

        let _ = CONTEXT.set(ctx);
        CONTEXT.get().unwrap()
    }
}

fn create_searcher_config(config: &LambdaSearcherConfig) -> SearcherConfig {
    let mut searcher_config = SearcherConfig::default();
    searcher_config.max_num_concurrent_split_searches = config.max_concurrent_split_searches;
    searcher_config
}

fn config_from_env() -> LambdaSearcherConfig {
    let memory_mb = std::env::var("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1024);

    LambdaSearcherConfig::for_memory(memory_mb)
}
