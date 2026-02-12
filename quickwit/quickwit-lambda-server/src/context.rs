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

use anyhow::Context as _;
use bytesize::ByteSize;
use quickwit_config::{CacheConfig, SearcherConfig};
use quickwit_search::SearcherContext;
use quickwit_storage::StorageResolver;
use tracing::info;

/// Lambda-specific searcher context that holds resources for search execution.
pub struct LambdaSearcherContext {
    pub searcher_context: Arc<SearcherContext>,
    pub storage_resolver: StorageResolver,
}

impl LambdaSearcherContext {
    /// Create a new Lambda searcher context from environment variables.
    pub fn try_from_env() -> anyhow::Result<Self> {
        info!("initializing lambda searcher context");

        let searcher_config = try_searcher_config_from_env()?;
        let searcher_context = Arc::new(SearcherContext::new(searcher_config, None, None));
        let storage_resolver = StorageResolver::configured(&Default::default());

        Ok(Self {
            searcher_context,
            storage_resolver,
        })
    }
}

/// Create a Lambda-optimized searcher config based on the `AWS_LAMBDA_FUNCTION_MEMORY_SIZE`
/// environment variable.
fn try_searcher_config_from_env() -> anyhow::Result<SearcherConfig> {
    let lambda_memory_mib: u64 = quickwit_common::get_from_env_opt(
        "AWS_LAMBDA_FUNCTION_MEMORY_SIZE",
        /* sensitive */ false,
    )
    .context("could not get aws lambda function memory size from ENV")?;
    let lambda_memory = ByteSize::mib(lambda_memory_mib);
    anyhow::ensure!(
        lambda_memory >= ByteSize::gib(1u64),
        "lambda memory must be at least 1GB"
    );
    let warmup_memory_budget = ByteSize::b(lambda_memory.as_u64() - ByteSize::mib(500).as_u64());

    let mut searcher_config = SearcherConfig::default();
    searcher_config.max_num_concurrent_split_searches = 20;
    searcher_config.warmup_memory_budget = warmup_memory_budget;
    searcher_config.fast_field_cache = CacheConfig::no_cache();
    searcher_config.split_footer_cache = CacheConfig::no_cache();
    searcher_config.predicate_cache = CacheConfig::no_cache();
    searcher_config.partial_request_cache = CacheConfig::no_cache();
    Ok(searcher_config)
}
