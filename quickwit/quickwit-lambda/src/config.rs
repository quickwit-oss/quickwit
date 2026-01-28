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

use anyhow::Context as _;
use bytesize::ByteSize;

/// Configuration for the Lambda handler's SearcherContext.
/// These settings are optimized for Lambda's memory constraints.
#[derive(Clone, Debug)]
pub struct LambdaSearcherConfig {
    /// Maximum concurrent split searches within a single Lambda invocation.
    pub max_concurrent_split_searches: usize,
    /// Warmup memory budget.
    pub warmup_memory_budget: ByteSize,
}

impl LambdaSearcherConfig {
    pub fn try_from_env() -> anyhow::Result<LambdaSearcherConfig> {
        let lambda_memory_mib: u64 =
            quickwit_common::get_from_env_opt("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", false)
                .context("could not get aws lambda function memory size from ENV")?;
        let lambda_memory = ByteSize::mib(lambda_memory_mib);
        Self::for_memory(lambda_memory)
    }
    /// Create a Lambda-optimized searcher config based on the allocated memory.
    pub fn for_memory(lambda_memory: ByteSize) -> anyhow::Result<Self> {
        // Warmup budget is about half of memory
        anyhow::ensure!(
            lambda_memory >= ByteSize::gib(1u64),
            "lambda memory must be at least 500MB"
        );
        let warmup_memory_budget =
            ByteSize::b(lambda_memory.as_u64() - ByteSize::mib(500).as_u64());
        Ok(Self {
            max_concurrent_split_searches: 20,
            warmup_memory_budget,
        })
    }
}
