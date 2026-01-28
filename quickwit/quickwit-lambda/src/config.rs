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

use bytesize::ByteSize;
// Re-export LambdaConfig from quickwit-config
pub use quickwit_config::LambdaConfig;

/// Configuration for the Lambda handler's SearcherContext.
/// These settings are optimized for Lambda's memory constraints.
#[derive(Clone, Debug)]
pub struct LambdaSearcherConfig {
    /// Memory allocated to the Lambda function in MB.
    pub memory_mb: usize,

    /// Fast field cache capacity (derived from memory_mb).
    pub fast_field_cache_capacity: ByteSize,

    /// Split footer cache capacity.
    pub split_footer_cache_capacity: ByteSize,

    /// Maximum concurrent split searches within a single Lambda invocation.
    pub max_concurrent_split_searches: usize,

    /// Warmup memory budget.
    pub warmup_memory_budget: ByteSize,
}

impl LambdaSearcherConfig {
    /// Create a Lambda-optimized searcher config based on the allocated memory.
    pub fn for_memory(memory_mb: usize) -> Self {
        // Allocate roughly 1/4 of memory to fast field cache
        let fast_field_cache_capacity = ByteSize::mb((memory_mb / 4) as u64);
        // Fixed reasonable sizes for other caches
        let split_footer_cache_capacity = ByteSize::mb(50);
        // Warmup budget is about half of memory
        let warmup_memory_budget = ByteSize::mb((memory_mb / 2) as u64);

        Self {
            memory_mb,
            fast_field_cache_capacity,
            split_footer_cache_capacity,
            max_concurrent_split_searches: 20,
            warmup_memory_budget,
        }
    }
}

impl Default for LambdaSearcherConfig {
    fn default() -> Self {
        // Default to 1024 MB Lambda
        Self::for_memory(1024)
    }
}
