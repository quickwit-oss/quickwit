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

use quickwit_config::CacheConfig;
use quickwit_proto::types::SplitId;
use quickwit_storage::{MemorySizedCache, OwnedBytes};

pub struct ListFieldsCache {
    cache: MemorySizedCache<SplitId>,
}

impl ListFieldsCache {
    pub fn new(config: &CacheConfig) -> ListFieldsCache {
        Self {
            cache: MemorySizedCache::from_config(
                config,
                &quickwit_storage::STORAGE_METRICS.partial_request_cache,
            ),
        }
    }

    pub fn get(&self, split_id: &SplitId) -> Option<OwnedBytes> {
        self.cache.get(split_id)
    }

    pub fn put(&self, split_id: SplitId, serialized_split_fields: OwnedBytes) {
        self.cache.put(split_id, serialized_split_fields);
    }
}
