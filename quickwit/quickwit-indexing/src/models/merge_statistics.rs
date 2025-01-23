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

use std::sync::atomic::Ordering;

use serde::Serialize;

use crate::actors::{PublisherCounters, UploaderCounters};

/// A Struct to hold all merge statistical data.
#[derive(Clone, Debug, Default, Serialize)]
pub struct MergeStatistics {
    /// Number of uploaded splits
    pub num_uploaded_splits: u64,
    /// Number of published splits
    pub num_published_splits: u64,
    /// Pipeline generation.
    pub generation: usize,
    /// Number of successive pipeline spawn attempts.
    pub num_spawn_attempts: usize,
    /// Number of merges currently in progress.
    pub num_ongoing_merges: usize,
}

impl MergeStatistics {
    pub fn add_actor_counters(
        mut self,
        uploader_counters: &UploaderCounters,
        publisher_counters: &PublisherCounters,
    ) -> Self {
        self.num_uploaded_splits += uploader_counters.num_uploaded_splits.load(Ordering::SeqCst);
        self.num_published_splits += publisher_counters.num_published_splits;
        self
    }

    pub fn set_num_spawn_attempts(mut self, num_spawn_attempts: usize) -> Self {
        self.num_spawn_attempts = num_spawn_attempts;
        self
    }

    pub fn set_generation(mut self, generation: usize) -> Self {
        self.generation = generation;
        self
    }

    pub fn set_ongoing_merges(mut self, n: usize) -> Self {
        self.num_ongoing_merges = n;
        self
    }
}
