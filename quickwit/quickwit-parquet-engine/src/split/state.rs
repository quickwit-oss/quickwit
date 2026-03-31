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

//! Shared split state enum used by both metrics and sketch splits.

use serde::{Deserialize, Serialize};

/// State of a split in the metastore.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SplitState {
    /// Split is staged (being written, not yet queryable).
    Staged,
    /// Split is published (queryable).
    Published,
    /// Split is marked for deletion.
    MarkedForDeletion,
}

impl SplitState {
    /// Returns a string representation for database storage.
    pub fn as_str(&self) -> &'static str {
        match self {
            SplitState::Staged => "Staged",
            SplitState::Published => "Published",
            SplitState::MarkedForDeletion => "MarkedForDeletion",
        }
    }
}

impl std::fmt::Display for SplitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
