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

use serde::{Deserialize, Serialize};

use crate::SortFieldMetadata;

/// Ordered physical sort fields resolved from the writer's schema.
/// Merge compatibility requires the same field names, orders, types, and list order.
/// An empty schema represents an unsorted split, including legacy splits.
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(transparent)]
pub struct IndexingSortSchema {
    /// Fields in comparison order. Serialized directly as the `sort_fields` array.
    pub fields: Vec<SortFieldMetadata>,
}

impl IndexingSortSchema {
    /// Returns whether no physical ordering is guaranteed.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}
