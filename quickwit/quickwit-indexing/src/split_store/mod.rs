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

mod indexing_split_cache;
mod indexing_split_store;
mod split_store_quota;

pub use indexing_split_cache::{IndexingSplitCache, get_tantivy_directory_from_split_bundle};
pub use indexing_split_store::IndexingSplitStore;
pub use split_store_quota::SplitStoreQuota;
