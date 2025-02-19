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

mod index_resource;
mod rest_handler;
mod source_resource;
mod split_resource;

pub use self::index_resource::get_index_metadata_handler;
pub use self::rest_handler::{index_management_handlers, IndexApi};
pub use self::split_resource::{ListSplitsQueryParams, ListSplitsResponse};
