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

mod grpc_adapter;
mod rest_handler;

pub use self::grpc_adapter::GrpcSearchAdapter;
pub use self::rest_handler::{
    SearchApi, SearchRequestQueryString, SortBy, search_get_handler, search_plan_get_handler,
    search_plan_post_handler, search_post_handler, search_request_from_api_request,
};
pub(crate) use self::rest_handler::{extract_index_id_patterns, extract_index_id_patterns_default};
