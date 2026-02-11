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

pub mod index_router;
mod log_msg_accessors;
pub mod rest_handler;

pub use index_router::IndexRouter;
#[cfg(any(test, feature = "testsuite"))]
pub use log_msg_accessors::{custom_field_accessor, tag_accessor};
pub use rest_handler::DatadogApi;
pub(crate) use rest_handler::datadog_api_handlers;
