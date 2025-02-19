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

mod response;
mod rest_handler;

pub use response::{RestIngestResponse, RestParseFailure};
#[cfg(test)]
pub(crate) use rest_handler::tests::setup_ingest_v1_service;
pub(crate) use rest_handler::{ingest_api_handlers, lines};
pub use rest_handler::{IngestApi, IngestApiSchemas};
