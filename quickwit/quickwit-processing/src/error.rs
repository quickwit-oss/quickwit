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

use thiserror::Error;
use vrl::datadog_grok::parse_grok_rules::Error as GrokError;

#[derive(Error, Debug)]
pub enum PipelineError {
    #[error("failed to compile Grok pattern: {source}")]
    GrokCompile {
        #[from]
        source: GrokError,
    },

    #[error("failed to run Grok pattern: {message}")]
    GrokParse { message: String },

    #[error("failed to parse path: {source}")]
    PathParseError {
        #[from]
        source: vrl::path::PathParseError,
    },

    #[error("unsupported pipeline type: {typ}")]
    UnsupportedType { typ: String },

    #[error("other pipeline error: {error}")]
    Other { error: String },

    #[error("failed to parse query: {message}")]
    QueryParse { message: String },
}
