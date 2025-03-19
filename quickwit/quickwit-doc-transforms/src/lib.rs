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

#![allow(clippy::bool_assert_comparison)]
#![deny(clippy::disallowed_methods)]

use serde::{Deserialize, Serialize};

mod default_field_search;
mod error;
mod filter;
mod flatten_tags;
mod normalize_field;
mod path_access;
mod pipeline;
mod processed_log;
mod transformers;

pub use flatten_tags::convert_tags;
pub use pipeline::{build_step, Pipeline, PipelineStep, PipelineStepConfig};
pub use processed_log::{DatadogLogMsg, ProcessedLog};

pub type Result<T> = std::result::Result<T, error::PipelineError>;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum StringOrVec {
    String(String),
    Vec(Vec<String>),
}

impl StringOrVec {
    pub fn contains(&self, value: &str) -> bool {
        match self {
            StringOrVec::String(val) => val == value,
            StringOrVec::Vec(vec) => vec.iter().any(|elem| *elem == value),
        }
    }
}
