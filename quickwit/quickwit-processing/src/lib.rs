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

mod date_time_parsing;
mod default_field_search;
mod error;
mod filter;
mod flatten_tags;
mod path_access;
mod percolate;
mod pipeline;
mod processed_log;
mod string_or_vec;
pub mod transformers;

pub use error::PipelineError;
pub use flatten_tags::convert_tags;
pub use pipeline::{Pipeline, PipelineConfig, PipelineStep, PipelineStepConfig, build_step};
pub use processed_log::{
    DatadogLogMsg, ExtraFts, MessageValue, ProcessedLog, get_preprocessing_pipeline,
};
pub use transformers::get_integrations_processor;

pub type Result<T> = std::result::Result<T, error::PipelineError>;
