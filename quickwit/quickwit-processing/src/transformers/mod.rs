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

mod attribute_remap;
mod category_processor;
mod core_string_attr_remapper;
mod date_remapper;
pub mod grok;
mod integrations_processor;
mod status_remapper;
mod string_builder;
mod syslog_processor;
mod user_agent_parser;

pub use attribute_remap::*;
pub use category_processor::*;
pub use core_string_attr_remapper::*;
pub use date_remapper::*;
pub use grok::*;
pub use integrations_processor::*;
pub use status_remapper::*;
pub use string_builder::*;
pub use syslog_processor::*;
pub use user_agent_parser::*;
use vrl::datadog_filter::Matcher;

use crate::error::PipelineError;
use crate::{PipelineStep, ProcessedLog};

/// A helper struct that wraps a pipeline step with a filter.
#[derive(Debug)]
pub struct FilteredStep<T: PipelineStep> {
    pub filter: Box<dyn Matcher<ProcessedLog>>,
    pub step: T,
}
impl<T: PipelineStep> FilteredStep<T> {
    pub fn new(filter: Box<dyn Matcher<ProcessedLog>>, step: T) -> Self {
        Self { filter, step }
    }
}

impl<T: PipelineStep> PipelineStep for FilteredStep<T> {
    fn apply(&self, value: &mut ProcessedLog) -> Result<(), PipelineError> {
        if !self.filter.run(value) {
            return Ok(());
        }
        self.step.apply(value)
    }
}
