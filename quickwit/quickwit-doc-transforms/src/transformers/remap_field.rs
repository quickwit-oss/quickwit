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

use serde_json::{json, Value};
use vrl::datadog_filter::Matcher;

use crate::error::PipelineError;
use crate::path_access::*;
use crate::pipeline::*;
use crate::ProcessedLog;

/// A step that remaps one nested path to a new location, optionally removing the original.
///
/// Operations are done on the `custom` field of the log.
#[derive(Debug)]
pub struct RemapStep {
    pub filter: Box<dyn Matcher<ProcessedLog>>,
    pub from_path: ParsedPath,
    pub to_path: ParsedPath,
    pub preserve_original: bool,
}

impl PipelineStep for RemapStep {
    fn apply(&self, value: &mut ProcessedLog) -> Result<(), PipelineError> {
        if !self.filter.run(value) {
            return Ok(());
        }
        // Extract the value at `from_path`
        let from_val_opt =
            get_nested_mut(&mut json!(value.custom), &self.from_path.segments).cloned();
        if let Some(from_val) = from_val_opt {
            // Insert it at `to_path`
            let mut custom_json = json!(value.custom);
            set_or_create_nested_mut(&mut custom_json, &self.to_path.segments, from_val);

            if !self.preserve_original {
                remove_nested(&mut custom_json, &self.from_path.segments);
            }
            match custom_json {
                Value::Object(obj) => value.custom = obj,
                _ => unreachable!("custom field should be an object"),
            };
        }

        Ok(())
    }
}
