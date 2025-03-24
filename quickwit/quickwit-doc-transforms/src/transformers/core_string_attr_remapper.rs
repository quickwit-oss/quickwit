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

use crate::error::PipelineError;
use crate::path_access::*;
use crate::pipeline::*;
use crate::ProcessedLog;

///
/// Copies a string value from `custom` to a core attr.
#[derive(Debug)]
pub struct CoreStringAttrRemapStep {
    pub sources: Vec<ParsedPath>,
    pub core_attr: CoreStringAttr,
}

#[derive(Debug)]
pub enum CoreStringAttr {
    Message,
    Service,
    TraceId,
    SpanId,
}

impl PipelineStep for CoreStringAttrRemapStep {
    fn apply(&self, value: &mut ProcessedLog) -> Result<(), PipelineError> {
        for from_path in &self.sources {
            // Extract the value at `from_path`
            let from_val_opt =
                get_nested(&value.custom, from_path.segments.iter().map(AsRef::as_ref)).cloned();
            if let Some(from_val) = from_val_opt {
                // We only support string values for now
                if let Some(from_val) = from_val.as_str() {
                    match self.core_attr {
                        CoreStringAttr::Message => {
                            value.message = from_val.to_string();
                        }
                        CoreStringAttr::Service => {
                            value.service = from_val.to_string();
                        }
                        CoreStringAttr::TraceId => {
                            value.trace_id = Some(from_val.to_string());
                        }
                        CoreStringAttr::SpanId => {
                            value.span_id = Some(from_val.to_string());
                        }
                    }
                }
                break;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::processed_log::tests::make_datadog_log_msg;

    #[test]
    fn test_core_string_attr_remap_step() {
        // Set up initial log
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());

        // Insert an entry in `log.custom` at key "foo"
        log.custom.insert("foo".to_string(), json!("bar_value"));

        // Create the RemapStep
        let step = CoreStringAttrRemapStep {
            sources: vec![ParsedPath {
                segments: vec!["foo".into()],
            }],
            core_attr: CoreStringAttr::Message,
        };

        // Apply the step
        step.apply(&mut log).unwrap();

        // Check the result
        assert_eq!(log.message, "bar_value");
    }
}
