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

use crate::ProcessedLog;
use crate::error::PipelineError;
use crate::path_access::*;
use crate::pipeline::*;

///
/// Copies a string value from `custom` to a core attr.
#[derive(Debug)]
pub struct CoreStringAttrRemapStep {
    pub sources: Vec<ParsedPath>,
    pub core_attr: CoreStringAttr,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum CoreStringAttr {
    Message,
    Service,
    Source,
    TraceId,
    SpanId,
    // Only used in preprocessing
    Host,
}

impl PipelineStep for CoreStringAttrRemapStep {
    fn apply(&self, value: &mut ProcessedLog) -> Result<(), PipelineError> {
        for from_path in &self.sources {
            // Extract the value at `from_path`
            let from_val_opt = get_nested(&value.custom, &from_path.original).cloned();
            if let Some(from_val) = from_val_opt {
                // We only support string values for now
                if let Some(from_val) = from_val.as_str() {
                    match self.core_attr {
                        CoreStringAttr::Message => {
                            value.message = from_val.to_string();
                        }
                        CoreStringAttr::Service => {
                            value.service = from_val.to_string().into();
                        }
                        CoreStringAttr::TraceId => {
                            value.trace_id = Some(from_val.to_string());
                        }
                        CoreStringAttr::SpanId => {
                            value.span_id = Some(from_val.to_string());
                        }
                        CoreStringAttr::Host => {
                            value.host = from_val.to_string().into();
                        }
                        CoreStringAttr::Source => {
                            value.source = from_val.to_string().into();
                        }
                    }
                    // Message attributes delete the source key
                    if self.core_attr == CoreStringAttr::Message {
                        remove_nested_from_map(&mut value.custom, from_path.segments.as_ref());
                    }
                    break;
                }
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
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());

        // Insert an entry in `log.custom` at key "foo"
        log.custom.insert("foo".to_string(), json!("bar_value"));

        // Create the RemapStep
        let step = CoreStringAttrRemapStep {
            sources: vec!["foo".into()],
            core_attr: CoreStringAttr::Message,
        };

        // Apply the step
        step.apply(&mut log).unwrap();

        // Check the result
        assert_eq!(log.message, "bar_value");
    }

    #[test]
    fn test_core_string_attr_remap_step_multiple_sources() {
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());

        // Insert entries in `log.custom`
        log.custom.insert("asdf".to_string(), json!("bar_value"));
        log.custom.insert("baz".to_string(), json!("baz_value"));

        // Create the RemapStep with multiple sources
        let step = CoreStringAttrRemapStep {
            sources: vec!["foo".into(), "baz".into()],
            core_attr: CoreStringAttr::Service,
        };

        step.apply(&mut log).unwrap();
        assert_eq!(log.service.unwrap(), "baz_value");
    }
    #[test]
    fn test_core_string_attr_remap_step_no_match() {
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());

        // Create the RemapStep with a source that doesn't exist
        let step = CoreStringAttrRemapStep {
            sources: vec!["non_existent_key".into()],
            core_attr: CoreStringAttr::Message,
        };

        step.apply(&mut log).unwrap();
        assert_eq!(log.message, "Test log message");
    }

    #[test]
    fn test_core_string_attr_remap_step_message_deletion() {
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());

        log.custom.insert("foo".to_string(), json!("bar_value"));

        let step = CoreStringAttrRemapStep {
            sources: vec!["foo".into()],
            core_attr: CoreStringAttr::Message,
        };

        step.apply(&mut log).unwrap();

        assert!(!log.custom.contains_key("foo"));
        assert_eq!(log.message, "bar_value");
    }
    #[test]
    fn test_core_string_attr_remap_step_message_deletion_nested() {
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());

        // Insert a nested entry in `log.custom`
        log.custom
            .insert("nested".to_string(), json!({"foo": "bar_value"}));

        let step = CoreStringAttrRemapStep {
            sources: vec!["nested.foo".into()],
            core_attr: CoreStringAttr::Message,
        };

        step.apply(&mut log).unwrap();

        assert!(
            !log.custom
                .get("nested")
                .expect("expect nested")
                .as_object()
                .unwrap()
                .contains_key("foo")
        );
        assert_eq!(log.message, "bar_value");
    }
    #[test]
    fn test_core_string_attr_remap_step_message_no_deletion_json() {
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());

        // Insert a json in a msg remap candidate
        log.custom.insert(
            "nested".to_string(),
            json!({"foo": json!({"foo": "bar_value"})}),
        );

        let step = CoreStringAttrRemapStep {
            sources: vec!["nested.foo".into()],
            core_attr: CoreStringAttr::Message,
        };

        step.apply(&mut log).unwrap();

        // We don't remap the message because the value is not a string
        assert!(
            log.custom
                .get("nested")
                .expect("expect nested")
                .as_object()
                .unwrap()
                .contains_key("foo")
        );
        assert_eq!(log.message, "Test log message");
    }
}
