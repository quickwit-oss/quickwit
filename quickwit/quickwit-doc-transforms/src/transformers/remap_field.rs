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

/// A step that copies a value from `custom` to a new location, optionally removing the original.
#[derive(Debug)]
pub struct AttributeRemapStep {
    pub sources: Vec<ParsedPath>,
    pub to_path: ParsedPath,
    pub preserve_original: bool,
}

impl PipelineStep for AttributeRemapStep {
    fn apply(&self, value: &mut ProcessedLog) -> Result<(), PipelineError> {
        for from_path in &self.sources {
            // Extract the value at `from_path`
            let from_val_opt = get_nested(&value.custom, from_path.iter()).cloned();
            if let Some(from_val) = from_val_opt {
                set_value_at_path_on_map(&mut value.custom, &self.to_path.segments, from_val);

                if !self.preserve_original {
                    remove_nested_from_map(&mut value.custom, &from_path.segments);
                }
                // TODO: exist on the first match? Is this correct?
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
    fn test_remapstep_move_value_remove_original() {
        // Set up initial log
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());

        // Insert an entry in `log.custom` at key "foo"
        log.custom.insert("foo".to_string(), json!("bar_value"));

        // Create the RemapStep
        let step = AttributeRemapStep {
            sources: vec![ParsedPath {
                segments: vec!["foo".into()],
            }],
            to_path: ParsedPath {
                segments: vec!["baz".into()],
            },
            preserve_original: false,
        };

        // Apply the step
        step.apply(&mut log).unwrap();

        // Verify the value was moved
        assert_eq!(
            log.custom.get("baz"),
            Some(&json!("bar_value")),
            "Expected 'baz' to contain the moved value"
        );
        // Verify the original is removed
        assert!(
            !log.custom.contains_key("foo"),
            "Expected the original 'foo' to be removed"
        );
    }

    #[test]
    fn test_remapstep_move_value_preserve_original() {
        // Set up initial log
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());

        // Insert an entry in `log.custom` at key "alpha"
        log.custom.insert("alpha".to_string(), json!("123"));

        // Create the RemapStep with `preserve_original = true`
        let step = AttributeRemapStep {
            sources: vec![ParsedPath {
                segments: vec!["alpha".into()],
            }],
            to_path: ParsedPath {
                segments: vec!["omega".into()],
            },
            preserve_original: true,
        };

        // Apply the step
        step.apply(&mut log).unwrap();

        // Verify the value was copied
        assert_eq!(
            log.custom.get("omega"),
            Some(&json!("123")),
            "Expected 'omega' to contain the moved value"
        );
        // Verify the original is preserved
        assert_eq!(
            log.custom.get("alpha"),
            Some(&json!("123")),
            "Expected original 'alpha' to remain"
        );
    }
}
