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

/// A step that copies a value from `custom` to a new location, optionally removing the original.
#[derive(Debug)]
pub struct AttributeRemapStep {
    pub sources: Vec<ParsedPath>,
    pub to_path: ParsedPath,
    pub preserve_original: bool,
    pub source_type: AttrRemapperTargetType,
    pub target_type: AttrRemapperTargetType,
    pub override_if_exists: bool,
    pub target_format: AttrRemapperTargetFormat,
}

impl PipelineStep for AttributeRemapStep {
    fn apply(&self, log: &mut ProcessedLog) -> Result<(), PipelineError> {
        // Check if the target path already exists and early exit if not overriding
        if !self.override_if_exists {
            match self.target_type {
                AttrRemapperTargetType::Tag => {
                    if log.tag.contains_key(&self.to_path.original) {
                        return Ok(());
                    }
                }
                AttrRemapperTargetType::Attribute => {
                    if get_nested(&log.custom, &self.to_path.original).is_some() {
                        return Ok(());
                    }
                }
            }
        }
        if let Some(value) = self.get(log) {
            self.set(log, value);
        }
        Ok(())
    }
}

impl AttributeRemapStep {
    fn set(&self, log: &mut ProcessedLog, value: serde_json::Value) {
        match self.target_type {
            AttrRemapperTargetType::Tag => {
                // TODO: Should we have a fallback here, if we can't convert the value to a string
                // or vec
                if let Ok(val) = value.try_into() {
                    log.tag.insert(self.to_path.original.clone(), val);
                }
            }
            AttrRemapperTargetType::Attribute => {
                let value = self.try_convert_to_target_type(value);
                set_value_at_path_on_map(&mut log.custom, &self.to_path.segments, value);
            }
        }
    }

    /// Try to cast the value to a the target type. If the cast is not possible, the original value
    /// is kept
    fn try_convert_to_target_type(&self, value: serde_json::Value) -> serde_json::Value {
        match self.target_format {
            AttrRemapperTargetFormat::Auto => value,
            AttrRemapperTargetFormat::Str => value,
            AttrRemapperTargetFormat::Integer => {
                if let Some(val) = value.as_str() {
                    match val.parse::<i64>() {
                        Ok(v) => serde_json::Value::Number(serde_json::Number::from(v)),
                        Err(_) => value,
                    }
                } else {
                    value
                }
            }
            AttrRemapperTargetFormat::Double => {
                if let Some(val) = value.as_str() {
                    match val.parse::<f64>() {
                        Ok(v) => {
                            serde_json::Value::Number(serde_json::Number::from_f64(v).unwrap())
                        }
                        Err(_) => value,
                    }
                } else {
                    value
                }
            }
        }
    }

    fn get(&self, log: &mut ProcessedLog) -> Option<serde_json::Value> {
        match self.source_type {
            AttrRemapperTargetType::Tag => {
                for from_path in &self.sources {
                    if let Some(from_val) = log.tag.get(&from_path.original) {
                        let val = from_val.clone().into();
                        if !self.preserve_original {
                            log.tag.remove(&from_path.original);
                        }
                        return Some(val);
                    }
                }
            }
            AttrRemapperTargetType::Attribute => {
                for from_path in &self.sources {
                    // Extract the value at `from_path`
                    let from_val_opt = get_nested(&log.custom, &from_path.original).cloned();
                    if let Some(from_val) = from_val_opt {
                        if !self.preserve_original {
                            remove_nested_from_map(&mut log.custom, &from_path.segments);
                        }
                        return Some(from_val);
                    }
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::processed_log::tests::make_datadog_log_msg;
    use crate::string_or_vec::StringOrVec;

    #[test]
    fn test_remapstep_move_value_remove_original() {
        // Set up initial log
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());

        // Insert an entry in `log.custom` at key "foo"
        log.custom.insert("foo".to_string(), json!("bar_value"));

        // Create the RemapStep
        let step = AttributeRemapStep {
            sources: vec!["foo".into()],
            to_path: "baz".into(),
            preserve_original: false,
            source_type: AttrRemapperTargetType::Attribute,
            target_type: AttrRemapperTargetType::Attribute,
            override_if_exists: false,
            target_format: AttrRemapperTargetFormat::Auto,
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
            sources: vec!["alpha".into()],
            to_path: "omega".into(),
            preserve_original: true,
            source_type: AttrRemapperTargetType::Attribute,
            target_type: AttrRemapperTargetType::Attribute,
            override_if_exists: false,
            target_format: AttrRemapperTargetFormat::Auto,
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

    /// Helper to insert into log.tag
    fn insert_tag(log: &mut ProcessedLog, key: &str, value: &str) {
        log.tag
            .insert(key.to_string(), StringOrVec::String(value.to_string()));
    }

    #[test]
    fn test_remap_tag_to_attribute_remove_original() {
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        insert_tag(&mut log, "foo_tag", "tag_value");

        let step = AttributeRemapStep {
            sources: vec!["foo_tag".into()],
            to_path: "dest_attr".into(),
            preserve_original: false,
            source_type: AttrRemapperTargetType::Tag,
            target_type: AttrRemapperTargetType::Attribute,
            override_if_exists: false,
            target_format: AttrRemapperTargetFormat::Auto,
        };

        step.apply(&mut log).unwrap();

        // Should have moved into custom["dest_attr"]
        assert_eq!(
            get_nested(&log.custom, "dest_attr").cloned(),
            Some(json!("tag_value"))
        );
        // Original tag should be removed
        assert!(!log.tag.contains_key("foo_tag"));
    }

    #[test]
    fn test_remap_tag_to_attribute_preserve_original() {
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        insert_tag(&mut log, "foo_tag", "tag_value");

        let step = AttributeRemapStep {
            sources: vec!["foo_tag".into()],
            to_path: "dest_attr".into(),
            preserve_original: true,
            source_type: AttrRemapperTargetType::Tag,
            target_type: AttrRemapperTargetType::Attribute,
            override_if_exists: false,
            target_format: AttrRemapperTargetFormat::Auto,
        };

        step.apply(&mut log).unwrap();

        // Should have copied into custom["dest_attr"]
        assert_eq!(
            get_nested(&log.custom, "dest_attr").cloned(),
            Some(json!("tag_value"))
        );
        // Original tag should still exist
        assert_eq!(
            log.tag.get("foo_tag"),
            Some(&StringOrVec::String("tag_value".to_string()))
        );
    }

    #[test]
    fn test_remap_attribute_to_tag_remove_original() {
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        log.custom
            .insert("foo_attr".to_string(), json!("attr_value"));

        let step = AttributeRemapStep {
            sources: vec!["foo_attr".into()],
            to_path: "dest_tag".into(),
            preserve_original: false,
            source_type: AttrRemapperTargetType::Attribute,
            target_type: AttrRemapperTargetType::Tag,
            override_if_exists: false,
            target_format: AttrRemapperTargetFormat::Auto,
        };

        step.apply(&mut log).unwrap();

        // Should have moved into tag["dest_tag"]
        assert!(log.tag.contains_key("dest_tag"));
        assert!(matches!(
            log.tag.get("dest_tag"),
            Some(StringOrVec::String(s)) if s == "attr_value"
        ));
        // Original custom attribute should be removed
        assert!(!log.custom.contains_key("foo_attr"));
    }

    #[test]
    fn test_remap_attribute_to_tag_preserve_original() {
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        log.custom
            .insert("foo_attr".to_string(), json!("attr_value"));

        let step = AttributeRemapStep {
            sources: vec!["foo_attr".into()],
            to_path: "dest_tag".into(),
            preserve_original: true,
            source_type: AttrRemapperTargetType::Attribute,
            target_type: AttrRemapperTargetType::Tag,
            override_if_exists: false,
            target_format: AttrRemapperTargetFormat::Auto,
        };

        step.apply(&mut log).unwrap();

        // Should have copied into tag["dest_tag"]
        assert!(log.tag.contains_key("dest_tag"));
        assert!(matches!(
            log.tag.get("dest_tag"),
            Some(StringOrVec::String(s)) if s == "attr_value"
        ));
        // Original custom attribute should still exist
        assert_eq!(log.custom.get("foo_attr"), Some(&json!("attr_value")));
    }

    #[test]
    fn test_override_existing_tag() {
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        insert_tag(&mut log, "src", "first");
        insert_tag(&mut log, "dest", "original");

        let step_no_override = AttributeRemapStep {
            sources: vec!["src".into()],
            to_path: "dest".into(),
            preserve_original: true,
            source_type: AttrRemapperTargetType::Tag,
            target_type: AttrRemapperTargetType::Tag,
            override_if_exists: false,
            target_format: AttrRemapperTargetFormat::Auto,
        };
        step_no_override.apply(&mut log).unwrap();
        // Without override, dest stays "original"
        assert!(matches!(
            log.tag.get("dest"),
            Some(StringOrVec::String(s)) if s == "original"
        ));

        let step_override = AttributeRemapStep {
            override_if_exists: true,
            ..step_no_override
        };
        step_override.apply(&mut log).unwrap();
        // With override, dest becomes "first"
        assert!(matches!(
            log.tag.get("dest"),
            Some(StringOrVec::String(s)) if s == "first"
        ));
    }

    #[test]
    fn test_override_existing_attribute() {
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        log.custom.insert("src".to_string(), json!("one"));
        log.custom.insert("dest".to_string(), json!("two"));

        let step_no_override = AttributeRemapStep {
            sources: vec!["src".into()],
            to_path: "dest".into(),
            preserve_original: true,
            source_type: AttrRemapperTargetType::Attribute,
            target_type: AttrRemapperTargetType::Attribute,
            override_if_exists: false,
            target_format: AttrRemapperTargetFormat::Auto,
        };
        step_no_override.apply(&mut log).unwrap();
        // Without override, dest stays "two"
        assert_eq!(log.custom.get("dest"), Some(&json!("two")));

        let step_override = AttributeRemapStep {
            override_if_exists: true,
            ..step_no_override
        };
        step_override.apply(&mut log).unwrap();
        // With override, dest becomes "one"
        assert_eq!(log.custom.get("dest"), Some(&json!("one")));
    }
}
