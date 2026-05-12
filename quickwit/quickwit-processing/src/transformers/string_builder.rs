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

use serde_json::{Map, Value};

use crate::ProcessedLog;
use crate::error::PipelineError;
use crate::path_access::*;
use crate::pipeline::*;

/// A step that constructs a string from a template and writes it to a field.
///
/// <https://docs.datadoghq.com/service_management/events/pipelines_and_processors/string_builder_processor/>
#[derive(Debug)]
pub struct StringBuilderStep {
    pub template: CompiledTemplateString,
    pub to_path: ParsedPath,
    /// If true, it replaces all missing attributes of template by an empty string. If false, skips
    /// the operation for missing attributes.
    pub is_replace_missing: bool,
}

impl PipelineStep for StringBuilderStep {
    fn apply(&self, log: &mut ProcessedLog) -> Result<(), PipelineError> {
        let (rendered_value, contained_missing_value) =
            render_with_json(&self.template, &log.custom);
        if contained_missing_value && !self.is_replace_missing {
            return Ok(());
        }
        set_value_at_path_on_map(
            &mut log.custom,
            self.to_path.as_ref(),
            Value::String(rendered_value),
        );
        Ok(())
    }
}

fn render_with_json(
    compiled: &CompiledTemplateString,
    json: &Map<String, serde_json::Value>,
) -> (String, bool) {
    let mut contained_missing_value = false;
    let rendered_str = compiled.render(|path| {
        let mut rendered_str = String::new();

        // ", " is the separator between nested values.
        //
        fn append_to_rendered(value: &str, rendered_str: &mut String) {
            if !rendered_str.is_empty() {
                rendered_str.push_str(", ");
            }
            rendered_str.push_str(value);
        }

        fn handle_val(value: &Value, rendered_str: &mut String) {
            match value {
                Value::String(s) => append_to_rendered(s, rendered_str),
                Value::Number(n) => append_to_rendered(&n.to_string(), rendered_str),
                Value::Bool(b) => append_to_rendered(&b.to_string(), rendered_str),
                Value::Array(vals) => {
                    for val in vals {
                        handle_val(val, rendered_str);
                    }
                }
                Value::Null | Value::Object(_) => {}
            };
        }

        let mut found_value_in_path = false;
        traverse_in_json_obj(json, path.as_ref(), &mut |val| {
            found_value_in_path = true;
            handle_val(val, &mut rendered_str)
        });
        if !found_value_in_path {
            contained_missing_value = true;
        }
        rendered_str
    });
    (rendered_str, contained_missing_value)
}

#[derive(Debug)]
enum TemplatePart {
    Literal(String),
    Lookup(ParsedPath),
}
impl TemplatePart {
    fn new_lookup(s: &str) -> Self {
        Self::Lookup(parse_path(s))
    }
}

#[derive(Debug)]
pub struct CompiledTemplateString {
    parts: Vec<TemplatePart>,
}

impl CompiledTemplateString {
    pub fn compile(template: &str) -> Self {
        let mut parts = Vec::new();
        let mut cursor = 0;

        while let Some(start) = template[cursor..].find("%{") {
            let start = cursor + start;
            // Add the literal segment before the token.
            if start > cursor {
                parts.push(TemplatePart::Literal(template[cursor..start].to_string()));
            }

            // Find the closing '}' after "%{".
            if let Some(end) = template[start + 2..].find('}') {
                let end = start + 2 + end;
                let key = &template[start + 2..end];
                parts.push(TemplatePart::new_lookup(key));
                cursor = end + 1;
            } else {
                // No closing '}' found; treat the rest as literal.
                parts.push(TemplatePart::Literal(template[start..].to_string()));
                break;
            }
        }

        // Append any remaining literal text.
        if cursor < template.len() {
            parts.push(TemplatePart::Literal(template[cursor..].to_string()));
        }

        Self { parts }
    }
    fn render<F>(&self, mut lookup: F) -> String
    where F: FnMut(&ParsedPath) -> String {
        let mut result = String::new();
        for part in &self.parts {
            match part {
                TemplatePart::Literal(text) => result.push_str(text),
                TemplatePart::Lookup(key) => {
                    let replacement = lookup(key);
                    result.push_str(&replacement);
                }
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {

    use serde_json::json;

    use super::*;
    use crate::processed_log::tests::make_datadog_log_msg;

    #[test]
    fn test_no_tokens() {
        let template = "Hello, world!";
        let compiled = CompiledTemplateString::compile(template);
        let output = compiled.render(|_path| "".to_string());
        assert_eq!(output, "Hello, world!");
    }

    #[test]
    fn test_returns_no_token() {
        let template = "Hello, %{world}!";
        let compiled = CompiledTemplateString::compile(template);
        assert_eq!(compiled.render(|_path| "".to_string()), "Hello, !");
        assert_eq!(
            compiled.render(|_path| "world".to_string()),
            "Hello, world!"
        );
    }

    fn apply_with_template(template: &str, is_replace_missing: bool, log: &mut ProcessedLog) {
        apply_with_template_with_out(template, is_replace_missing, log, "out");
    }

    fn apply_with_template_with_out(
        template: &str,
        is_replace_missing: bool,
        log: &mut ProcessedLog,
        out: &str,
    ) {
        let step = StringBuilderStep {
            template: CompiledTemplateString::compile(template),
            to_path: out.into(),

            is_replace_missing,
        };
        step.apply(log).unwrap();
    }

    #[test]
    fn test_string_builder_step() {
        let get_log = || {
            let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());

            // Insert an entry in `log.custom` at key "alpha"
            log.custom.insert("alpha".to_string(), json!(123));
            log.custom.insert("beta".to_string(), json!("noice"));
            log
        };

        let mut log = get_log();
        apply_with_template("Hello, %{alpha}!", true, &mut log);
        assert_eq!(&log.custom["out"], &json!("Hello, 123!"),);

        // Don't replace if missing
        let mut log = get_log();
        apply_with_template("Hello, %{blub}!", false, &mut log);
        assert_eq!(log.custom.get("out"), None);
    }

    #[test]
    fn test_string_builder_step_nested() {
        let get_log = || {
            let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());

            // Insert an entry in `log.custom` at key "alpha"
            log.custom.insert("alpha".to_string(), json!(123));
            log.custom.insert("beta".to_string(), json!("noice"));
            log.custom
                .insert("nested".to_string(), json!({"name": "fred"}));

            log
        };

        let mut log = get_log();
        apply_with_template_with_out(
            "Hello, %{alpha} %{nested.name}!",
            true,
            &mut log,
            "nested.out",
        );
        assert_eq!(
            log.custom["nested"].get("out"),
            Some(&json!("Hello, 123 fred!")),
        );

        let mut log = get_log();
        apply_with_template_with_out(
            "Hello, %{alpha} %{nested.name}!",
            true,
            &mut log,
            "nested.out",
        );
        assert_eq!(
            log.custom["nested"].get("out"),
            Some(&json!("Hello, 123 fred!")),
        );

        // Don't replace if missing
        let mut log = get_log();
        apply_with_template("Hello, %{nested.abc}!", false, &mut log);
        assert_eq!(log.custom.get("out"), None);
        // Don't replace if missing
        let mut log = get_log();
        apply_with_template("Hello, %{asdf.abc}!", false, &mut log);
        assert_eq!(log.custom.get("out"), None);
    }
}
