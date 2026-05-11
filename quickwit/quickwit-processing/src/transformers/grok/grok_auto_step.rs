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

use std::collections::BTreeMap;

use tracing::error;
use vrl::datadog_grok::parse_grok::parse_grok;
use vrl::datadog_grok::parse_grok_rules::GrokRule;

use super::grok_rules::get_grok_rules_by_source;
use crate::ProcessedLog;
use crate::error::PipelineError;
use crate::pipeline::*;
use crate::transformers::vrl_value_to_serde_json;

#[derive(Debug)]
/// The GrokParserAutoStep is a pipeline step that applies grok parsing to the log message based
/// on the value in the `source` field of the log message.
pub struct GrokParserAutoStep {
    pub grok_rules_by_source: BTreeMap<String, Vec<GrokRule>>,
}

impl PipelineStep for GrokParserAutoStep {
    fn apply(&self, processed_log: &mut ProcessedLog) -> crate::Result<()> {
        let log_line = &processed_log.message;
        let Some(source) = processed_log.source.as_ref() else {
            // If the source is not set, we cannot apply grok parsing.
            return Ok(());
        };
        let Some(grok_rules) = self.grok_rules_by_source.get(source) else {
            return Ok(());
        };
        let result = parse_grok(log_line, grok_rules);

        use vrl::datadog_grok::parse_grok::FatalError;
        match result {
            Ok(parsed) => {
                // TODO: handle errors in parsed.internal_errors
                let json_val = vrl_value_to_serde_json(parsed.parsed)?;
                if let serde_json::Value::Object(json_obj) = json_val {
                    processed_log.custom.extend(json_obj);
                } else {
                    // TODO: Rate limit this error
                    error!(
                        limit_per_min = 10,
                        "grok is supposed to return an object. received something else"
                    );
                };
            }
            Err(e) => match e {
                FatalError::NoMatch => {}
                FatalError::RegexEngineError => {
                    return Err(PipelineError::GrokParse {
                        message: "Regex engine error".to_string(),
                    });
                }
            },
        }

        Ok(())
    }
}

pub fn build_grok_parser_auto_step() -> Result<GrokParserAutoStep, PipelineError> {
    let grok_rules_by_source = get_grok_rules_by_source()
        .iter()
        .map(|(name, rules)| (name.clone(), rules.clone()))
        .collect();

    Ok(GrokParserAutoStep {
        grok_rules_by_source,
    })
}
