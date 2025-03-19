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

use vrl::datadog_grok::parse_grok::parse_grok;
use vrl::datadog_grok::parse_grok_rules::GrokRule;

use super::grok_rules::get_grok_rules_by_source;
use crate::error::PipelineError;
use crate::pipeline::*;
use crate::transformers::vrl_value_to_serde_json;
use crate::ProcessedLog;

#[derive(Debug)]
/// The GrokParserAutoStep is a pipeline step that applies grok parsing to the log message based
/// on the value in the `source` field of the log message.
pub struct GrokParserAutoStep {
    pub grok_rules_by_source: BTreeMap<String, Vec<GrokRule>>,
}

impl PipelineStep for GrokParserAutoStep {
    fn apply(&self, value: &mut ProcessedLog) -> crate::Result<()> {
        let log_line = &value.message;
        let Some(grok_rules) = self.grok_rules_by_source.get(&value.source) else {
            return Ok(());
        };
        let result = parse_grok(log_line, grok_rules);

        use vrl::datadog_grok::parse_grok::FatalError;
        match result {
            Ok(parsed) => {
                let json_val = vrl_value_to_serde_json(parsed.parsed)?;
                // TODO: handle errors in parsed.internal_errors
                // TODO: Remove clones

                // The unwrap() is safe, grok always returns an object.
                for (key, v) in json_val.as_object().unwrap() {
                    value.custom.insert(key.clone(), v.clone());
                }
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
