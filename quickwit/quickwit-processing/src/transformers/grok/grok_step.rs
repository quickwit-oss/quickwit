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

use vrl::datadog_grok::parse_grok::parse_grok;
use vrl::datadog_grok::parse_grok_rules::GrokRule;

use super::grok_rules::{LogsProcessingGrokRules, build_grok_rules};
use crate::ProcessedLog;
use crate::error::PipelineError;
use crate::pipeline::*;
use crate::transformers::vrl_value_to_serde_json;

#[derive(Debug)]
pub struct GrokParserStep {
    pub grok_rules: Vec<GrokRule>,
}

impl PipelineStep for GrokParserStep {
    fn apply(&self, value: &mut ProcessedLog) -> crate::Result<()> {
        let log_line = &value.message;
        let result = parse_grok(log_line, &self.grok_rules);

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

pub fn build_grok_parser_step(
    patterns: &LogsProcessingGrokRules,
) -> Result<GrokParserStep, PipelineError> {
    let grok_rules = build_grok_rules(&patterns.support_rules, &patterns.match_rules)?;

    Ok(GrokParserStep { grok_rules })
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::ProcessedLog;
    use crate::pipeline::Pipeline;
    use crate::processed_log::tests::make_datadog_log_msg;

    #[test]
    fn test_vrl_grok_step() -> Result<(), Box<dyn std::error::Error>> {
        let grok = r#"{
            "supportRules": "",
            "matchRules": "_date time=%{TIME:time} ip=%{IP:ip}\n"
        }"#;
        let grok_rules: LogsProcessingGrokRules = serde_json::from_str(grok).unwrap();

        let step = build_grok_parser_step(&grok_rules)?;
        let pipeline = Pipeline::from_steps(vec![Box::new(step)]);

        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        log.message = "time=12:34:56 ip=1.2.3.4".to_string();
        pipeline.apply(&mut log)?;

        // Check the results stored in data.custom
        assert_eq!(log.custom["time"], "12:34:56");
        assert_eq!(log.custom["ip"], "1.2.3.4");
        Ok(())
    }

    #[test]
    fn test_vrl_grok_compile_error() {
        let grok = r#"{
            "supportRules": "",
            "matchRules": "time (??"
        }"#;
        let grok_rules: LogsProcessingGrokRules = serde_json::from_str(grok).unwrap();

        let res = build_grok_parser_step(&grok_rules);

        assert!(res.is_err());
    }

    #[test]
    fn test_vrl_grok_partial_success() {
        let grok = r#"{
            "supportRules": "",
            "matchRules": "time (??\n_date time=%{TIME:time} ip=%{IP:ip}\n"
        }"#;
        let grok_rules: LogsProcessingGrokRules = serde_json::from_str(grok).unwrap();
        let res = build_grok_parser_step(&grok_rules).unwrap();

        // Only one rule should be compiled
        assert!(res.grok_rules.len() == 1);
    }
}
