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

use vrl::datadog_filter::Matcher;
use vrl::datadog_grok::parse_grok::parse_grok;
use vrl::datadog_grok::parse_grok_rules::{parse_grok_rules, GrokRule};
use vrl::value::Value as VrlValue;

use crate::error::PipelineError;
use crate::pipeline::*;
use crate::ProcessedLog;

#[derive(Debug)]
pub struct GrokParserStep {
    pub filter: Box<dyn Matcher<ProcessedLog>>,
    pub grok_rules: Vec<GrokRule>,
}

impl PipelineStep for GrokParserStep {
    fn apply(&self, value: &mut ProcessedLog) -> crate::Result<()> {
        if !self.filter.run(value) {
            return Ok(());
        }

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

/// A helper function to convert VRL's `Value` into `serde_json::Value`.
fn vrl_value_to_serde_json(v: VrlValue) -> crate::Result<serde_json::Value> {
    let value = match v {
        VrlValue::Bytes(s) => {
            // This can't fail, because the grok parser only returns strings.
            serde_json::Value::String(String::from_utf8(s.to_vec()).map_err(|err| {
                PipelineError::Other {
                    source: err.to_string(),
                }
            })?)
        }
        VrlValue::Float(f) => serde_json::Value::from(f.into_inner()),
        VrlValue::Array(arr) => {
            let json_arr: crate::Result<Vec<_>> =
                arr.into_iter().map(vrl_value_to_serde_json).collect();
            serde_json::Value::Array(json_arr?)
        }
        VrlValue::Object(map) => {
            let json_map = map
                .into_iter()
                .map(|(k, v)| Ok((String::from(k.clone()), vrl_value_to_serde_json(v)?)))
                .collect::<crate::Result<_>>()?;
            serde_json::Value::Object(json_map)
        }
        VrlValue::Null => serde_json::Value::Null,
        VrlValue::Boolean(b) => b.into(),
        VrlValue::Regex(_value_regex) => serde_json::Value::Null,
        VrlValue::Integer(i) => i.into(),
        VrlValue::Timestamp(date_time) => date_time.to_rfc3339().into(),
    };
    Ok(value)
}

pub fn build_grok_parser_step(
    pattern_strings: &[String],
    filter: Box<dyn Matcher<ProcessedLog>>,
) -> Result<GrokParserStep, PipelineError> {
    let aliases = BTreeMap::new();

    let grok_rules = parse_grok_rules(pattern_strings, aliases)
        .map_err(|source| PipelineError::GrokCompile { source })?;

    Ok(GrokParserStep { filter, grok_rules })
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::pipeline::Pipeline;
    use crate::processed_log::tests::make_datadog_log_msg;
    use crate::ProcessedLog;

    #[test]
    fn test_vrl_grok_step() -> Result<(), Box<dyn std::error::Error>> {
        let filter = Box::new(true);
        let pattern_strings = vec!["time=%{TIME:time} ip=%{IP:ip}".to_string()];

        let step = build_grok_parser_step(&pattern_strings, filter)?;
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
        let invalid_pattern = "(??"; // broken pattern
        let pattern_strings = vec![invalid_pattern.to_string()];

        let res = build_grok_parser_step(&pattern_strings, Box::new(true));
        assert!(res.is_err());
    }
}
