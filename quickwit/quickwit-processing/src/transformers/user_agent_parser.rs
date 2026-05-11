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

use std::sync::OnceLock;

use serde_json::Value;
use uaparser::{Parser, UserAgentParser};

use crate::ProcessedLog;
use crate::error::PipelineError;
use crate::path_access::*;
use crate::pipeline::*;

static UAPARSER: OnceLock<UserAgentParser> = OnceLock::new();
fn get_user_agent_parser() -> &'static UserAgentParser {
    UAPARSER.get_or_init(|| {
        UserAgentParser::from_bytes(include_bytes!("../../ua_regexes.yaml"))
            .expect("Failed to load user agent parser")
    })
}

/// A step that tries to parse a user agent string from the log and
/// extracts details about the OS, device, and browser.
///
/// <https://docs.datadoghq.com/logs/log_configuration/processors/?tab=ui#user-agent-parser>
#[derive(Debug)]
pub struct UserAgentParserStep {
    pub sources: Vec<ParsedPath>,
    pub to_path: ParsedPath,
    pub is_encoded: bool,
    pub combine_version_details: bool,
}

impl PipelineStep for UserAgentParserStep {
    fn apply(&self, value: &mut ProcessedLog) -> Result<(), PipelineError> {
        for from_path in &self.sources {
            let Some(source_val_opt) = get_nested(&value.custom, &from_path.original).cloned()
            else {
                continue;
            };
            if let Some(mut user_agent_string) = source_val_opt.as_str().map(String::from) {
                if self.is_encoded
                    && let Ok(decoded) = urlencoding::decode(&user_agent_string)
                {
                    user_agent_string = decoded.to_string();
                }
                let ua_parser = get_user_agent_parser();
                let client = ua_parser.parse(&user_agent_string);
                let mut user_agent_details = serde_json::Map::new();

                // Add the parsed details to the map
                if let Ok(os) = serde_json::to_value(client.os) {
                    user_agent_details.insert("os".to_string(), os);
                }
                if let Ok(device) = serde_json::to_value(client.device) {
                    user_agent_details.insert("device".to_string(), device);
                }
                if let Ok(browser) = serde_json::to_value(client.user_agent) {
                    user_agent_details.insert("browser".to_string(), browser);
                    if self.combine_version_details {
                        // TODO
                        // If combine_version_details is true, combine major, minor, and patch into
                        // a single version string?
                    }
                }

                // Set the parsed user agent details at the specified path
                set_value_at_path_on_map(
                    &mut value.custom,
                    &self.to_path.segments,
                    Value::Object(user_agent_details),
                );
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
    fn test_user_agent_parser_processor() {
        let yaml = r#"
type: user-agent-parser
id: "123456"
name: ""
enabled: true
sources:
  - user_agent
target: user_agent_details
"#;

        let config: PipelineStepConfig =
            serde_yaml::from_str(yaml).expect("Deserialization failed");
        let step = build_step(&config).unwrap();

        let mut agent_log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        agent_log.custom.insert(
            "user_agent".to_string(),
            json!(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) \
                 Chrome/58.0.3029.110 Safari/537.3"
            ),
        );
        step.apply(&mut agent_log).unwrap();
        assert_eq!(
            agent_log.custom["user_agent_details"]["browser"]["family"],
            "Chrome"
        );
        assert_eq!(
            agent_log.custom["user_agent_details"]["browser"]["major"],
            "58"
        );
        assert_eq!(
            agent_log.custom["user_agent_details"]["browser"]["minor"],
            "0"
        );
        assert_eq!(
            agent_log.custom["user_agent_details"]["os"]["family"],
            "Windows"
        );
        assert_eq!(
            agent_log.custom["user_agent_details"]["device"]["family"],
            "Other"
        );
    }
}
