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
use syslog_rfc5424::message::ProcId;
use syslog_rfc5424::{SyslogSeverity, parse_message};
use time::OffsetDateTime;

use crate::ProcessedLog;
use crate::error::PipelineError;
use crate::pipeline::PipelineStep;

/// Processor that parses syslog messages and extracts structured data.
///
/// This processor attempts to parse the message field as an RFC5424 syslog message
/// only if the source field is "syslog".
/// If successful, it extracts:
/// - Syslog metadata (severity, facility, appname, procid, etc.) into custom.syslog
/// - Structured data elements into custom fields
/// - Overrides core fields (message, host, timestamp, status) with syslog values
#[derive(Debug)]
pub struct SyslogProcessor;

impl SyslogProcessor {
    /// Map syslog severity to status string
    fn severity_to_status(severity: SyslogSeverity) -> &'static str {
        match severity {
            SyslogSeverity::SEV_EMERG => "emergency",
            SyslogSeverity::SEV_ALERT => "alert",
            SyslogSeverity::SEV_CRIT => "critical",
            SyslogSeverity::SEV_ERR => "error",
            SyslogSeverity::SEV_WARNING => "warning",
            SyslogSeverity::SEV_NOTICE => "notice",
            SyslogSeverity::SEV_INFO => "info",
            SyslogSeverity::SEV_DEBUG => "debug",
        }
    }
}

impl PipelineStep for SyslogProcessor {
    fn apply(&self, processed_log: &mut ProcessedLog) -> Result<(), PipelineError> {
        if let Some(source) = &processed_log.source
            && source != "syslog"
        {
            return Ok(());
        }
        if let Ok(parsed_msg) = parse_message(&processed_log.message) {
            processed_log.status = Self::severity_to_status(parsed_msg.severity).to_string();

            let mut syslog_map = Map::new();

            // unfortunately, the prival field is not included in the parsed_msg struct
            // so we have to calculate it manually
            let prival = (parsed_msg.facility as i32) * 8 + (parsed_msg.severity as i32);
            syslog_map.insert("prival".to_string(), Value::Number(prival.into()));
            syslog_map.insert(
                "version".to_string(),
                Value::Number(parsed_msg.version.into()),
            );
            syslog_map.insert(
                "severity".to_string(),
                Value::Number((parsed_msg.severity as i32).into()),
            );
            syslog_map.insert(
                "facility".to_string(),
                Value::Number((parsed_msg.facility as i32).into()),
            );
            if let Some(appname) = &parsed_msg.appname {
                syslog_map.insert("appname".to_string(), Value::String(appname.clone()));
            }
            if let Some(procid) = &parsed_msg.procid {
                match procid {
                    ProcId::PID(pid) => {
                        syslog_map.insert("procid".to_string(), Value::Number((*pid).into()));
                    }
                    ProcId::Name(name) => {
                        syslog_map.insert("procid".to_string(), Value::String(name.clone()));
                    }
                }
            }

            if let Some(msgid) = &parsed_msg.msgid {
                syslog_map.insert("msgid".to_string(), Value::String(msgid.clone()));
            }

            if let Some(hostname) = &parsed_msg.hostname {
                processed_log.host = Some(hostname.clone());
                syslog_map.insert("hostname".to_string(), Value::String(hostname.clone()));
            }

            if let Some(timestamp) = parsed_msg.timestamp {
                // Convert unix timestamp to time::OffsetDateTime
                if let Ok(offset_dt) = OffsetDateTime::from_unix_timestamp(timestamp) {
                    processed_log.timestamp = offset_dt;
                }
                syslog_map.insert("timestamp".to_string(), Value::Number(timestamp.into()));
            }

            // TODO: check what intake is doing and see if we need to extract structured data
            // // Extract structured data elements
            // for (_sd_id, sd_element) in parsed_msg.sd.iter() {
            //     for (key, value) in sd_element {
            //         processed_log.custom.insert(key.clone(), Value::String(value.clone()));
            //     }
            // }

            processed_log.message = parsed_msg.msg;
            processed_log
                .custom
                .insert("syslog".to_string(), Value::Object(syslog_map));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processed_log::tests::make_processed_log;

    #[test]
    fn test_syslog_parsing_rfc5424() {
        let mut processed_log = make_processed_log();
        processed_log.message = r#"<190>1 2025-10-14T07:10:44+00:00 aaabf280-c060-4637-b4e6-68f3d3c44872 vcap_nginx_access - - -  localhost - [14/Oct/2025:07:10:44 +0000] "GET /healthz HTTP/1.1" 200 214 "-" "curl/7.81.0" 127.0.0.1 vcap_request_id:064249e3-9311-4bf8-9dd7-a0068fa73016 response_time:0.002"#.to_string();
        processed_log.source = Some("syslog".to_string()); // Set source to syslog so processor runs

        let processor = SyslogProcessor;
        processor.apply(&mut processed_log).unwrap();

        // For <190>: facility=23 (190 >> 3 = 23), severity=6 (190 & 7 = 6)
        assert_eq!(processed_log.custom["syslog"]["prival"], 190); // facility=23, severity=6 -> 23*8+6=190
        assert_eq!(processed_log.custom["syslog"]["severity"], 6); // 190 & 7 = 6 (info)
        assert_eq!(processed_log.custom["syslog"]["facility"], 23); // 190 >> 3 = 23 (local7)
        assert_eq!(
            processed_log.custom["syslog"]["appname"],
            "vcap_nginx_access"
        );
        assert_eq!(processed_log.custom["syslog"]["version"], 1);

        // Check that ProcessedLog fields were overridden
        assert_eq!(
            processed_log.host,
            Some("aaabf280-c060-4637-b4e6-68f3d3c44872".to_string())
        );
        assert_eq!(processed_log.status, "info"); // severity 6 maps to "info"
        assert_eq!(
            processed_log.message,
            " localhost - [14/Oct/2025:07:10:44 +0000] \"GET /healthz HTTP/1.1\" 200 214 \"-\" \
             \"curl/7.81.0\" 127.0.0.1 vcap_request_id:064249e3-9311-4bf8-9dd7-a0068fa73016 \
             response_time:0.002"
        );
    }

    #[test]
    fn test_non_syslog_source_unchanged() {
        let mut processed_log = make_processed_log();
        let original_message = r#"<190>1 2025-10-14T07:10:44+00:00 aaabf280-c060-4637-b4e6-68f3d3c44872 vcap_nginx_access - - -  localhost - [14/Oct/2025:07:10:44 +0000] "GET /healthz HTTP/1.1" 200 214 "-" "curl/7.81.0" 127.0.0.1 vcap_request_id:064249e3-9311-4bf8-9dd7-a0068fa73016 response_time:0.002"#.to_string();
        let original_host = processed_log.host.clone();
        let original_status = processed_log.status.clone();

        processed_log.message = original_message.clone();
        // source is not syslog so processor should not run
        processed_log.source = Some("not-syslog".to_string());

        let processor = SyslogProcessor;
        processor.apply(&mut processed_log).unwrap();

        assert_eq!(processed_log.host, original_host);
        assert_eq!(processed_log.status, original_status);
        assert_eq!(processed_log.message, original_message);
        assert!(!processed_log.custom.contains_key("syslog"));
    }
}
