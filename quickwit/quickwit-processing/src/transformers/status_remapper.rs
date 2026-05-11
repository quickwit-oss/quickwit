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

use std::fmt;
use std::fmt::{Display, Formatter};

use serde_json::Value;

use crate::ProcessedLog;
use crate::error::PipelineError;
use crate::path_access::*;
use crate::pipeline::*;

/// A step that remaps one nested path to a new location, optionally removing the original.
///
/// Operations are done on the `custom` field of the log.
#[derive(Debug)]
pub struct StatusRemapStep {
    pub sources: Vec<ParsedPath>,
}

impl PipelineStep for StatusRemapStep {
    fn apply(&self, value: &mut ProcessedLog) -> Result<(), PipelineError> {
        for from_path in &self.sources {
            // Extract the value at `from_path`
            let from_val_opt = get_nested(&value.custom, &from_path.original).cloned();
            if let Some(from_val) = from_val_opt {
                let log_status = remap_log_status(from_val);
                value.status = log_status.to_string();
                break;
            }
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum LogSeverity {
    Emerg,    // 0
    Alert,    // 1
    Critical, // 2
    Error,    // 3
    Warning,  // 4
    Notice,   // 5
    Info,     // 6
    Debug,    // 7
    Ok,
}
impl Display for LogSeverity {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            LogSeverity::Emerg => write!(f, "emergency"),
            LogSeverity::Alert => write!(f, "alert"),
            LogSeverity::Critical => write!(f, "critical"),
            LogSeverity::Error => write!(f, "error"),
            LogSeverity::Warning => write!(f, "warn"),
            LogSeverity::Notice => write!(f, "notice"),
            LogSeverity::Info => write!(f, "info"),
            LogSeverity::Debug => write!(f, "debug"),
            LogSeverity::Ok => write!(f, "ok"),
        }
    }
}

// According to http://docs.datadoghq.com/logs/log_configuration/processors/?tab=api#log-status-remapper
// and https://github.com/DataDog/logs-backend/blob/8ca107d04d6fbca6ef00702ab474aa841c1da748/domains/event-platform/libs/processing/processing-common/src/main/java/com/dd/logs/processing/processors/StatusRemapper.java
//
pub fn remap_log_status(input: Value) -> LogSeverity {
    match input {
        Value::Number(num) => {
            // Try Syslog severity levels
            // https://en.wikipedia.org/wiki/Syslog#Severity_level
            //
            let n: i64 = if let Some(i) = num.as_i64() {
                i
            } else if let Some(u) = num.as_u64() {
                u as i64
            } else if let Some(f) = num.as_f64() {
                f.trunc() as i64
            } else {
                -1 // Invalid number, default to Info
            };
            match n {
                0 => LogSeverity::Emerg,
                1 => LogSeverity::Alert,
                2 => LogSeverity::Critical,
                3 => LogSeverity::Error,
                4 => LogSeverity::Warning,
                5 => LogSeverity::Notice,
                6 => LogSeverity::Info,
                7 => LogSeverity::Debug,
                _ => LogSeverity::Info,
            }
        }
        // Handle strings: apply case-insensitive matching on the start of the string.
        Value::String(mut severity_name) => {
            severity_name.make_ascii_lowercase();
            if severity_name.starts_with("emerg") || severity_name.starts_with("f") {
                LogSeverity::Emerg
            } else if severity_name.starts_with("a") {
                LogSeverity::Alert
            } else if severity_name.starts_with("c") {
                LogSeverity::Critical
            } else if severity_name.starts_with("err") || severity_name == "e" {
                LogSeverity::Error
            } else if severity_name.starts_with("w") {
                LogSeverity::Warning
            } else if severity_name.starts_with("n") {
                LogSeverity::Notice
            } else if severity_name.starts_with("i") {
                LogSeverity::Info
            } else if severity_name.starts_with("d")
                || severity_name.starts_with("t")
                || severity_name.starts_with("v")
            {
                LogSeverity::Debug
            } else if severity_name.starts_with("o") || severity_name.starts_with("s") {
                LogSeverity::Ok
            } else {
                LogSeverity::Info
            }
        }
        // For any other JSON value types, default to Info.
        _ => LogSeverity::Info,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_numbers() {
        assert_eq!(remap_log_status(json!(0)), LogSeverity::Emerg);
        assert_eq!(remap_log_status(json!(1)), LogSeverity::Alert);
        assert_eq!(remap_log_status(json!(2)), LogSeverity::Critical);
        assert_eq!(remap_log_status(json!(3)), LogSeverity::Error);
        assert_eq!(remap_log_status(json!(4)), LogSeverity::Warning);
        assert_eq!(remap_log_status(json!(5)), LogSeverity::Notice);
        assert_eq!(remap_log_status(json!(6)), LogSeverity::Info);
        assert_eq!(remap_log_status(json!(7)), LogSeverity::Debug);
        // Out-of-range numbers default to Info.
        assert_eq!(remap_log_status(json!(10)), LogSeverity::Info);
    }

    #[test]
    fn test_strings() {
        assert_eq!(remap_log_status(json!("emergency")), LogSeverity::Emerg);
        assert_eq!(remap_log_status(json!("fatal error")), LogSeverity::Emerg);
        assert_eq!(remap_log_status(json!("alerting")), LogSeverity::Alert);
        assert_eq!(remap_log_status(json!("critical")), LogSeverity::Critical);
        assert_eq!(
            remap_log_status(json!("error occurred")),
            LogSeverity::Error
        );
        assert_eq!(
            remap_log_status(json!("warning message")),
            LogSeverity::Warning
        );
        assert_eq!(remap_log_status(json!("notice")), LogSeverity::Notice);
        assert_eq!(remap_log_status(json!("info update")), LogSeverity::Info);
        assert_eq!(remap_log_status(json!("debug message")), LogSeverity::Debug);
        assert_eq!(remap_log_status(json!("trace route")), LogSeverity::Debug);
        assert_eq!(
            remap_log_status(json!("verbose logging")),
            LogSeverity::Debug
        );
        assert_eq!(remap_log_status(json!("OK")), LogSeverity::Ok);
        assert_eq!(remap_log_status(json!("Success")), LogSeverity::Ok);
        // Unmatched strings default to Info.
        assert_eq!(remap_log_status(json!("blub string")), LogSeverity::Info);
    }

    #[test]
    fn test_invalid_json() {
        // Non-string and non-number JSON types should default to Info.
        assert_eq!(remap_log_status(json!([1, 2, 3])), LogSeverity::Info);
        assert_eq!(
            remap_log_status(json!({"status": "error"})),
            LogSeverity::Info
        );
    }
}
