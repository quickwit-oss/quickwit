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
use crate::processed_log::try_parse_and_update_timestamp;

/// A step that tries to parse a timestamp and set it as the log's timestamp.
#[derive(Debug)]
pub struct DateRemapStep {
    pub sources: Vec<ParsedPath>,
}

impl PipelineStep for DateRemapStep {
    fn apply(&self, value: &mut ProcessedLog) -> Result<(), PipelineError> {
        for from_path in &self.sources {
            // Extract the value at `from_path`
            let from_val_opt = get_nested(&value.custom, &from_path.original).cloned();
            if let Some(from_val) = from_val_opt {
                try_parse_and_update_timestamp(value, &from_val);
                break;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use time::OffsetDateTime;

    use super::*;
    use crate::processed_log::tests::make_datadog_log_msg;

    #[test]
    fn test_date_remap() {
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());

        log.custom
            .insert("timestamp".to_string(), json!("2021-01-01T00:00:00Z"));

        let step = DateRemapStep {
            sources: vec!["timestamp".into()],
        };

        step.apply(&mut log).unwrap();

        assert_eq!(
            log.timestamp,
            OffsetDateTime::from_unix_timestamp(1609459200).unwrap()
        );
    }
}
