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

use quickwit_processing::{DatadogLogMsg, PipelineStepConfig, ProcessedLog, build_step};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GrokTest {
    pub source: String,
    pub sample_results: Vec<SampleResult>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SampleResult {
    pub sample: String,
    pub result: serde_json::Map<String, Value>,
}

#[test]
/// This is a test to test the compatibility of the grok parser with json logs.
fn test_grok_parser_compat_test() {
    let test_data = include_str!("grok_tests.json");
    let grok_test: Vec<GrokTest> = serde_json::from_str(test_data).expect(
        "Failed to parse
 JSON",
    );

    let yaml = r#"
 type: auto-grok
 id: "123456"
 name: "auto-grok-parser test"
 enabled: true
 "#;
    let total_samples = grok_test
        .iter()
        .map(|x| x.sample_results.len())
        .sum::<usize>();
    let mut matched_samples = 0;
    let parsed_sources = grok_test.len();
    let mut matched_sources = 0;
    for test in grok_test {
        let config: PipelineStepConfig =
            serde_yaml::from_str(yaml).expect("Deserialization failed");
        let step = build_step(&config).unwrap();

        let num_samples_in_source = test.sample_results.len();
        let mut matched_for_source = 0;
        for sample_result in test.sample_results {
            let mut agent_log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
            agent_log.message = sample_result.sample.to_string();
            agent_log.source = Some(test.source.clone());

            step.apply(&mut agent_log).unwrap();

            let mut expected = sample_result.result.clone();
            normalize_numbers_in_obj(&mut expected);
            normalize_numbers_in_obj(&mut agent_log.custom);

            if agent_log.custom != expected && test.source == "agent" {
                println!(
                    "{}: ❌ test failed for sample: {}. Expected: {:?}, got: {:?}",
                    test.source, sample_result.sample, expected, agent_log.custom
                );
            }

            if agent_log.custom == expected {
                matched_samples += 1;
                matched_for_source += 1;
            }
        }
        if matched_for_source == num_samples_in_source {
            matched_sources += 1;
        }

        if matched_for_source == num_samples_in_source {
            println!("{}: ✅ all test succeeded", test.source);
        } else if matched_for_source == 0 {
            println!("{}: ❌ all test failed", test.source);
        } else {
            println!(
                "{}: ⭕ partial supported {}/{} tests succeeded",
                test.source, matched_for_source, num_samples_in_source,
            );
        }
    }
    assert_eq!(matched_samples, 423);
    assert_eq!(total_samples, 520);

    assert_eq!(matched_sources, 113);
    assert_eq!(parsed_sources, 154);
}

/// Convert numbers to f64 recursively
fn normalize_numbers_in_obj(value: &mut serde_json::Map<String, serde_json::Value>) {
    for val in value.values_mut() {
        normalize_numbers(val);
    }
}
/// Convert numbers to f64 recursively
pub fn normalize_numbers(value: &mut Value) {
    match value {
        Value::Number(n) => {
            if let Some(val) = n.as_u64() {
                *value = Value::Number(serde_json::Number::from_f64(val as f64).unwrap());
            } else if let Some(val) = n.as_i64() {
                *value = Value::Number(serde_json::Number::from_f64(val as f64).unwrap());
            }
        }
        Value::Array(arr) => {
            for item in arr {
                normalize_numbers(item);
            }
        }
        Value::Object(map) => {
            normalize_numbers_in_obj(map);
        }
        Value::Null | Value::Bool(_) | Value::String(_) => {}
    }
}

pub fn make_datadog_log_msg() -> DatadogLogMsg {
    DatadogLogMsg {
        message: "Test log message".to_string().into(),
        status: Some("INFO".to_string()),
        timestamp: Some(OffsetDateTime::now_utc()),
        hostname: "test-host".to_string().into(),
        service: "test-service".to_string().into(),
        ddsource: "rust".to_string().into(),
        ddtags: vec!["env:dev".into(), "region:us-east".into()],
    }
}
