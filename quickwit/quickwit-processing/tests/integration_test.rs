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

use quickwit_processing::{
    DatadogLogMsg, MessageValue, PipelineStep, PipelineStepConfig, ProcessedLog, build_step,
    get_integrations_processor,
};
use serde::Deserialize;
use serde_json::Value;
use time::OffsetDateTime;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestSuite {
    pub id: String,
    pub tests: Vec<TestCase>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestCase {
    pub sample: String,
    pub service: Option<String>,
    pub result: LogRecord,
    // TODO:: What is the purpose of this field?
    pub tags: Option<Vec<String>>,
    // TODO:: What is the purpose of this field? It exists only once
    // Ignore it for now (any type)
    pub test_only_for_sources: Option<Value>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogRecord {
    pub custom: serde_json::Map<String, Value>,
    pub message: MessageValue,
    pub status: Option<String>,
    pub service: Option<String>,
    pub trace_id: Option<String>,
    #[serde(alias = "ddtags")]
    pub tags: Vec<String>,
    #[serde(
        default,
        with = "time::serde::timestamp::milliseconds::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub timestamp: Option<OffsetDateTime>,
}

/// Holds total‐check and success‐check counters for each assertion.
#[derive(Default)]
struct CheckTypeCounters {
    // totals
    pub total_message: usize,
    pub total_custom: usize,
    pub total_custom_fully_failed: usize,
    pub total_service: usize,
    pub total_status: usize,
    pub total_timestamp: usize,
    pub total_trace_id: usize,
    // successes
    pub succ_message: usize,
    pub succ_custom: usize,
    pub succ_service: usize,
    pub succ_status: usize,
    pub succ_timestamp: usize,
    pub succ_trace_id: usize,
}

impl CheckTypeCounters {
    /// Bump totals & successes based on the `report`.
    fn bump(&mut self, report: &TestCaseReport) {
        // message
        self.total_message += 1;
        if report.message_matched {
            self.succ_message += 1;
        }

        // custom
        self.total_custom += 1;
        if report.custom_matched {
            self.succ_custom += 1;
        }
        // “fully empty” custom only counted when custom itself failed
        if report.custom_fully_failed || !report.custom_matched {
            self.total_custom_fully_failed += 1;
        }

        // optional checks
        if let Some(ok) = report.service_matched {
            self.total_service += 1;
            if ok {
                self.succ_service += 1;
            }
        }
        if let Some(ok) = report.status_matched {
            self.total_status += 1;
            if ok {
                self.succ_status += 1;
            }
        }
        if let Some(ok) = report.timestamp_matched {
            self.total_timestamp += 1;
            if ok {
                self.succ_timestamp += 1;
            }
        }
        if let Some(ok) = report.trace_id_matched {
            self.total_trace_id += 1;
            if ok {
                self.succ_trace_id += 1;
            }
        }
    }

    /// Print how many *succeeded* out of the total for each category.
    pub fn print(&self) {
        println!(
            "message:      {}/{} succeeded",
            self.succ_message, self.total_message
        );
        println!(
            "custom:       {}/{} succeeded",
            self.succ_custom, self.total_custom
        );
        println!(
            "service:      {}/{} succeeded",
            self.succ_service, self.total_service
        );
        println!(
            "status:       {}/{} succeeded",
            self.succ_status, self.total_status
        );
        println!(
            "timestamp:    {}/{} succeeded",
            self.succ_timestamp, self.total_timestamp
        );
        println!(
            "trace_id:     {}/{} succeeded",
            self.succ_trace_id, self.total_trace_id
        );
    }
}

/// Minimal reproducer: a category-processor with a wildcard query `*get*` did panic with
///   "Bytecode and closure evaluation mismatch for query 0!"
/// when the field value matches case-insensitively but not case-sensitively
/// (e.g. `*get*` vs "Get").
#[test]
fn test_category_processor_wildcard_or_regression() {
    let yaml = r#"
type: category-processor
name: test
enabled: true
categories:
  - filter:
      query: "@x:*get*"
    name: "abc"
target: result
"#;
    let config: PipelineStepConfig =
        serde_yaml::from_str(yaml).expect("YAML deserialization failed");
    let step = build_step(&config).expect("build_step failed");

    let mut processed_log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
    processed_log
        .custom
        .insert("x".to_string(), serde_json::json!("Get"));

    step.apply(&mut processed_log)
        .expect("category processor should not fail");
}

#[test]
/// This is a test to test the compatibility of the integrations processor
///
/// Each integration has a set of tests that are defined in the `integrations` folder.
/// Each integration test file is named `<integration_name>_tests.yaml` and contains a test suite.
/// The test suite contains a set of test cases.
/// Each test case has a list of field values that are expected to be matched (checks).
fn test_integrations_processor() {
    let integrations_processor = get_integrations_processor();

    let mut fully_supported_sources = 0;
    let mut partial_supported_sources = 0;
    let mut all_tests_failed_sources = 0;
    let mut num_sources = 0;
    // For each check, collect some stats
    let mut check_counters = CheckTypeCounters::default();

    let mut global_num_checks = 0;
    let mut global_num_succeeded_checks = 0;

    let mut entries = std::fs::read_dir("./integrations")
        .expect("Failed to read integrations directory")
        .map(|e| e.unwrap())
        .collect::<Vec<_>>();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        if entry.file_name().to_str().unwrap().ends_with("_tests.yaml") {
            continue;
        }
        // We expect for every file in the `integrations` folder to have a corresponding test file
        // if we append `_tests.yaml` to the file name. e.g. `nginx.yaml` should have a
        // corresponding `nginx_tests.yaml`.
        let file_name = entry.file_name().to_str().unwrap().to_string();
        //if !file_name.contains("mongodb") {
        //continue;
        //}
        let file_name_without_extension = file_name.trim_end_matches(".yaml");
        let test_file_name = format!("{file_name_without_extension}_tests.yaml");
        let test_file_path = format!("./integrations/{test_file_name}");
        println!("Testing integration: {file_name}");
        // Load the test file
        let test_file_content = std::fs::read_to_string(&test_file_path).unwrap_or_else(|_| {
            panic!("Test file {test_file_name} not found for integration {file_name}")
        });
        // Deserialize the test file
        let test_suite = match serde_yaml::from_str::<TestSuite>(&test_file_content) {
            Ok(test_suite) => test_suite,
            Err(e) => {
                // Some tests are messy and contain weird stuff
                println!("Failed to parse test file {test_file_name}: {e}");
                continue;
            }
        };

        // Iterate over the tests in the test suite and apply the integration processor on message
        // Partial support means that some fields are matched, but not all.
        let mut num_test_cases_fully_succeeded = 0;
        for test_case in &test_suite.tests {
            let test_report = run_test_case(
                test_case,
                file_name_without_extension,
                integrations_processor,
            );
            global_num_checks += test_report.num_checks();
            global_num_succeeded_checks += test_report.num_successful_checks();
            if test_report.num_failed_checks() == 0 {
                num_test_cases_fully_succeeded += 1;
            }
            check_counters.bump(&test_report);
        }
        let num_tests_integration = test_suite.tests.len();
        num_sources += 1;
        if num_test_cases_fully_succeeded == num_tests_integration {
            fully_supported_sources += 1;
        } else if num_test_cases_fully_succeeded > 0 {
            partial_supported_sources += 1;
        }

        if num_test_cases_fully_succeeded != 0 {
            all_tests_failed_sources += 1;
        }

        if num_test_cases_fully_succeeded == num_tests_integration {
            println!(
                "✅ {file_name_without_extension}: \
                 {num_test_cases_fully_succeeded}/{num_tests_integration} tests matched",
            );
        } else if num_test_cases_fully_succeeded > 0 {
            println!(
                "☑️ {file_name_without_extension}: \
                 {num_test_cases_fully_succeeded}/{num_tests_integration} tests matched",
            );
        } else {
            println!(
                "⚠️ {file_name_without_extension}: \
                 {num_test_cases_fully_succeeded}/{num_tests_integration} tests matched",
            );
        }
    }
    println!(
        "num sources where all checks in all test pass {fully_supported_sources}/{num_sources}",
    );
    println!(
        "num sources where all checks in some tests pass {partial_supported_sources}/{num_sources}"
    );
    println!(
        "num sources where each test has some failing checks \
         {all_tests_failed_sources}/{num_sources}",
    );
    check_counters.print();

    println!("{global_num_succeeded_checks}/{global_num_checks} checks succeeded");
    assert_eq!(global_num_succeeded_checks, 5797);
    assert_eq!(global_num_checks, 8041);
}

#[derive(Debug, Default)]
struct TestCaseReport {
    /// If the custom check failed or not
    custom_matched: bool,
    /// If custom check fully failed (i.e. no custom field was produced)
    custom_fully_failed: bool,
    /// If the message check failed or not
    message_matched: bool,
    /// If the service check failed or not and if it is part of the test case
    service_matched: Option<bool>,
    /// If the status check failed or not, if applicable
    status_matched: Option<bool>,
    /// If the timestamp check failed or not, if applicable
    timestamp_matched: Option<bool>,
    /// If the trace_id check failed or not, if applicable
    trace_id_matched: Option<bool>,
}
impl TestCaseReport {
    fn iter_checks(&self) -> impl Iterator<Item = (&str, bool)> {
        let mut checks = vec![
            ("custom", self.custom_matched),
            ("message", self.message_matched),
        ];
        if let Some(service) = &self.service_matched {
            checks.push(("service", *service));
        }
        if let Some(status) = &self.status_matched {
            checks.push(("status", *status));
        }
        if let Some(timestamp) = &self.timestamp_matched {
            checks.push(("timestamp", *timestamp));
        }
        if let Some(trace_id) = &self.trace_id_matched {
            checks.push(("trace_id", *trace_id));
        }
        checks.into_iter()
    }
    fn num_checks(&self) -> usize {
        self.iter_checks().count()
    }
    fn num_failed_checks(&self) -> usize {
        self.iter_checks().filter(|(_, v)| !*v).count()
    }
    fn num_successful_checks(&self) -> usize {
        self.iter_checks().filter(|(_, v)| *v).count()
    }
}

fn run_test_case(
    test_case: &TestCase,
    file_name_without_extension: &str,
    integrations_processor: &'static dyn PipelineStep,
) -> TestCaseReport {
    let mut report = TestCaseReport::default();
    let mut msg = make_datadog_log_msg();
    msg.message = test_case.sample.to_string().into();
    msg.ddsource = Some(file_name_without_extension.to_string());
    let mut processed_log = ProcessedLog::from_datadog_log_msg(msg);
    //println!("Processing log for source: {}", &processed_log.message);
    //println!(
    //"Processing log for custom: {}",
    //serde_json::to_string(&processed_log.custom).unwrap()
    //);

    integrations_processor
        .apply(&mut processed_log)
        .expect("Failed to apply integration processor");
    // Normalize numbers in the custom field
    let datadog_log_msg = DatadogLogMsg {
        message: test_case.sample.to_string().into(),
        status: test_case.result.status.clone(),
        timestamp: test_case.result.timestamp,
        hostname: None,
        service: test_case.service.clone(),
        ddsource: None,
        ddtags: test_case.result.tags.clone(),
    };
    let mut expected_log = ProcessedLog::from_datadog_log_msg(datadog_log_msg.clone());
    expected_log.trace_id = test_case.result.trace_id.clone();
    let mut expected_custom = expected_log.custom.clone();
    normalize_numbers_in_obj(&mut processed_log.custom);
    normalize_numbers_in_obj(&mut expected_custom);

    report.custom_matched = processed_log.custom != expected_custom;
    report.message_matched = processed_log.message == expected_log.message;
    if processed_log.message.is_empty() && !report.message_matched {
        println!("Expected empty message but got: {}", expected_log.message);
    }
    if processed_log.custom != expected_custom && processed_log.custom.is_empty() {
        report.custom_fully_failed = true;
        //println!("Source {test_file_name}");
    }

    if test_case.service.is_some() {
        report.service_matched = Some(processed_log.service == test_case.service);
    }
    if let Some(status) = &test_case.result.status {
        report.status_matched = Some(processed_log.status == *status);
    }
    if let Some(timestamp) = &test_case.result.timestamp {
        report.timestamp_matched = Some(processed_log.timestamp == *timestamp);
    }
    if let Some(_trace_id) = &expected_log.trace_id {
        report.trace_id_matched = Some(processed_log.trace_id == expected_log.trace_id);
    }
    report
}

fn normalize_numbers_in_obj(value: &mut serde_json::Map<String, serde_json::Value>) {
    for val in value.values_mut() {
        normalize_numbers(val);
    }
}

fn normalize_numbers(value: &mut Value) {
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
            for val in map.values_mut() {
                normalize_numbers(val);
            }
        }
        _ => {}
    }
}

pub fn make_datadog_log_msg() -> DatadogLogMsg {
    DatadogLogMsg {
        message: "Test log message".to_string().into(),
        status: Some("INFO".to_string()),
        timestamp: Some(OffsetDateTime::now_utc()),
        hostname: Some("test-host".to_string()),
        service: Some("test-service".to_string()),
        ddsource: Some("rust".to_string()),
        ddtags: vec!["env:dev".into(), "region:us-east".into()],
    }
}
