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
use std::sync::atomic::{AtomicU64, Ordering};

use enum_iterator::{Sequence, all, cardinality};
use serde::{Deserialize, Serialize};

use crate::payload::TelemetryEvent;

#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize, Sequence,
)]
#[serde(rename_all = "snake_case")]
pub enum ApiKind {
    ElasticsearchSearch,
    ElasticsearchIngest,
    ElasticsearchOther,
    NativeSearch,
    NativeIngest,
    Otlp,
    Jaeger,
}

/// Indexed by `api_kind as usize`.
struct ApiRequestCounts([AtomicU64; cardinality::<ApiKind>()]);

impl ApiRequestCounts {
    const fn new() -> Self {
        Self([const { AtomicU64::new(0) }; cardinality::<ApiKind>()])
    }

    fn record(&self, api_kind: ApiKind) {
        self.0[api_kind as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the bucketed counts of the APIs that received requests, and resets the counts.
    fn take(&self) -> Option<BTreeMap<ApiKind, String>> {
        let mut api_usage = BTreeMap::new();
        for api_kind in all::<ApiKind>() {
            let num_requests = self.0[api_kind as usize].swap(0, Ordering::Relaxed);
            if num_requests > 0 {
                api_usage.insert(api_kind, request_count_bucket(num_requests).to_string());
            }
        }
        if api_usage.is_empty() {
            return None;
        }
        Some(api_usage)
    }
}

static API_REQUEST_COUNTS: ApiRequestCounts = ApiRequestCounts::new();

pub fn record_api_request(api_kind: ApiKind) {
    if crate::is_telemetry_enabled() {
        API_REQUEST_COUNTS.record(api_kind);
    }
}

impl TelemetryEvent {
    pub fn running() -> Self {
        TelemetryEvent::Running { api_usage: None }
    }

    pub fn end_command(return_code: i32) -> Self {
        TelemetryEvent::EndCommand {
            return_code,
            api_usage: None,
        }
    }

    /// Adds the API request counts since the previous report. Only called once the event is
    /// queued, so that a dropped event never loses the counts.
    pub(crate) fn add_usage(&mut self) {
        if let TelemetryEvent::Running { api_usage }
        | TelemetryEvent::EndCommand { api_usage, .. } = self
        {
            *api_usage = API_REQUEST_COUNTS.take();
        }
    }
}

fn request_count_bucket(num_requests: u64) -> &'static str {
    match num_requests {
        0 => "0",
        1..=9 => "1-9",
        10..=99 => "10-99",
        100..=999 => "100-999",
        1_000..=9_999 => "1000-9999",
        10_000..=99_999 => "10000-99999",
        100_000..=999_999 => "100000-999999",
        _ => "1000000+",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_kind_discriminants_follow_declaration_order() {
        for (index, api_kind) in all::<ApiKind>().enumerate() {
            assert_eq!(api_kind as usize, index);
        }
    }

    #[test]
    fn test_api_request_counts_reports_buckets_and_resets() {
        let api_request_counts = ApiRequestCounts::new();
        api_request_counts.record(ApiKind::NativeSearch);
        for _ in 0..12 {
            api_request_counts.record(ApiKind::ElasticsearchIngest);
        }
        let api_usage = api_request_counts.take().unwrap();
        assert_eq!(
            api_usage,
            BTreeMap::from([
                (ApiKind::ElasticsearchIngest, "10-99".to_string()),
                (ApiKind::NativeSearch, "1-9".to_string()),
            ])
        );
        assert_eq!(api_request_counts.take(), None);
    }

    #[test]
    fn test_buckets() {
        assert_eq!(request_count_bucket(9), "1-9");
        assert_eq!(request_count_bucket(10), "10-99");
        assert_eq!(request_count_bucket(1_000_000), "1000000+");
    }
}
