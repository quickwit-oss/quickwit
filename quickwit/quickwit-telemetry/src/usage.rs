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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};

use serde::{Deserialize, Serialize};

use crate::payload::TelemetryEvent;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
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

impl ApiKind {
    /// In declaration order: `ApiRequestCounts` is indexed by `api_kind as usize`.
    const ALL: [ApiKind; 7] = [
        ApiKind::ElasticsearchSearch,
        ApiKind::ElasticsearchIngest,
        ApiKind::ElasticsearchOther,
        ApiKind::NativeSearch,
        ApiKind::NativeIngest,
        ApiKind::Otlp,
        ApiKind::Jaeger,
    ];
}

struct ApiRequestCounts([AtomicU64; ApiKind::ALL.len()]);

impl ApiRequestCounts {
    const fn new() -> Self {
        Self([const { AtomicU64::new(0) }; ApiKind::ALL.len()])
    }

    fn record(&self, api_kind: ApiKind) {
        self.0[api_kind as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the bucketed counts of the APIs that received requests, and resets the counts.
    fn take(&self) -> Option<BTreeMap<ApiKind, String>> {
        let mut api_usage = BTreeMap::new();
        for api_kind in ApiKind::ALL {
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterSummary {
    pub num_indexes: String,
    pub source_types: BTreeSet<String>,
}

static CLUSTER_SUMMARY: Mutex<Option<ClusterSummary>> = Mutex::new(None);

pub fn record_api_request(api_kind: ApiKind) {
    API_REQUEST_COUNTS.record(api_kind);
}

/// Only the node running the control plane calls it, so each cluster reports its summary once.
pub fn set_cluster_summary(num_indexes: usize, source_types: BTreeSet<String>) {
    let cluster_summary = ClusterSummary {
        num_indexes: index_count_bucket(num_indexes).to_string(),
        source_types,
    };
    *CLUSTER_SUMMARY
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = Some(cluster_summary);
}

impl TelemetryEvent {
    pub fn running() -> Self {
        TelemetryEvent::Running {
            api_usage: None,
            cluster_summary: None,
        }
    }

    pub fn end_command(return_code: i32) -> Self {
        TelemetryEvent::EndCommand {
            return_code,
            api_usage: None,
            cluster_summary: None,
        }
    }

    /// Adds the API request counts since the previous report and the cluster summary. Only called
    /// once the event is queued, so that a dropped event never loses the counts.
    pub(crate) fn add_usage(&mut self) {
        if let TelemetryEvent::Running {
            api_usage,
            cluster_summary,
        }
        | TelemetryEvent::EndCommand {
            api_usage,
            cluster_summary,
            ..
        } = self
        {
            *api_usage = API_REQUEST_COUNTS.take();
            *cluster_summary = CLUSTER_SUMMARY
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
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

fn index_count_bucket(num_indexes: usize) -> &'static str {
    match num_indexes {
        0 => "0",
        1 => "1",
        2..=10 => "2-10",
        11..=100 => "11-100",
        101..=1_000 => "101-1000",
        _ => "1001+",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_kind_all_matches_discriminants() {
        for (index, api_kind) in ApiKind::ALL.iter().enumerate() {
            assert_eq!(*api_kind as usize, index);
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
        assert_eq!(index_count_bucket(1), "1");
        assert_eq!(index_count_bucket(2), "2-10");
        assert_eq!(index_count_bucket(1_001), "1001+");
    }
}
