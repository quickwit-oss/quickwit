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
use std::mem;
use std::sync::Mutex;

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
    OtlpLogs,
    OtlpTraces,
    Jaeger,
    UiIndexPageLoad,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexingSourceKind {
    File,
    IngestV1,
    IngestV2,
    Kafka,
    Kinesis,
    Nats,
    #[serde(rename = "pubsub")]
    PubSub,
    Pulsar,
    /// Sources for tests and the CLI, such as `vec` or `stdin`.
    Other,
}

/// Documents indexed from one type of source since the previous report, as ranges.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexingUsage {
    pub indexed_docs: String,
    pub indexed_bytes: String,
    pub rejected_docs: String,
}

#[derive(Default)]
struct SourceIndexingCounts {
    num_indexed_docs: u64,
    num_indexed_bytes: u64,
    num_rejected_docs: u64,
}

/// One lock keeps snapshots from splitting a batch across reports.
struct UsageCounts {
    num_api_requests: BTreeMap<ApiKind, u64>,
    indexing_counts: BTreeMap<IndexingSourceKind, SourceIndexingCounts>,
}

impl UsageCounts {
    const fn new() -> Self {
        Self {
            num_api_requests: BTreeMap::new(),
            indexing_counts: BTreeMap::new(),
        }
    }

    fn record_api_request(&mut self, api_kind: ApiKind) {
        *self.num_api_requests.entry(api_kind).or_default() += 1;
    }

    fn record_indexed_batch(
        &mut self,
        source_kind: IndexingSourceKind,
        num_indexed_docs: u64,
        num_indexed_bytes: u64,
        num_rejected_docs: u64,
    ) {
        if num_indexed_docs == 0 && num_rejected_docs == 0 {
            return;
        }
        let source_counts = self.indexing_counts.entry(source_kind).or_default();
        source_counts.num_indexed_docs += num_indexed_docs;
        source_counts.num_indexed_bytes += num_indexed_bytes;
        source_counts.num_rejected_docs += num_rejected_docs;
    }

    fn take(
        &mut self,
    ) -> (
        BTreeMap<ApiKind, String>,
        BTreeMap<IndexingSourceKind, IndexingUsage>,
    ) {
        let api_usage = mem::take(&mut self.num_api_requests)
            .into_iter()
            .map(|(api_kind, num_requests)| (api_kind, magnitude_bucket(num_requests)))
            .collect();
        let indexing_usage = mem::take(&mut self.indexing_counts)
            .into_iter()
            .map(|(source_kind, source_counts)| {
                let source_usage = IndexingUsage {
                    indexed_docs: magnitude_bucket(source_counts.num_indexed_docs),
                    indexed_bytes: magnitude_bucket(source_counts.num_indexed_bytes),
                    rejected_docs: magnitude_bucket(source_counts.num_rejected_docs),
                };
                (source_kind, source_usage)
            })
            .collect();
        (api_usage, indexing_usage)
    }
}

static USAGE_COUNTS: Mutex<UsageCounts> = Mutex::new(UsageCounts::new());

pub fn record_api_request(api_kind: ApiKind) {
    if crate::is_telemetry_enabled() {
        USAGE_COUNTS.lock().unwrap().record_api_request(api_kind);
    }
}

pub fn record_indexed_batch(
    source_kind: IndexingSourceKind,
    num_indexed_docs: u64,
    num_indexed_bytes: u64,
    num_rejected_docs: u64,
) {
    if crate::is_telemetry_enabled() {
        USAGE_COUNTS.lock().unwrap().record_indexed_batch(
            source_kind,
            num_indexed_docs,
            num_indexed_bytes,
            num_rejected_docs,
        );
    }
}

impl TelemetryEvent {
    pub fn running() -> Self {
        TelemetryEvent::Running {
            api_usage: BTreeMap::new(),
            indexing_usage: BTreeMap::new(),
        }
    }

    pub fn end_command(return_code: i32) -> Self {
        TelemetryEvent::EndCommand {
            return_code,
            api_usage: BTreeMap::new(),
            indexing_usage: BTreeMap::new(),
        }
    }

    /// Call only after enqueueing so dropped events do not discard usage counts.
    pub(crate) fn add_usage(&mut self) {
        if let TelemetryEvent::Running {
            api_usage,
            indexing_usage,
        }
        | TelemetryEvent::EndCommand {
            api_usage,
            indexing_usage,
            ..
        } = self
        {
            (*api_usage, *indexing_usage) = USAGE_COUNTS.lock().unwrap().take();
        }
    }
}

/// Returns the power-of-ten range that holds `count`, such as `100-999`.
fn magnitude_bucket(count: u64) -> String {
    if count == 0 {
        return "0".to_string();
    }
    let lower_bound = 10u64.pow(count.ilog10());
    match lower_bound.checked_mul(10) {
        Some(upper_bound) => format!("{lower_bound}-{}", upper_bound - 1),
        None => format!("{lower_bound}+"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_usage_counts_reports_buckets_and_resets() {
        let mut usage_counts = UsageCounts::new();
        usage_counts.record_api_request(ApiKind::NativeSearch);
        for _ in 0..12 {
            usage_counts.record_api_request(ApiKind::ElasticsearchIngest);
        }
        usage_counts.record_indexed_batch(IndexingSourceKind::Kafka, 1_500, 2_000_000, 0);
        usage_counts.record_indexed_batch(IndexingSourceKind::Kafka, 500, 1_000_000, 3);
        usage_counts.record_indexed_batch(IndexingSourceKind::File, 0, 0, 0);
        let (api_usage, indexing_usage) = usage_counts.take();
        assert_eq!(
            api_usage,
            BTreeMap::from([
                (ApiKind::ElasticsearchIngest, "10-99".to_string()),
                (ApiKind::NativeSearch, "1-9".to_string()),
            ])
        );
        let expected_kafka_usage = IndexingUsage {
            indexed_docs: "1000-9999".to_string(),
            indexed_bytes: "1000000-9999999".to_string(),
            rejected_docs: "1-9".to_string(),
        };
        assert_eq!(
            indexing_usage,
            BTreeMap::from([(IndexingSourceKind::Kafka, expected_kafka_usage)])
        );
        let (api_usage, indexing_usage) = usage_counts.take();
        assert!(api_usage.is_empty());
        assert!(indexing_usage.is_empty());
    }

    #[test]
    fn test_magnitude_bucket() {
        assert_eq!(magnitude_bucket(0), "0");
        assert_eq!(magnitude_bucket(9), "1-9");
        assert_eq!(magnitude_bucket(10), "10-99");
        assert_eq!(magnitude_bucket(1_000_000), "1000000-9999999");
        assert_eq!(magnitude_bucket(u64::MAX), "10000000000000000000+");
    }
}
