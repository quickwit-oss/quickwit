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
    OtlpLogs,
    OtlpTraces,
    Jaeger,
    UiIndexPageLoad,
}

/// Type of the source that documents are indexed from.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize, Sequence,
)]
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
    fn take(&self) -> BTreeMap<ApiKind, String> {
        let mut api_usage = BTreeMap::new();
        for api_kind in all::<ApiKind>() {
            let num_requests = self.0[api_kind as usize].swap(0, Ordering::Relaxed);
            if num_requests > 0 {
                api_usage.insert(api_kind, magnitude_bucket(num_requests));
            }
        }
        api_usage
    }
}

struct SourceIndexingCounts {
    num_indexed_docs: AtomicU64,
    num_indexed_bytes: AtomicU64,
    num_rejected_docs: AtomicU64,
}

impl SourceIndexingCounts {
    const fn new() -> Self {
        Self {
            num_indexed_docs: AtomicU64::new(0),
            num_indexed_bytes: AtomicU64::new(0),
            num_rejected_docs: AtomicU64::new(0),
        }
    }
}

/// Indexed by `source_kind as usize`.
struct IndexingCounts([SourceIndexingCounts; cardinality::<IndexingSourceKind>()]);

impl IndexingCounts {
    const fn new() -> Self {
        Self([const { SourceIndexingCounts::new() }; cardinality::<IndexingSourceKind>()])
    }

    fn record(
        &self,
        source_kind: IndexingSourceKind,
        num_indexed_docs: u64,
        num_indexed_bytes: u64,
        num_rejected_docs: u64,
    ) {
        let source_counts = &self.0[source_kind as usize];
        source_counts
            .num_indexed_docs
            .fetch_add(num_indexed_docs, Ordering::Relaxed);
        source_counts
            .num_indexed_bytes
            .fetch_add(num_indexed_bytes, Ordering::Relaxed);
        source_counts
            .num_rejected_docs
            .fetch_add(num_rejected_docs, Ordering::Relaxed);
    }

    /// Returns the bucketed counts of the types of sources that received documents, and resets
    /// the counts.
    fn take(&self) -> BTreeMap<IndexingSourceKind, IndexingUsage> {
        let mut indexing_usage = BTreeMap::new();
        for source_kind in all::<IndexingSourceKind>() {
            let source_counts = &self.0[source_kind as usize];
            let num_indexed_docs = source_counts.num_indexed_docs.swap(0, Ordering::Relaxed);
            let num_indexed_bytes = source_counts.num_indexed_bytes.swap(0, Ordering::Relaxed);
            let num_rejected_docs = source_counts.num_rejected_docs.swap(0, Ordering::Relaxed);
            if num_indexed_docs == 0 && num_rejected_docs == 0 {
                continue;
            }
            let source_usage = IndexingUsage {
                indexed_docs: magnitude_bucket(num_indexed_docs),
                indexed_bytes: magnitude_bucket(num_indexed_bytes),
                rejected_docs: magnitude_bucket(num_rejected_docs),
            };
            indexing_usage.insert(source_kind, source_usage);
        }
        indexing_usage
    }
}

static API_REQUEST_COUNTS: ApiRequestCounts = ApiRequestCounts::new();

static INDEXING_COUNTS: IndexingCounts = IndexingCounts::new();

pub fn record_api_request(api_kind: ApiKind) {
    if crate::is_telemetry_enabled() {
        API_REQUEST_COUNTS.record(api_kind);
    }
}

/// Records a batch of documents from a source: the documents and bytes that were indexed, and
/// the documents that were rejected.
pub fn record_indexed_batch(
    source_kind: IndexingSourceKind,
    num_indexed_docs: u64,
    num_indexed_bytes: u64,
    num_rejected_docs: u64,
) {
    if crate::is_telemetry_enabled() {
        INDEXING_COUNTS.record(
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

    /// Adds the API request and indexing counts since the previous report. Only called once the
    /// event is queued, so that a dropped event never loses the counts.
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
            *api_usage = API_REQUEST_COUNTS.take();
            *indexing_usage = INDEXING_COUNTS.take();
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
    fn test_discriminants_follow_declaration_order() {
        for (index, api_kind) in all::<ApiKind>().enumerate() {
            assert_eq!(api_kind as usize, index);
        }
        for (index, source_kind) in all::<IndexingSourceKind>().enumerate() {
            assert_eq!(source_kind as usize, index);
        }
    }

    #[test]
    fn test_api_request_counts_reports_buckets_and_resets() {
        let api_request_counts = ApiRequestCounts::new();
        api_request_counts.record(ApiKind::NativeSearch);
        for _ in 0..12 {
            api_request_counts.record(ApiKind::ElasticsearchIngest);
        }
        let api_usage = api_request_counts.take();
        assert_eq!(
            api_usage,
            BTreeMap::from([
                (ApiKind::ElasticsearchIngest, "10-99".to_string()),
                (ApiKind::NativeSearch, "1-9".to_string()),
            ])
        );
        assert!(api_request_counts.take().is_empty());
    }

    #[test]
    fn test_indexing_counts_reports_buckets_and_resets() {
        let indexing_counts = IndexingCounts::new();
        indexing_counts.record(IndexingSourceKind::Kafka, 1_500, 2_000_000, 0);
        indexing_counts.record(IndexingSourceKind::Kafka, 500, 1_000_000, 3);
        let indexing_usage = indexing_counts.take();
        let expected_kafka_usage = IndexingUsage {
            indexed_docs: "1000-9999".to_string(),
            indexed_bytes: "1000000-9999999".to_string(),
            rejected_docs: "1-9".to_string(),
        };
        assert_eq!(
            indexing_usage,
            BTreeMap::from([(IndexingSourceKind::Kafka, expected_kafka_usage)])
        );
        assert!(indexing_counts.take().is_empty());
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
