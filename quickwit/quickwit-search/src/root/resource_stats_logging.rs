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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use quickwit_proto::search::{LeafSearchResponse, RootResourceStats};
use tracing::info;

#[derive(Default)]
struct ReceivedStats {
    // Only resource stats are retained, never hits or aggregation payloads.
    responses: Vec<LeafSearchResponse>,
    num_failed_splits: u64,
}

/// Lives outside the search future so cancellation logs the stats already received.
/// In-flight leaves and failed RPCs cannot contribute CPU stats: their work is unknown,
/// not zero. The log explicitly reports coverage and keeps missing timings absent.
pub(crate) struct RootResourceStatsLogGuard {
    start: Instant,
    pub status: &'static str,
    pub leaf_num_calls: AtomicU64,
    pub leaf_num_calls_including_retries: Arc<AtomicU64>,
    received: Mutex<ReceivedStats>,
}

impl RootResourceStatsLogGuard {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
            status: "cancelled",
            leaf_num_calls: AtomicU64::new(0),
            leaf_num_calls_including_retries: Arc::default(),
            received: Mutex::default(),
        }
    }

    pub fn record_response(&self, response: &LeafSearchResponse) {
        let mut received = self.received.lock().expect("resource stats lock poisoned");
        received.num_failed_splits += response.failed_splits.len() as u64;
        received.responses.push(LeafSearchResponse {
            resource_stats: response.resource_stats.clone(),
            ..Default::default()
        });
    }

    fn snapshot(&self) -> (Option<RootResourceStats>, usize, usize) {
        let received = self.received.lock().expect("resource stats lock poisoned");
        let num_with_stats = received
            .responses
            .iter()
            .filter(|resp| resp.resource_stats.is_some())
            .count();
        let stats = super::compute_root_resource_stats(
            &received.responses,
            self.leaf_num_calls.load(Ordering::Relaxed),
            self.leaf_num_calls_including_retries
                .load(Ordering::Relaxed),
            received.num_failed_splits,
        );
        (stats, received.responses.len(), num_with_stats)
    }
}

impl Drop for RootResourceStatsLogGuard {
    fn drop(&mut self) {
        let (stats, num_responses, num_with_stats) = self.snapshot();
        let leaf_num_calls = self.leaf_num_calls.load(Ordering::Relaxed);
        let leaf_sum = stats
            .as_ref()
            .and_then(|stats| stats.leaf_resources_sum.as_ref());
        let split_sum = leaf_sum.and_then(|stats| stats.split_resources_sum.as_ref());
        let leaf_worst = stats
            .as_ref()
            .and_then(|stats| stats.leaf_resources_worst.as_ref());
        info!(
            status = self.status,
            root_wall_time_microsecs = self.start.elapsed().as_micros() as u64,
            leaf_num_calls,
            leaf_num_calls_including_retries = self.leaf_num_calls_including_retries.load(Ordering::Relaxed),
            leaf_num_responses = num_responses,
            leaf_num_responses_with_stats = num_with_stats,
            resource_stats_available = stats.is_some(),
            resource_stats_partial = self.status != "success"
                || num_with_stats as u64 != leaf_num_calls
                || stats.as_ref().is_some_and(|stats| stats.num_failed_splits != 0),
            num_failed_splits = stats.as_ref().map(|stats| stats.num_failed_splits),
            sleaf_wall_time_microsecs = leaf_sum.map(|stats| stats.wall_time_microsecs),
            wleaf_wall_time_microsecs = leaf_worst.map(|stats| stats.wall_time_microsecs),
            sleaf_localexec_num_splits = leaf_sum.map(|stats| stats.localexec_num_splits),
            sleaf_ssplit_cpu_search_microsecs = split_sum.map(|stats| stats.cpu_search_microsecs),
            sleaf_ssplit_warmup_microsecs = split_sum.map(|stats| stats.warmup_microsecs),
            sleaf_ssplit_wait_for_cpu_pool_microsecs = split_sum.map(|stats| stats.wait_for_cpu_pool_microsecs),
            sleaf_ssplit_wait_for_search_permit_microsecs = split_sum.map(|stats| stats.wait_for_search_permit_microsecs),
            leaf_resources_sum = ?leaf_sum,
            leaf_resources_worst = ?leaf_worst,
            "root_resource_stats"
        );
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::search::{LeafResourceStats, SplitResourceStats};

    use super::*;

    #[derive(Clone, Default)]
    struct LogCapture(Arc<Mutex<Vec<std::collections::BTreeMap<String, String>>>>);

    impl tracing::Subscriber for LogCapture {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct Fields(std::collections::BTreeMap<String, String>);
            impl tracing::field::Visit for Fields {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    self.0
                        .insert(field.name().to_string(), format!("{value:?}"));
                }
            }
            let mut fields = Fields(Default::default());
            event.record(&mut fields);
            self.0.lock().unwrap().push(fields.0);
        }
    }

    #[tokio::test]
    async fn timeout_logs_partial_stats_once() {
        use tracing::instrument::WithSubscriber;

        let capture = LogCapture::default();
        let search = async {
            let mut guard = RootResourceStatsLogGuard::new();
            guard.leaf_num_calls.store(2, Ordering::Relaxed);
            guard.record_response(&LeafSearchResponse {
                resource_stats: Some(LeafResourceStats {
                    split_resources_sum: Some(SplitResourceStats {
                        cpu_search_microsecs: 123,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            });
            std::future::pending::<()>().await;
            guard.status = "success";
        };
        async {
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(1), search)
                    .await
                    .is_err()
            );
        }
        .with_subscriber(capture.clone())
        .await;
        let events = capture.0.lock().unwrap();
        assert_eq!(events.len(), 1);
        let fields = &events[0];
        assert!(fields["message"].contains("root_resource_stats"));
        assert_eq!(fields["status"], "\"cancelled\"");
        assert_eq!(fields["resource_stats_partial"], "true");
        assert_eq!(fields["leaf_num_responses"], "1");
        assert_eq!(fields["sleaf_ssplit_cpu_search_microsecs"], "123");
    }

    #[test]
    fn success_logs_complete_stats_once() {
        let capture = LogCapture::default();
        tracing::subscriber::with_default(capture.clone(), || {
            let mut guard = RootResourceStatsLogGuard::new();
            guard.leaf_num_calls.store(1, Ordering::Relaxed);
            guard.record_response(&LeafSearchResponse {
                resource_stats: Some(LeafResourceStats::default()),
                ..Default::default()
            });
            guard.status = "success";
        });
        let events = capture.0.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["status"], "\"success\"");
        assert_eq!(events[0]["resource_stats_partial"], "false");
        assert_eq!(events[0]["resource_stats_available"], "true");
    }

    #[test]
    fn error_without_responses_logs_missing_cpu_once() {
        let capture = LogCapture::default();
        tracing::subscriber::with_default(capture.clone(), || {
            let mut guard = RootResourceStatsLogGuard::new();
            guard.status = "error";
        });
        let events = capture.0.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["status"], "\"error\"");
        assert_eq!(events[0]["resource_stats_partial"], "true");
        assert!(!events[0].contains_key("sleaf_ssplit_cpu_search_microsecs"));
    }

    #[test]
    fn missing_leaf_responses_do_not_report_zero_cpu() {
        let guard = RootResourceStatsLogGuard::new();
        guard.leaf_num_calls.store(4, Ordering::Relaxed);
        let (stats, num_responses, num_with_stats) = guard.snapshot();
        assert!(stats.is_none());
        assert_eq!((num_responses, num_with_stats), (0, 0));
        assert_eq!(guard.status, "cancelled");
    }

    #[test]
    fn partial_leaf_stats_survive_failure() {
        let mut guard = RootResourceStatsLogGuard::new();
        guard.leaf_num_calls.store(4, Ordering::Relaxed);
        guard
            .leaf_num_calls_including_retries
            .store(5, Ordering::Relaxed);
        guard.record_response(&LeafSearchResponse {
            resource_stats: Some(LeafResourceStats {
                split_resources_sum: Some(SplitResourceStats {
                    cpu_search_microsecs: 123,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
        guard.record_response(&LeafSearchResponse::default());
        guard.status = "error";
        let (stats, num_responses, num_with_stats) = guard.snapshot();
        let stats = stats.unwrap();
        assert_eq!((num_responses, num_with_stats), (2, 1));
        assert_eq!(stats.leaf_num_calls, 4);
        assert_eq!(stats.leaf_num_calls_including_retries, 5);
        assert_eq!(
            stats
                .leaf_resources_sum
                .unwrap()
                .split_resources_sum
                .unwrap()
                .cpu_search_microsecs,
            123
        );
    }
}
