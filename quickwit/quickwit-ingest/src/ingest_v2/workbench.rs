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

use std::collections::{BTreeMap, HashSet};

use quickwit_common::pubsub::EventBroker;
use quickwit_common::rate_limited_error;
use quickwit_proto::control_plane::{
    GetOrCreateOpenShardsFailure, GetOrCreateOpenShardsFailureReason,
};
use quickwit_proto::ingest::ingester::{
    PersistFailure, PersistFailureReason, PersistRequest, PersistSuccess,
};
use quickwit_proto::ingest::router::{
    IngestFailure, IngestFailureReason, IngestResponseV2, IngestSubrequest, IngestSuccess,
};
use quickwit_proto::ingest::{IngestV2Error, RateLimitingCause};
use quickwit_proto::types::{NodeId, ShardId, SubrequestId};
use tracing::warn;

use super::publish_tracker::PublishTracker;
use super::router::PersistRequestSummary;

/// A helper struct for managing the state of the subrequests of an ingest request during multiple
/// persist attempts.
#[derive(Default)]
pub(super) struct IngestWorkbench {
    pub subworkbenches: BTreeMap<SubrequestId, IngestSubworkbench>,
    pub rate_limited_shards: HashSet<ShardId>,
    pub num_successes: usize,
    /// The number of batch persist attempts. This is not sum of the number of attempts for each
    /// subrequest.
    pub num_attempts: usize,
    pub max_num_attempts: usize,
    /// List of leaders that have been marked as temporarily unavailable.
    /// These leaders have encountered a transport error during an attempt and will be treated as
    /// if they were out of the pool for subsequent attempts.
    ///
    /// (The point here is to make sure we do not wait for the failure detection to kick the node
    /// out of the ingest node.)
    pub unavailable_leaders: HashSet<NodeId>,
    publish_tracker: Option<PublishTracker>,
}

/// Returns an iterator of pending of subrequests, sorted by sub request id.
pub(super) fn pending_subrequests(
    subworkbenches: &BTreeMap<SubrequestId, IngestSubworkbench>,
) -> impl Iterator<Item = &IngestSubrequest> {
    subworkbenches.values().filter_map(|subworbench| {
        if subworbench.is_pending() {
            Some(&subworbench.subrequest)
        } else {
            None
        }
    })
}

impl IngestWorkbench {
    fn new_inner(
        ingest_subrequests: Vec<IngestSubrequest>,
        max_num_attempts: usize,
        publish_tracker: Option<PublishTracker>,
    ) -> Self {
        let subworkbenches: BTreeMap<SubrequestId, IngestSubworkbench> = ingest_subrequests
            .into_iter()
            .map(|subrequest| {
                (
                    subrequest.subrequest_id,
                    IngestSubworkbench::new(subrequest),
                )
            })
            .collect();

        Self {
            subworkbenches,
            max_num_attempts,
            publish_tracker,
            ..Default::default()
        }
    }

    pub fn new(ingest_subrequests: Vec<IngestSubrequest>, max_num_attempts: usize) -> Self {
        Self::new_inner(ingest_subrequests, max_num_attempts, None)
    }

    pub fn new_with_publish_tracking(
        ingest_subrequests: Vec<IngestSubrequest>,
        max_num_attempts: usize,
        event_broker: EventBroker,
    ) -> Self {
        Self::new_inner(
            ingest_subrequests,
            max_num_attempts,
            Some(PublishTracker::new(event_broker)),
        )
    }

    pub fn new_attempt(&mut self) {
        self.num_attempts += 1;
    }

    /// Returns true if all subrequests were successfully persisted or if the
    /// number of attempts has been exhausted.
    pub fn is_complete(&self) -> bool {
        self.num_successes >= self.subworkbenches.len()
            || self.num_attempts >= self.max_num_attempts
            || self.has_no_pending_subrequests()
    }

    pub fn is_last_attempt(&self) -> bool {
        self.num_attempts >= self.max_num_attempts
    }

    fn has_no_pending_subrequests(&self) -> bool {
        self.subworkbenches
            .values()
            .all(|subworbench| !subworbench.is_pending())
    }

    pub fn record_persist_request(&self, persist_request: &PersistRequest) {
        if let Some(publish_tracker) = &self.publish_tracker {
            let shards = persist_request
                .subrequests
                .iter()
                .map(|subrequest| subrequest.shard_id());
            publish_tracker.register_requested_shards(shards);
        }
    }

    pub fn record_get_or_create_open_shards_failure(
        &mut self,
        open_shards_failure: GetOrCreateOpenShardsFailure,
    ) {
        let last_failure = match open_shards_failure.reason() {
            GetOrCreateOpenShardsFailureReason::IndexNotFound => SubworkbenchFailure::IndexNotFound,
            GetOrCreateOpenShardsFailureReason::SourceNotFound => {
                SubworkbenchFailure::SourceNotFound
            }
            GetOrCreateOpenShardsFailureReason::NoIngestersAvailable => {
                SubworkbenchFailure::NoShardsAvailable
            }
            GetOrCreateOpenShardsFailureReason::Unspecified => {
                warn!(
                    "failure reason for subrequest `{}` is unspecified",
                    open_shards_failure.subrequest_id
                );
                SubworkbenchFailure::Internal
            }
        };
        self.record_failure(open_shards_failure.subrequest_id, last_failure);
    }

    pub fn record_persist_success(&mut self, persist_success: PersistSuccess) {
        let Some(subworkbench) = self.subworkbenches.get_mut(&persist_success.subrequest_id) else {
            warn!(
                "could not find subrequest `{}` in workbench",
                persist_success.subrequest_id
            );
            return;
        };
        if let Some(publish_tracker) = &mut self.publish_tracker {
            if let Some(position) = &persist_success.replication_position_inclusive {
                publish_tracker.track_persisted_shard_position(
                    persist_success.shard_id().clone(),
                    position.clone(),
                );
            }
        }
        self.num_successes += 1;
        subworkbench.num_attempts += 1;
        subworkbench.persist_success_opt = Some(persist_success);
    }

    pub fn record_persist_error(
        &mut self,
        persist_error: IngestV2Error,
        persist_summary: PersistRequestSummary,
    ) {
        // Persist responses use dedicated failure reasons for `ShardNotFound` and
        // `TooManyRequests`: in reality, we should never have to handle these cases here.
        match persist_error {
            IngestV2Error::Timeout(_) => {
                for subrequest_id in persist_summary.subrequest_ids {
                    let failure = SubworkbenchFailure::Persist(PersistFailureReason::Timeout);
                    self.record_failure(subrequest_id, failure);
                }
            }
            IngestV2Error::Unavailable(_) => {
                self.unavailable_leaders.insert(persist_summary.leader_id);
                for subrequest_id in persist_summary.subrequest_ids {
                    self.record_ingester_unavailable(subrequest_id);
                }
            }
            IngestV2Error::Internal(internal_err_msg) => {
                rate_limited_error!(limit_per_min=6, err_msg=%internal_err_msg, "persist error: internal error during persist");
                for subrequest_id in persist_summary.subrequest_ids {
                    self.record_internal_error(subrequest_id);
                }
            }
            IngestV2Error::ShardNotFound { shard_id } => {
                rate_limited_error!(limit_per_min=6, shard_id=%shard_id, "persist error: shard not found");
                for subrequest_id in persist_summary.subrequest_ids {
                    self.record_internal_error(subrequest_id);
                }
            }
            IngestV2Error::TooManyRequests(rate_limiting_cause) => {
                for subrequest_id in persist_summary.subrequest_ids {
                    self.record_too_many_requests(subrequest_id, rate_limiting_cause);
                }
            }
        }
    }

    pub fn record_persist_failure(&mut self, persist_failure: &PersistFailure) {
        let failure = SubworkbenchFailure::Persist(persist_failure.reason());
        self.record_failure(persist_failure.subrequest_id, failure);
    }

    fn record_failure(&mut self, subrequest_id: SubrequestId, failure: SubworkbenchFailure) {
        let Some(subworkbench) = self.subworkbenches.get_mut(&subrequest_id) else {
            warn!("could not find subrequest `{}` in workbench", subrequest_id);
            return;
        };
        subworkbench.num_attempts += 1;
        subworkbench.last_failure_opt = Some(failure);
    }

    pub fn record_no_shards_available(&mut self, subrequest_id: SubrequestId) {
        self.record_failure(subrequest_id, SubworkbenchFailure::NoShardsAvailable);
    }

    pub fn record_all_shards_rate_limited(&mut self, subrequest_id: SubrequestId) {
        self.record_failure(
            subrequest_id,
            SubworkbenchFailure::RateLimited(RateLimitingCause::AllShardsRateLimited),
        );
    }

    /// Marks a node as unavailable for the span of the workbench.
    ///
    /// Remaining attempts will treat the node as if it was not in the ingester pool.
    pub fn record_ingester_unavailable(&mut self, subrequest_id: SubrequestId) {
        self.record_failure(subrequest_id, SubworkbenchFailure::Unavailable);
    }

    fn record_internal_error(&mut self, subrequest_id: SubrequestId) {
        self.record_failure(subrequest_id, SubworkbenchFailure::Internal);
    }

    fn record_too_many_requests(
        &mut self,
        subrequest_id: SubrequestId,
        rate_limiting_cause: RateLimitingCause,
    ) {
        self.record_failure(
            subrequest_id,
            SubworkbenchFailure::RateLimited(rate_limiting_cause),
        );
    }

    pub async fn into_ingest_result(self) -> IngestResponseV2 {
        let num_subworkbenches = self.subworkbenches.len();
        let mut successes = Vec::with_capacity(self.num_successes);
        let mut failures = Vec::with_capacity(num_subworkbenches - self.num_successes);

        // We consider the last retry outcome as the actual outcome.
        for subworkbench in self.subworkbenches.into_values() {
            if let Some(persist_success) = subworkbench.persist_success_opt {
                let success = IngestSuccess {
                    subrequest_id: persist_success.subrequest_id,
                    index_uid: persist_success.index_uid,
                    source_id: persist_success.source_id,
                    shard_id: persist_success.shard_id,
                    replication_position_inclusive: persist_success.replication_position_inclusive,
                    num_ingested_docs: persist_success.num_persisted_docs,
                    parse_failures: persist_success.parse_failures,
                };
                successes.push(success);
            } else if let Some(failure) = subworkbench.last_failure_opt {
                let failure = IngestFailure {
                    subrequest_id: subworkbench.subrequest.subrequest_id,
                    index_id: subworkbench.subrequest.index_id,
                    source_id: subworkbench.subrequest.source_id,
                    reason: failure.reason() as i32,
                };
                failures.push(failure);
            }
        }
        let num_successes = successes.len();
        let num_failures = failures.len();
        assert_eq!(num_successes + num_failures, num_subworkbenches);

        if let Some(publish_tracker) = self.publish_tracker {
            publish_tracker.wait_publish_complete().await;
        }

        // For tests, we sort the successes and failures by subrequest_id
        #[cfg(test)]
        {
            for success in &mut successes {
                success
                    .parse_failures
                    .sort_by_key(|parse_failure| parse_failure.doc_uid());
            }
            successes.sort_by_key(|success| success.subrequest_id);
            failures.sort_by_key(|failure| failure.subrequest_id);
        }

        IngestResponseV2 {
            successes,
            failures,
        }
    }
}

#[derive(Debug)]
pub(super) enum SubworkbenchFailure {
    // There is no entry in the routing table for this index.
    IndexNotFound,
    // There is no entry in the routing table for this source.
    SourceNotFound,
    // The routing table entry for this source is empty, shards are all closed, or their leaders
    // are unavailable.
    NoShardsAvailable,
    // This is an error returned by the ingester: e.g. shard not found, shard closed, rate
    // limited, resource exhausted, etc.
    Persist(PersistFailureReason),
    Internal,
    // The ingester is no longer in the pool or a transport error occurred.
    Unavailable,
    // The ingester is rate limited.
    RateLimited(RateLimitingCause),
}

impl SubworkbenchFailure {
    /// Returns the final `IngestFailureReason` returned to the client.
    fn reason(&self) -> IngestFailureReason {
        match self {
            Self::IndexNotFound => IngestFailureReason::IndexNotFound,
            Self::SourceNotFound => IngestFailureReason::SourceNotFound,
            Self::Internal => IngestFailureReason::Internal,
            Self::NoShardsAvailable => IngestFailureReason::NoShardsAvailable,
            // In our last attempt, we did not manage to reach the ingester.
            // We can consider that as a no shards available.
            Self::Unavailable => IngestFailureReason::NoShardsAvailable,
            Self::RateLimited(rate_limiting_cause) => match rate_limiting_cause {
                RateLimitingCause::RouterLoadShedding => IngestFailureReason::RouterLoadShedding,
                RateLimitingCause::LoadShedding => IngestFailureReason::RouterLoadShedding,
                RateLimitingCause::WalFull => IngestFailureReason::WalFull,
                RateLimitingCause::CircuitBreaker => IngestFailureReason::CircuitBreaker,
                RateLimitingCause::AttemptedShardsRateLimited => {
                    IngestFailureReason::AttemptedShardsRateLimited
                }
                RateLimitingCause::AllShardsRateLimited => {
                    IngestFailureReason::AllShardsRateLimited
                }
                RateLimitingCause::Unknown => IngestFailureReason::Unspecified,
            },
            Self::Persist(persist_failure_reason) => (*persist_failure_reason).into(),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct IngestSubworkbench {
    pub subrequest: IngestSubrequest,
    pub persist_success_opt: Option<PersistSuccess>,
    pub last_failure_opt: Option<SubworkbenchFailure>,
    /// The number of persist attempts for this subrequest.
    pub num_attempts: usize,
}

impl IngestSubworkbench {
    pub fn new(subrequest: IngestSubrequest) -> Self {
        Self {
            subrequest,
            ..Default::default()
        }
    }

    pub fn is_pending(&self) -> bool {
        self.persist_success_opt.is_none() && self.last_failure_is_transient()
    }

    /// Returns `false` if and only if the last attempt suggests retrying (on any node) will fail.
    /// e.g.:
    /// - the index does not exist
    /// - the source does not exist.
    fn last_failure_is_transient(&self) -> bool {
        match self.last_failure_opt {
            Some(SubworkbenchFailure::IndexNotFound) => false,
            Some(SubworkbenchFailure::SourceNotFound) => false,
            Some(SubworkbenchFailure::Internal) => true,
            Some(SubworkbenchFailure::NoShardsAvailable) => true,
            Some(SubworkbenchFailure::Persist(_)) => true,
            Some(SubworkbenchFailure::Unavailable) => true,
            Some(SubworkbenchFailure::RateLimited(_)) => true,
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_proto::indexing::ShardPositionsUpdate;
    use quickwit_proto::ingest::ingester::{PersistFailureReason, PersistSubrequest};
    use quickwit_proto::types::{IndexUid, Position, ShardId, SourceUid};

    use super::*;

    #[test]
    fn test_ingest_subworkbench() {
        let subrequest = IngestSubrequest {
            ..Default::default()
        };
        let mut subworkbench = IngestSubworkbench::new(subrequest);
        assert!(subworkbench.is_pending());
        assert!(subworkbench.last_failure_is_transient());

        subworkbench.last_failure_opt = Some(SubworkbenchFailure::Unavailable);
        assert!(subworkbench.is_pending());
        assert!(subworkbench.last_failure_is_transient());

        subworkbench.last_failure_opt = Some(SubworkbenchFailure::Internal);
        assert!(subworkbench.is_pending());
        assert!(subworkbench.last_failure_is_transient());

        subworkbench.last_failure_opt = Some(SubworkbenchFailure::NoShardsAvailable);
        assert!(subworkbench.is_pending());
        assert!(subworkbench.last_failure_is_transient());

        subworkbench.last_failure_opt = Some(SubworkbenchFailure::IndexNotFound);
        assert!(!subworkbench.is_pending());
        assert!(!subworkbench.last_failure_is_transient());
        subworkbench.last_failure_opt = Some(SubworkbenchFailure::SourceNotFound);
        assert!(!subworkbench.is_pending());
        assert!(!subworkbench.last_failure_is_transient());

        subworkbench.last_failure_opt = Some(SubworkbenchFailure::Persist(
            PersistFailureReason::ShardRateLimited,
        ));
        assert!(subworkbench.is_pending());
        assert!(subworkbench.last_failure_is_transient());

        let persist_success = PersistSuccess {
            ..Default::default()
        };
        subworkbench.persist_success_opt = Some(persist_success);
        assert!(!subworkbench.is_pending());
    }

    #[test]
    fn test_ingest_workbench() {
        let workbench = IngestWorkbench::new(Vec::new(), 1);
        assert!(workbench.is_complete());

        let ingest_subrequests = vec![IngestSubrequest {
            ..Default::default()
        }];
        let mut workbench = IngestWorkbench::new(ingest_subrequests, 1);
        assert!(!workbench.is_last_attempt());
        assert!(!workbench.is_complete());

        workbench.new_attempt();
        assert!(workbench.is_last_attempt());
        assert!(workbench.is_complete());

        let ingest_subrequests = vec![
            IngestSubrequest {
                subrequest_id: 0,
                ..Default::default()
            },
            IngestSubrequest {
                subrequest_id: 1,
                ..Default::default()
            },
        ];
        let mut workbench = IngestWorkbench::new(ingest_subrequests, 1);
        assert_eq!(pending_subrequests(&workbench.subworkbenches).count(), 2);
        assert!(!workbench.is_complete());

        let persist_success = PersistSuccess {
            subrequest_id: 0,
            ..Default::default()
        };
        workbench.record_persist_success(persist_success);

        assert_eq!(workbench.num_successes, 1);
        assert_eq!(pending_subrequests(&workbench.subworkbenches).count(), 1);
        assert_eq!(
            pending_subrequests(&workbench.subworkbenches)
                .next()
                .unwrap()
                .subrequest_id,
            1
        );

        let subworkbench = workbench.subworkbenches.get(&0).unwrap();
        assert_eq!(subworkbench.num_attempts, 1);
        assert!(!subworkbench.is_pending());

        let persist_failure = PersistFailure {
            subrequest_id: 1,
            ..Default::default()
        };
        workbench.record_persist_failure(&persist_failure);

        assert_eq!(workbench.num_successes, 1);
        assert_eq!(pending_subrequests(&workbench.subworkbenches).count(), 1);
        assert_eq!(
            pending_subrequests(&workbench.subworkbenches)
                .next()
                .unwrap()
                .subrequest_id,
            1
        );

        let subworkbench = workbench.subworkbenches.get(&1).unwrap();
        assert_eq!(subworkbench.num_attempts, 1);
        assert!(subworkbench.last_failure_opt.is_some());

        let persist_success = PersistSuccess {
            subrequest_id: 1,
            ..Default::default()
        };
        workbench.record_persist_success(persist_success);

        assert!(workbench.is_complete());
        assert_eq!(workbench.num_successes, 2);
        assert_eq!(pending_subrequests(&workbench.subworkbenches).count(), 0);
    }

    #[tokio::test]
    async fn test_workbench_publish_tracking_empty() {
        let workbench =
            IngestWorkbench::new_with_publish_tracking(Vec::new(), 1, EventBroker::default());
        assert!(workbench.is_complete());
        assert_eq!(
            workbench.into_ingest_result().await,
            IngestResponseV2::default()
        );
    }

    #[tokio::test]
    async fn test_workbench_publish_tracking_happy_path() {
        let event_broker = EventBroker::default();
        let shard_id_1 = ShardId::from("test-shard-1");
        let shard_id_2 = ShardId::from("test-shard-2");
        let ingest_subrequests = vec![
            IngestSubrequest {
                subrequest_id: 0,
                ..Default::default()
            },
            IngestSubrequest {
                subrequest_id: 1,
                ..Default::default()
            },
        ];
        let mut workbench =
            IngestWorkbench::new_with_publish_tracking(ingest_subrequests, 1, event_broker.clone());
        assert_eq!(pending_subrequests(&workbench.subworkbenches).count(), 2);
        assert!(!workbench.is_complete());

        let persist_request = PersistRequest {
            subrequests: vec![
                PersistSubrequest {
                    subrequest_id: 0,
                    shard_id: Some(shard_id_1.clone()),
                    ..Default::default()
                },
                PersistSubrequest {
                    subrequest_id: 1,
                    shard_id: Some(shard_id_2.clone()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        workbench.record_persist_request(&persist_request);

        let persist_success = PersistSuccess {
            subrequest_id: 0,
            shard_id: Some(shard_id_1.clone()),
            replication_position_inclusive: Some(Position::offset(42usize)),
            ..Default::default()
        };
        workbench.record_persist_success(persist_success);

        let persist_failure = PersistFailure {
            subrequest_id: 1,
            shard_id: Some(shard_id_2.clone()),
            ..Default::default()
        };
        workbench.record_persist_failure(&persist_failure);

        let persist_success = PersistSuccess {
            subrequest_id: 1,
            shard_id: Some(shard_id_2.clone()),
            replication_position_inclusive: Some(Position::offset(66usize)),
            ..Default::default()
        };

        // retry to persist shard 2

        let persist_request = PersistRequest {
            subrequests: vec![PersistSubrequest {
                subrequest_id: 1,
                shard_id: Some(shard_id_2.clone()),
                ..Default::default()
            }],
            ..Default::default()
        };
        workbench.record_persist_request(&persist_request);

        workbench.record_persist_success(persist_success);

        assert!(workbench.is_complete());
        assert_eq!(workbench.num_successes, 2);
        assert_eq!(pending_subrequests(&workbench.subworkbenches).count(), 0);

        event_broker.publish(ShardPositionsUpdate {
            source_uid: SourceUid {
                index_uid: IndexUid::for_test("test-index", 0),
                source_id: "test-source".to_string(),
            },
            updated_shard_positions: vec![
                (shard_id_1, Position::offset(42usize)),
                (shard_id_2, Position::offset(66usize)),
            ]
            .into_iter()
            .collect(),
        });

        let ingest_response = workbench.into_ingest_result().await;
        assert_eq!(ingest_response.successes.len(), 2);
        assert_eq!(ingest_response.failures.len(), 0);
    }

    #[tokio::test]
    async fn test_workbench_publish_tracking_waits() {
        let event_broker = EventBroker::default();
        let shard_id_1 = ShardId::from("test-shard-1");
        let shard_id_2 = ShardId::from("test-shard-2");
        let ingest_subrequests = vec![
            IngestSubrequest {
                subrequest_id: 0,
                ..Default::default()
            },
            IngestSubrequest {
                subrequest_id: 1,
                ..Default::default()
            },
        ];
        let mut workbench =
            IngestWorkbench::new_with_publish_tracking(ingest_subrequests, 1, event_broker.clone());

        let persist_request = PersistRequest {
            subrequests: vec![
                PersistSubrequest {
                    subrequest_id: 0,
                    shard_id: Some(shard_id_1.clone()),
                    ..Default::default()
                },
                PersistSubrequest {
                    subrequest_id: 1,
                    shard_id: Some(shard_id_2.clone()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        workbench.record_persist_request(&persist_request);

        let persist_success = PersistSuccess {
            subrequest_id: 0,
            shard_id: Some(shard_id_1.clone()),
            replication_position_inclusive: Some(Position::offset(42usize)),
            ..Default::default()
        };
        workbench.record_persist_success(persist_success);

        let persist_success = PersistSuccess {
            subrequest_id: 1,
            shard_id: Some(shard_id_2.clone()),
            replication_position_inclusive: Some(Position::offset(66usize)),
            ..Default::default()
        };
        workbench.record_persist_success(persist_success);

        assert!(workbench.is_complete());
        assert_eq!(workbench.num_successes, 2);
        assert_eq!(pending_subrequests(&workbench.subworkbenches).count(), 0);

        event_broker.publish(ShardPositionsUpdate {
            source_uid: SourceUid {
                index_uid: IndexUid::for_test("test-index", 0),
                source_id: "test-source".to_string(),
            },
            updated_shard_positions: vec![(shard_id_2, Position::offset(66usize))]
                .into_iter()
                .collect(),
        });
        // still waits for shard 1 to be published
        tokio::time::timeout(Duration::from_millis(200), workbench.into_ingest_result())
            .await
            .unwrap_err();
    }

    #[test]
    fn test_ingest_workbench_record_get_or_create_open_shards_failure() {
        let ingest_subrequests = vec![IngestSubrequest {
            subrequest_id: 0,
            ..Default::default()
        }];
        let mut workbench = IngestWorkbench::new(ingest_subrequests, 1);

        let get_or_create_open_shards_failure = GetOrCreateOpenShardsFailure {
            subrequest_id: 42,
            reason: GetOrCreateOpenShardsFailureReason::IndexNotFound as i32,
            ..Default::default()
        };
        workbench.record_get_or_create_open_shards_failure(get_or_create_open_shards_failure);

        let get_or_create_open_shards_failure = GetOrCreateOpenShardsFailure {
            subrequest_id: 0,
            reason: GetOrCreateOpenShardsFailureReason::SourceNotFound as i32,
            ..Default::default()
        };
        workbench.record_get_or_create_open_shards_failure(get_or_create_open_shards_failure);

        assert_eq!(workbench.num_successes, 0);

        let subworkbench = workbench.subworkbenches.get(&0).unwrap();
        assert!(matches!(
            subworkbench.last_failure_opt,
            Some(SubworkbenchFailure::SourceNotFound)
        ));
        assert_eq!(subworkbench.num_attempts, 1);
    }

    #[test]
    fn test_ingest_workbench_record_persist_success() {
        let ingest_subrequests = vec![IngestSubrequest {
            subrequest_id: 0,
            ..Default::default()
        }];
        let mut workbench = IngestWorkbench::new(ingest_subrequests, 1);

        let persist_success = PersistSuccess {
            subrequest_id: 42,
            ..Default::default()
        };
        workbench.record_persist_success(persist_success);

        let persist_success = PersistSuccess {
            subrequest_id: 0,
            ..Default::default()
        };
        workbench.record_persist_success(persist_success);

        assert_eq!(workbench.num_successes, 1);

        let subworkbench = workbench.subworkbenches.get(&0).unwrap();
        assert!(matches!(
            subworkbench.persist_success_opt,
            Some(PersistSuccess { .. })
        ));
        assert_eq!(subworkbench.num_attempts, 1);
    }

    #[test]
    fn test_ingest_workbench_record_persist_error_timeout() {
        let ingest_subrequests = vec![IngestSubrequest {
            subrequest_id: 0,
            ..Default::default()
        }];
        let mut workbench = IngestWorkbench::new(ingest_subrequests, 1);

        let persist_error = IngestV2Error::Timeout("request timed out".to_string());
        let leader_id = NodeId::from("test-leader");
        let persist_summary = PersistRequestSummary {
            leader_id: leader_id.clone(),
            subrequest_ids: vec![0],
        };
        workbench.record_persist_error(persist_error, persist_summary);

        let subworkbench = workbench.subworkbenches.get(&0).unwrap();
        assert_eq!(subworkbench.num_attempts, 1);

        assert!(matches!(
            subworkbench.last_failure_opt,
            Some(SubworkbenchFailure::Persist(PersistFailureReason::Timeout))
        ));
        assert!(subworkbench.persist_success_opt.is_none());
    }

    #[test]
    fn test_ingest_workbench_record_persist_error_unavailable() {
        let ingest_subrequests = vec![IngestSubrequest {
            subrequest_id: 0,
            ..Default::default()
        }];
        let mut workbench = IngestWorkbench::new(ingest_subrequests, 1);

        let persist_error = IngestV2Error::Unavailable("connection error".to_string());
        let leader_id = NodeId::from("test-leader");
        let persist_summary = PersistRequestSummary {
            leader_id: leader_id.clone(),
            subrequest_ids: vec![0],
        };
        workbench.record_persist_error(persist_error, persist_summary);

        assert!(workbench.unavailable_leaders.contains(&leader_id));

        let subworkbench = workbench.subworkbenches.get(&0).unwrap();
        assert_eq!(subworkbench.num_attempts, 1);

        assert!(matches!(
            subworkbench.last_failure_opt,
            Some(SubworkbenchFailure::Unavailable)
        ));
        assert!(subworkbench.persist_success_opt.is_none());
    }

    #[test]
    fn test_ingest_workbench_record_persist_error_internal() {
        let ingest_subrequests = vec![IngestSubrequest {
            subrequest_id: 0,
            ..Default::default()
        }];
        let mut workbench = IngestWorkbench::new(ingest_subrequests, 1);

        let persist_error = IngestV2Error::Internal("IO error".to_string());
        let persist_summary = PersistRequestSummary {
            leader_id: NodeId::from("test-leader"),
            subrequest_ids: vec![0],
        };
        workbench.record_persist_error(persist_error, persist_summary);

        let subworkbench = workbench.subworkbenches.get(&0).unwrap();
        assert_eq!(subworkbench.num_attempts, 1);

        assert!(matches!(
            &subworkbench.last_failure_opt,
            Some(SubworkbenchFailure::Internal)
        ));
        assert!(subworkbench.persist_success_opt.is_none());
    }

    #[test]
    fn test_ingest_workbench_record_persist_failure() {
        let ingest_subrequests = vec![IngestSubrequest {
            subrequest_id: 0,
            ..Default::default()
        }];
        let mut workbench = IngestWorkbench::new(ingest_subrequests, 1);

        let persist_failure = PersistFailure {
            subrequest_id: 42,
            reason: PersistFailureReason::ShardRateLimited as i32,
            ..Default::default()
        };
        workbench.record_persist_failure(&persist_failure);

        let persist_failure = PersistFailure {
            subrequest_id: 0,
            shard_id: Some(ShardId::from(1)),
            reason: PersistFailureReason::WalFull as i32,
            ..Default::default()
        };
        workbench.record_persist_failure(&persist_failure);

        assert_eq!(workbench.num_successes, 0);

        let subworkbench = workbench.subworkbenches.get(&0).unwrap();
        assert!(matches!(
            subworkbench.last_failure_opt,
            Some(SubworkbenchFailure::Persist(reason)) if reason == PersistFailureReason::WalFull
        ));
        assert_eq!(subworkbench.num_attempts, 1);
    }

    #[test]
    fn test_ingest_workbench_record_no_shards_available() {
        let ingest_subrequests = vec![IngestSubrequest {
            subrequest_id: 0,
            ..Default::default()
        }];
        let mut workbench = IngestWorkbench::new(ingest_subrequests, 1);

        workbench.record_no_shards_available(42);
        workbench.record_no_shards_available(0);

        assert_eq!(workbench.num_successes, 0);

        let subworkbench = workbench.subworkbenches.get(&0).unwrap();
        assert!(matches!(
            subworkbench.last_failure_opt,
            Some(SubworkbenchFailure::NoShardsAvailable)
        ));
        assert_eq!(subworkbench.num_attempts, 1);
    }

    #[tokio::test]
    async fn test_ingest_workbench_into_ingest_result() {
        let workbench = IngestWorkbench::new(Vec::new(), 0);
        let response = workbench.into_ingest_result().await;
        assert!(response.successes.is_empty());
        assert!(response.failures.is_empty());

        let ingest_subrequests = vec![
            IngestSubrequest {
                subrequest_id: 0,
                ..Default::default()
            },
            IngestSubrequest {
                subrequest_id: 1,
                ..Default::default()
            },
        ];
        let mut workbench = IngestWorkbench::new(ingest_subrequests, 1);
        let persist_success = PersistSuccess {
            ..Default::default()
        };
        let subworkbench = workbench.subworkbenches.get_mut(&0).unwrap();
        subworkbench.persist_success_opt = Some(persist_success);

        workbench.record_no_shards_available(1);

        let response = workbench.into_ingest_result().await;
        assert_eq!(response.successes.len(), 1);
        assert_eq!(response.successes[0].subrequest_id, 0);

        assert_eq!(response.failures.len(), 1);
        assert_eq!(response.failures[0].subrequest_id, 1);
        assert_eq!(
            response.failures[0].reason(),
            IngestFailureReason::NoShardsAvailable
        );

        let ingest_subrequests = vec![IngestSubrequest {
            subrequest_id: 0,
            ..Default::default()
        }];
        let mut workbench = IngestWorkbench::new(ingest_subrequests, 1);
        let failure = SubworkbenchFailure::Persist(PersistFailureReason::Timeout);
        workbench.record_failure(0, failure);

        let ingest_response = workbench.into_ingest_result().await;
        assert_eq!(ingest_response.successes.len(), 0);
        assert_eq!(
            ingest_response.failures[0].reason(),
            IngestFailureReason::Timeout
        );
    }
}
