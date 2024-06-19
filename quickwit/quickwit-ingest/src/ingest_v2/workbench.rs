// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::{BTreeMap, HashSet};

use quickwit_proto::control_plane::{
    GetOrCreateOpenShardsFailure, GetOrCreateOpenShardsFailureReason,
};
use quickwit_proto::ingest::ingester::{PersistFailure, PersistFailureReason, PersistSuccess};
use quickwit_proto::ingest::router::{
    IngestFailure, IngestFailureReason, IngestResponseV2, IngestSubrequest, IngestSuccess,
};
use quickwit_proto::ingest::{IngestV2Error, IngestV2Result};
use quickwit_proto::types::{NodeId, SubrequestId};
use tracing::warn;

use super::router::PersistRequestSummary;

/// A helper struct for managing the state of the subrequests of an ingest request during multiple
/// persist attempts.
#[derive(Default)]
pub(super) struct IngestWorkbench {
    pub subworkbenches: BTreeMap<SubrequestId, IngestSubworkbench>,
    pub num_successes: usize,
    /// The number of batch persist attempts. This is not sum of the number of attempts for each
    /// subrequest.
    pub num_attempts: usize,
    pub max_num_attempts: usize,
    // List of leaders that have been marked as temporarily unavailable.
    // These leaders have encountered a transport error during an attempt and will be treated as if
    // they were out of the pool for subsequent attempts.
    //
    // (The point here is to make sure we do not wait for the failure detection to kick the node
    // out of the ingest node.)
    pub unavailable_leaders: HashSet<NodeId>,
}

impl IngestWorkbench {
    pub fn new(ingest_subrequests: Vec<IngestSubrequest>, max_num_attempts: usize) -> Self {
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
            ..Default::default()
        }
    }

    pub fn new_attempt(&mut self) {
        self.num_attempts += 1;
    }

    /// Returns true if all subrequests were successful or if the number of
    /// attempts has been exhausted.
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

    #[cfg(not(test))]
    pub fn pending_subrequests(&self) -> impl Iterator<Item = &IngestSubrequest> {
        self.subworkbenches.values().filter_map(|subworbench| {
            if subworbench.is_pending() {
                Some(&subworbench.subrequest)
            } else {
                None
            }
        })
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
            IngestV2Error::Internal(_)
            | IngestV2Error::ShardNotFound { .. }
            | IngestV2Error::TooManyRequests => {
                for subrequest_id in persist_summary.subrequest_ids {
                    self.record_internal_error(subrequest_id);
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

    /// Marks a node as unavailable for the span of the workbench.
    ///
    /// Remaining attempts will treat the node as if it was not in the ingester pool.
    pub fn record_ingester_unavailable(&mut self, subrequest_id: SubrequestId) {
        self.record_failure(subrequest_id, SubworkbenchFailure::Unavailable);
    }

    fn record_internal_error(&mut self, subrequest_id: SubrequestId) {
        self.record_failure(subrequest_id, SubworkbenchFailure::Internal);
    }

    pub fn into_ingest_result(self) -> IngestV2Result<IngestResponseV2> {
        let num_subworkbenches = self.subworkbenches.len();
        let mut successes = Vec::with_capacity(self.num_successes);
        let mut failures = Vec::with_capacity(num_subworkbenches - self.num_successes);

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
        if self.num_successes == 0
            && num_failures > 0
            && failures.iter().all(|failure| {
                matches!(
                    failure.reason(),
                    IngestFailureReason::RateLimited
                        | IngestFailureReason::ResourceExhausted
                        | IngestFailureReason::Timeout
                )
            })
        {
            return Err(IngestV2Error::TooManyRequests);
        }
        let response = IngestResponseV2 {
            successes,
            failures,
        };
        Ok(response)
    }

    #[cfg(test)]
    pub fn pending_subrequests(&self) -> impl Iterator<Item = &IngestSubrequest> {
        use itertools::Itertools;

        self.subworkbenches
            .values()
            .filter_map(|subworbench| {
                if subworbench.is_pending() {
                    Some(&subworbench.subrequest)
                } else {
                    None
                }
            })
            .sorted_by_key(|subrequest| subrequest.subrequest_id)
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

    /// Returns `false` if and only if the last attempt suggests retrying will fail.
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
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::ingest::ingester::PersistFailureReason;
    use quickwit_proto::types::ShardId;

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
            PersistFailureReason::RateLimited,
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
        assert_eq!(workbench.pending_subrequests().count(), 2);
        assert!(!workbench.is_complete());

        let persist_success = PersistSuccess {
            subrequest_id: 0,
            ..Default::default()
        };
        workbench.record_persist_success(persist_success);

        assert_eq!(workbench.num_successes, 1);
        assert_eq!(workbench.pending_subrequests().count(), 1);
        assert_eq!(
            workbench
                .pending_subrequests()
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
        assert_eq!(workbench.pending_subrequests().count(), 1);
        assert_eq!(
            workbench
                .pending_subrequests()
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
        assert_eq!(workbench.pending_subrequests().count(), 0);
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
            reason: PersistFailureReason::RateLimited as i32,
            ..Default::default()
        };
        workbench.record_persist_failure(&persist_failure);

        let persist_failure = PersistFailure {
            subrequest_id: 0,
            shard_id: Some(ShardId::from(1)),
            reason: PersistFailureReason::ResourceExhausted as i32,
            ..Default::default()
        };
        workbench.record_persist_failure(&persist_failure);

        assert_eq!(workbench.num_successes, 0);

        let subworkbench = workbench.subworkbenches.get(&0).unwrap();
        assert!(matches!(
            subworkbench.last_failure_opt,
            Some(SubworkbenchFailure::Persist(reason)) if reason == PersistFailureReason::ResourceExhausted
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

    #[test]
    fn test_ingest_workbench_into_ingest_result() {
        let workbench = IngestWorkbench::new(Vec::new(), 0);
        let response = workbench.into_ingest_result().unwrap();
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

        let response = workbench.into_ingest_result().unwrap();
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

        let error = workbench.into_ingest_result().unwrap_err();
        assert_eq!(error, IngestV2Error::TooManyRequests);
    }
}
