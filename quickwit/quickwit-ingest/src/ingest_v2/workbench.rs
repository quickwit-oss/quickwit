// Copyright (C) 2023 Quickwit, Inc.
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

use std::collections::HashMap;

use quickwit_proto::control_plane::{
    GetOrCreateOpenShardsFailure, GetOrCreateOpenShardsFailureReason,
};
use quickwit_proto::ingest::ingester::{PersistFailure, PersistFailureReason, PersistSuccess};
use quickwit_proto::ingest::router::{
    IngestFailure, IngestFailureReason, IngestResponseV2, IngestSubrequest, IngestSuccess,
};
use quickwit_proto::ingest::IngestV2Result;
use quickwit_proto::types::SubrequestId;
use tracing::warn;

/// A helper struct for managing the state of the subrequests of an ingest request during multiple
/// persist attempts.
#[derive(Default)]
pub(super) struct IngestWorkbench {
    pub subworkbenches: HashMap<SubrequestId, IngestSubworkbench>,
    pub num_successes: usize,
    /// The number of batch persist attempts. This is not sum of the number of attempts for each
    /// subrequest.
    pub num_attempts: usize,
    pub max_num_attempts: usize,
}

impl IngestWorkbench {
    pub fn new(ingest_subrequests: Vec<IngestSubrequest>, max_num_attempts: usize) -> Self {
        let subworkbenches: HashMap<SubrequestId, IngestSubworkbench> = ingest_subrequests
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
        let Some(subworkbench) = self
            .subworkbenches
            .get_mut(&open_shards_failure.subrequest_id)
        else {
            warn!(
                "could not find subrequest `{}` in workbench",
                open_shards_failure.subrequest_id
            );
            return;
        };
        subworkbench.num_attempts += 1;

        let last_failure = match open_shards_failure.reason() {
            GetOrCreateOpenShardsFailureReason::IndexNotFound => SubworkbenchFailure::IndexNotFound,
            GetOrCreateOpenShardsFailureReason::SourceNotFound => {
                SubworkbenchFailure::SourceNotFound
            }
            GetOrCreateOpenShardsFailureReason::Unspecified => {
                warn!(
                    "failure reason for subrequest `{}` is unspecified",
                    open_shards_failure.subrequest_id
                );
                SubworkbenchFailure::Internal("unspecified".to_string())
            }
        };
        subworkbench.last_failure_opt = Some(last_failure);
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

    pub fn record_persist_failure(&mut self, persist_failure: PersistFailure) {
        let Some(subworkbench) = self.subworkbenches.get_mut(&persist_failure.subrequest_id) else {
            warn!(
                "could not find subrequest `{}` in workbench",
                persist_failure.subrequest_id
            );
            return;
        };
        subworkbench.num_attempts += 1;
        subworkbench.last_failure_opt = Some(SubworkbenchFailure::Persist(persist_failure));
    }

    pub fn record_no_shards_available(&mut self, subrequest_id: SubrequestId) {
        let Some(subworkbench) = self.subworkbenches.get_mut(&subrequest_id) else {
            warn!("could not find subrequest `{}` in workbench", subrequest_id);
            return;
        };
        subworkbench.num_attempts += 1;
        subworkbench.last_failure_opt = Some(SubworkbenchFailure::NoShardsAvailable);
    }

    pub fn record_internal_error(&mut self, subrequest_id: SubrequestId, error_message: String) {
        let Some(subworkbench) = self.subworkbenches.get_mut(&subrequest_id) else {
            warn!("could not find subrequest `{}` in workbench", subrequest_id);
            return;
        };
        subworkbench.num_attempts += 1;
        subworkbench.last_failure_opt = Some(SubworkbenchFailure::Internal(error_message));
    }

    pub fn into_ingest_response(self) -> IngestV2Result<IngestResponseV2> {
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
        assert!(successes.len() + failures.len() == num_subworkbenches);

        Ok(IngestResponseV2 {
            successes,
            failures,
        })
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
    IndexNotFound,
    SourceNotFound,
    NoShardsAvailable,
    Persist(PersistFailure),
    Internal(String),
}

impl SubworkbenchFailure {
    fn reason(&self) -> IngestFailureReason {
        match self {
            Self::IndexNotFound => IngestFailureReason::IndexNotFound,
            Self::SourceNotFound => IngestFailureReason::SourceNotFound,
            Self::Internal(_) => IngestFailureReason::Internal,
            Self::NoShardsAvailable => IngestFailureReason::NoShardsAvailable,
            Self::Persist(persist_failure) => match persist_failure.reason() {
                PersistFailureReason::RateLimited => IngestFailureReason::RateLimited,
                PersistFailureReason::ResourceExhausted => IngestFailureReason::ResourceExhausted,
                PersistFailureReason::ShardClosed => IngestFailureReason::NoShardsAvailable,
                PersistFailureReason::Unspecified => IngestFailureReason::Unspecified,
            },
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

    fn last_failure_is_transient(&self) -> bool {
        match self.last_failure_opt {
            Some(SubworkbenchFailure::IndexNotFound) => false,
            Some(SubworkbenchFailure::SourceNotFound) => false,
            Some(SubworkbenchFailure::Internal(_)) => false,
            Some(SubworkbenchFailure::NoShardsAvailable) => true,
            Some(SubworkbenchFailure::Persist(_)) => true,
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ingest_subworkbench() {
        let subrequest = IngestSubrequest {
            ..Default::default()
        };
        let mut subworkbench = IngestSubworkbench::new(subrequest);
        assert!(subworkbench.is_pending());
        assert!(subworkbench.last_failure_is_transient());

        subworkbench.last_failure_opt = Some(SubworkbenchFailure::NoShardsAvailable);
        assert!(subworkbench.is_pending());
        assert!(subworkbench.last_failure_is_transient());

        subworkbench.last_failure_opt = Some(SubworkbenchFailure::IndexNotFound);
        assert!(!subworkbench.is_pending());
        assert!(!subworkbench.last_failure_is_transient());

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
        workbench.record_persist_failure(persist_failure);

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
        workbench.record_persist_failure(persist_failure);

        let persist_failure = PersistFailure {
            subrequest_id: 0,
            reason: PersistFailureReason::ResourceExhausted as i32,
            ..Default::default()
        };
        workbench.record_persist_failure(persist_failure);

        assert_eq!(workbench.num_successes, 0);

        let subworkbench = workbench.subworkbenches.get(&0).unwrap();
        assert!(matches!(
            subworkbench.last_failure_opt,
            Some(SubworkbenchFailure::Persist ( PersistFailure { reason, .. })) if reason == PersistFailureReason::ResourceExhausted as i32
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
}
