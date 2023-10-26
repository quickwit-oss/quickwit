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

use quickwit_proto::ingest::ingester::{PersistFailure, PersistSuccess};
use quickwit_proto::ingest::router::{
    IngestFailure, IngestFailureReason, IngestResponseV2, IngestSubrequest, IngestSuccess,
};
use quickwit_proto::ingest::IngestV2Result;
use quickwit_proto::types::SubrequestId;
use tracing::warn;

/// A helper struct for managing the state of the subrequests of an ingest request during multiple
/// persist attempts.
pub(super) struct IngestWorkbench {
    subworkbenches: HashMap<SubrequestId, IngestSubworkbench>,
    num_successes: usize,
}

impl IngestWorkbench {
    pub fn new(subrequests: Vec<IngestSubrequest>) -> Self {
        let subworkbenches = subrequests
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
            num_successes: 0,
        }
    }

    #[cfg(not(test))]
    pub fn pending_subrequests(&self) -> impl Iterator<Item = &IngestSubrequest> {
        self.subworkbenches.values().filter_map(|subworbench| {
            if !subworbench.is_success() {
                Some(&subworbench.subrequest)
            } else {
                None
            }
        })
    }

    pub fn record_success(&mut self, persist_success: PersistSuccess) {
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

    pub fn record_failure(&mut self, persist_failure: PersistFailure) {
        let Some(subworkbench) = self.subworkbenches.get_mut(&persist_failure.subrequest_id) else {
            warn!(
                "could not find subrequest `{}` in workbench",
                persist_failure.subrequest_id
            );
            return;
        };
        subworkbench.num_attempts += 1;
        subworkbench.last_attempt_failure_opt = Some(SubworkbenchFailure::Persist(persist_failure));
    }

    pub fn record_no_shards_available(&mut self, subrequest_id: SubrequestId) {
        let Some(subworkbench) = self.subworkbenches.get_mut(&subrequest_id) else {
            warn!("could not find subrequest `{}` in workbench", subrequest_id);
            return;
        };
        subworkbench.num_attempts += 1;
        subworkbench.last_attempt_failure_opt = Some(SubworkbenchFailure::NoShardsAvailable);
    }

    pub fn is_complete(&self) -> bool {
        self.num_successes == self.subworkbenches.len()
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
            } else if let Some(failure) = subworkbench.last_attempt_failure_opt {
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
                if !subworbench.is_success() {
                    Some(&subworbench.subrequest)
                } else {
                    None
                }
            })
            .sorted_by_key(|subrequest| subrequest.subrequest_id)
    }
}

#[derive(Debug)]
enum SubworkbenchFailure {
    NoShardsAvailable,
    Persist(PersistFailure),
}

impl SubworkbenchFailure {
    fn reason(&self) -> IngestFailureReason {
        // TODO: Return a better failure reason for `Self::Persist`.
        match self {
            Self::NoShardsAvailable => IngestFailureReason::NoShardsAvailable,
            Self::Persist(_) => IngestFailureReason::Unspecified,
        }
    }
}

pub(super) struct IngestSubworkbench {
    subrequest: IngestSubrequest,
    persist_success_opt: Option<PersistSuccess>,
    last_attempt_failure_opt: Option<SubworkbenchFailure>,
    num_attempts: usize,
}

impl IngestSubworkbench {
    pub fn new(subrequest: IngestSubrequest) -> Self {
        Self {
            subrequest,
            persist_success_opt: None,
            last_attempt_failure_opt: None,
            num_attempts: 0,
        }
    }

    pub fn is_success(&self) -> bool {
        self.persist_success_opt.is_some()
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
        assert!(!subworkbench.is_success());

        let persist_success = PersistSuccess {
            ..Default::default()
        };
        subworkbench.persist_success_opt = Some(persist_success);
        assert!(subworkbench.is_success());
    }

    #[test]
    fn test_ingest_workbench() {
        let subrequests = vec![
            IngestSubrequest {
                subrequest_id: 0,
                ..Default::default()
            },
            IngestSubrequest {
                subrequest_id: 1,
                ..Default::default()
            },
        ];
        let mut workbench = IngestWorkbench::new(subrequests);
        assert_eq!(workbench.pending_subrequests().count(), 2);
        assert!(!workbench.is_complete());

        let persist_success = PersistSuccess {
            subrequest_id: 0,
            ..Default::default()
        };
        workbench.record_success(persist_success);

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
        assert!(subworkbench.is_success());

        let persist_failure = PersistFailure {
            subrequest_id: 1,
            ..Default::default()
        };
        workbench.record_failure(persist_failure);

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
        assert!(subworkbench.last_attempt_failure_opt.is_some());

        let persist_success = PersistSuccess {
            subrequest_id: 1,
            ..Default::default()
        };
        workbench.record_success(persist_success);

        assert!(workbench.is_complete());
        assert_eq!(workbench.num_successes, 2);
        assert_eq!(workbench.pending_subrequests().count(), 0);
    }
}
