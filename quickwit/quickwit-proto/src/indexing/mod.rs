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

use std::fmt;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::ops::{Add, Mul, Sub};

use bytesize::ByteSize;
use quickwit_actors::AskError;
use quickwit_common::pubsub::Event;
use quickwit_common::rate_limited_error;
use quickwit_common::tower::{MakeLoadShedError, RpcName, TimeoutExceeded};
use serde::{Deserialize, Serialize};
use thiserror;

use crate::metastore::MetastoreError;
use crate::types::{IndexUid, NodeId, PipelineUid, Position, ShardId, SourceId, SourceUid};
use crate::{GrpcServiceError, ServiceError, ServiceErrorCode};

include!("../codegen/quickwit/quickwit.indexing.rs");

pub const INDEXING_FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!("../codegen/quickwit/indexing_descriptor.bin");

pub type IndexingResult<T> = std::result::Result<T, IndexingError>;

#[derive(Debug, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexingError {
    #[error("internal error: {0}")]
    Internal(String),
    #[error("metastore error: {0}")]
    Metastore(#[from] MetastoreError),
    #[error("request timed out: {0}")]
    Timeout(String),
    #[error("too many requests")]
    TooManyRequests,
    #[error("service unavailable: {0}")]
    Unavailable(String),
}
impl From<TimeoutExceeded> for IndexingError {
    fn from(_timeout_exceeded: TimeoutExceeded) -> Self {
        Self::Timeout("tower layer timeout".to_string())
    }
}

impl ServiceError for IndexingError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::Internal(err_msg) => {
                rate_limited_error!(limit_per_min = 6, "indexing error: {err_msg}");
                ServiceErrorCode::Internal
            }
            Self::Metastore(metastore_error) => metastore_error.error_code(),
            Self::Timeout(_) => ServiceErrorCode::Timeout,
            Self::TooManyRequests => ServiceErrorCode::TooManyRequests,
            Self::Unavailable(_) => ServiceErrorCode::Unavailable,
        }
    }
}

impl GrpcServiceError for IndexingError {
    fn new_internal(message: String) -> Self {
        Self::Internal(message)
    }

    fn new_timeout(message: String) -> Self {
        Self::Timeout(message)
    }

    fn new_too_many_requests() -> Self {
        Self::TooManyRequests
    }

    fn new_unavailable(message: String) -> Self {
        Self::Unavailable(message)
    }
}

impl MakeLoadShedError for IndexingError {
    fn make_load_shed_error() -> Self {
        Self::TooManyRequests
    }
}

impl From<AskError<IndexingError>> for IndexingError {
    fn from(error: AskError<IndexingError>) -> Self {
        match error {
            AskError::ErrorReply(error) => error,
            AskError::MessageNotDelivered => {
                Self::new_unavailable("request could not be delivered to actor".to_string())
            }
            AskError::ProcessMessageError => {
                Self::new_internal("an error occurred while processing the request".to_string())
            }
        }
    }
}

/// Uniquely identifies an indexing pipeline. There can be multiple indexing pipelines per
/// source `(index_uid, source_id)` running simultaneously on an indexer.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct IndexingPipelineId {
    pub node_id: NodeId,
    pub index_uid: IndexUid,
    pub source_id: SourceId,
    pub pipeline_uid: PipelineUid,
}

impl IndexingPipelineId {
    pub fn merge_pipeline_id(&self) -> MergePipelineId {
        MergePipelineId {
            node_id: self.node_id.clone(),
            index_uid: self.index_uid.clone(),
            source_id: self.source_id.clone(),
        }
    }
}

impl Display for IndexingPipelineId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.index_uid, &self.source_id)
    }
}

/// Uniquely identifies a merge pipeline. There exists at most one merge pipeline per
/// `(index_uid, source_id)` running on indexer at any given time fed by one or more indexing
/// pipelines.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct MergePipelineId {
    pub node_id: NodeId,
    pub index_uid: IndexUid,
    pub source_id: SourceId,
}

impl Display for MergePipelineId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "merge:{}:{}", self.index_uid, &self.source_id)
    }
}

impl Display for IndexingTask {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.index_uid(), &self.source_id)
    }
}

impl Eq for IndexingTask {}

// TODO: This implementation conflicts with the default derived implementation. It would be better
// to use a wrapper over `IndexingTask` where we need to group indexing tasks by index UID and
// source ID.
impl Hash for IndexingTask {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.index_uid.hash(state);
        self.source_id.hash(state);
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct PipelineMetrics {
    pub cpu_load: CpuCapacity,
    // Indexing throughput (when the CPU is working).
    // This measure the theoretical maximum number of MB/s a full indexing pipeline could process
    // provided enough data was being ingested.
    pub throughput_mb_per_sec: u16,
}

impl Display for PipelineMetrics {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{},{}MB/s", self.cpu_load, self.throughput_mb_per_sec)
    }
}

/// One full pipeline (including merging) is assumed to consume 4 CPU threads.
/// The actual number somewhere between 3 and 4. Quickwit is not super sensitive to this number.
///
/// It simply impacts the point where we prefer to work on balancing the load over the different
/// indexers and the point where we prefer improving other feature of the system (shard locality,
/// grouping pipelines associated to a given index on the same node, etc.).
pub const PIPELINE_FULL_CAPACITY: CpuCapacity = CpuCapacity::from_cpu_millis(4_000u32);

/// One full pipeline (including merging) is supposed to have the capacity to index at least 20mb/s.
/// This is a defensive value: In reality, this is typically above 30mb/s.
pub const PIPELINE_THROUGHPUT: ByteSize = ByteSize::mb(20);

/// The CpuCapacity represents an amount of CPU resource available.
///
/// It is usually expressed in CPU millis (For instance, one full CPU thread is
/// displayed as `1000m`).
#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Deserialize, Serialize, Ord, PartialOrd, utoipa::ToSchema,
)]
#[serde(
    into = "CpuCapacityForSerialization",
    try_from = "CpuCapacityForSerialization"
)]
pub struct CpuCapacity(u32);

/// Short helper function to build `CpuCapacity`.
#[inline(always)]
pub const fn mcpu(milli_cpus: u32) -> CpuCapacity {
    CpuCapacity::from_cpu_millis(milli_cpus)
}

impl CpuCapacity {
    #[inline(always)]
    pub const fn from_cpu_millis(cpu_millis: u32) -> CpuCapacity {
        CpuCapacity(cpu_millis)
    }

    #[inline(always)]
    pub fn cpu_millis(self) -> u32 {
        self.0
    }

    #[inline(always)]
    pub fn zero() -> CpuCapacity {
        CpuCapacity::from_cpu_millis(0u32)
    }

    #[inline(always)]
    pub fn one_cpu_thread() -> CpuCapacity {
        CpuCapacity::from_cpu_millis(1_000u32)
    }
}

impl Sub<CpuCapacity> for CpuCapacity {
    type Output = CpuCapacity;

    #[inline(always)]
    fn sub(self, rhs: CpuCapacity) -> Self::Output {
        CpuCapacity::from_cpu_millis(self.0 - rhs.0)
    }
}

impl Add<CpuCapacity> for CpuCapacity {
    type Output = CpuCapacity;

    #[inline(always)]
    fn add(self, rhs: CpuCapacity) -> Self::Output {
        CpuCapacity::from_cpu_millis(self.0 + rhs.0)
    }
}

impl Mul<u32> for CpuCapacity {
    type Output = CpuCapacity;

    #[inline(always)]
    fn mul(self, rhs: u32) -> CpuCapacity {
        CpuCapacity::from_cpu_millis(self.0 * rhs)
    }
}

impl Mul<f32> for CpuCapacity {
    type Output = CpuCapacity;

    #[inline(always)]
    fn mul(self, scale: f32) -> CpuCapacity {
        CpuCapacity::from_cpu_millis((self.0 as f32 * scale) as u32)
    }
}

impl Display for CpuCapacity {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}m", self.0)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum CpuCapacityForSerialization {
    Float(f32),
    MilliCpuWithUnit(String),
}

impl TryFrom<CpuCapacityForSerialization> for CpuCapacity {
    type Error = String;

    fn try_from(
        cpu_capacity_for_serialization: CpuCapacityForSerialization,
    ) -> Result<CpuCapacity, Self::Error> {
        match cpu_capacity_for_serialization {
            CpuCapacityForSerialization::Float(cpu_capacity) => {
                Ok(CpuCapacity((cpu_capacity * 1000.0f32) as u32))
            }
            CpuCapacityForSerialization::MilliCpuWithUnit(cpu_capacity_str) => {
                Self::from_str(&cpu_capacity_str)
            }
        }
    }
}

impl FromStr for CpuCapacity {
    type Err = String;

    fn from_str(cpu_capacity_str: &str) -> Result<Self, Self::Err> {
        let Some(milli_cpus_without_unit_str) = cpu_capacity_str.strip_suffix('m') else {
            return Err(format!(
                "invalid cpu capacity: `{cpu_capacity_str}`. String format expects a trailing 'm'."
            ));
        };
        let milli_cpus: u32 = milli_cpus_without_unit_str
            .parse::<u32>()
            .map_err(|_err| format!("invalid cpu capacity: `{cpu_capacity_str}`."))?;
        Ok(CpuCapacity(milli_cpus))
    }
}

impl From<CpuCapacity> for CpuCapacityForSerialization {
    fn from(cpu_capacity: CpuCapacity) -> CpuCapacityForSerialization {
        CpuCapacityForSerialization::MilliCpuWithUnit(format!("{}m", cpu_capacity.0))
    }
}

/// Whenever a shard position update is detected (whether it is emit by an indexing pipeline local
/// to the cluster or received via chitchat), the shard positions service publishes a
/// `ShardPositionsUpdate` event through the cluster's `EventBroker`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardPositionsUpdate {
    pub source_uid: SourceUid,
    // Only shards that received an update are listed here.
    pub updated_shard_positions: Vec<(ShardId, Position)>,
}

impl Event for ShardPositionsUpdate {}

impl RpcName for ApplyIndexingPlanRequest {
    fn rpc_name() -> &'static str {
        "apply_indexing_plan"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cpu_capacity_serialization() {
        assert_eq!(CpuCapacity::from_str("2000m").unwrap(), mcpu(2000));
        assert_eq!(CpuCapacity::from_cpu_millis(2500), mcpu(2500));
        assert_eq!(
            CpuCapacity::from_str("2.5").unwrap_err(),
            "invalid cpu capacity: `2.5`. String format expects a trailing 'm'."
        );
        assert_eq!(
            serde_json::from_value::<CpuCapacity>(serde_json::Value::String("1200m".to_string()))
                .unwrap(),
            mcpu(1200)
        );
        assert_eq!(
            serde_json::from_value::<CpuCapacity>(serde_json::Value::Number(
                serde_json::Number::from_f64(1.2f64).unwrap()
            ))
            .unwrap(),
            mcpu(1200)
        );
        assert_eq!(
            serde_json::from_value::<CpuCapacity>(serde_json::Value::Number(
                serde_json::Number::from(1u32)
            ))
            .unwrap(),
            mcpu(1000)
        );
        assert_eq!(CpuCapacity::from_cpu_millis(2500).to_string(), "2500m");
        assert_eq!(serde_json::to_string(&mcpu(2500)).unwrap(), "\"2500m\"");
    }
}
