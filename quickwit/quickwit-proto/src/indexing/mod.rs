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

use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::ops::{Add, Mul, Sub};
use std::{fmt, io};

use anyhow::anyhow;
use quickwit_actors::AskError;
use serde::{Deserialize, Serialize};
use thiserror;

use crate::types::{IndexUid, SourceId};
use crate::{ServiceError, ServiceErrorCode};

include!("../codegen/quickwit/quickwit.indexing.rs");

pub type IndexingResult<T> = std::result::Result<T, IndexingError>;

#[derive(Debug, thiserror::Error)]
pub enum IndexingError {
    #[error("indexing pipeline `{index_id}` for source `{source_id}` does not exist")]
    MissingPipeline { index_id: String, source_id: String },
    #[error(
        "pipeline #{pipeline_ord} for index `{index_id}` and source `{source_id}` already exists"
    )]
    PipelineAlreadyExists {
        index_id: String,
        source_id: SourceId,
        pipeline_ord: usize,
    },
    #[error("I/O error `{0}`")]
    Io(io::Error),
    #[error("invalid params `{0}`")]
    InvalidParams(anyhow::Error),
    #[error("Spanw pipelines errors `{pipeline_ids:?}`")]
    SpawnPipelinesError {
        pipeline_ids: Vec<IndexingPipelineId>,
    },
    #[error("a metastore error occurred: {0}")]
    MetastoreError(String),
    #[error("a storage resolver error occurred: {0}")]
    StorageResolverError(String),
    #[error("an internal error occurred: {0}")]
    Internal(String),
    #[error("the ingest service is unavailable")]
    Unavailable,
}

impl From<IndexingError> for tonic::Status {
    fn from(error: IndexingError) -> Self {
        match error {
            IndexingError::MissingPipeline {
                index_id,
                source_id,
            } => tonic::Status::not_found(format!("missing pipeline {index_id}/{source_id}")),
            IndexingError::PipelineAlreadyExists {
                index_id,
                source_id,
                pipeline_ord,
            } => tonic::Status::already_exists(format!(
                "pipeline {index_id}/{source_id} {pipeline_ord} already exists "
            )),
            IndexingError::Io(error) => tonic::Status::internal(error.to_string()),
            IndexingError::InvalidParams(error) => {
                tonic::Status::invalid_argument(error.to_string())
            }
            IndexingError::SpawnPipelinesError { pipeline_ids } => {
                tonic::Status::internal(format!("error spawning pipelines {:?}", pipeline_ids))
            }
            IndexingError::Internal(string) => tonic::Status::internal(string),
            IndexingError::MetastoreError(string) => tonic::Status::internal(string),
            IndexingError::StorageResolverError(string) => tonic::Status::internal(string),
            IndexingError::Unavailable => {
                tonic::Status::unavailable("indexing service is unavailable")
            }
        }
    }
}

impl From<tonic::Status> for IndexingError {
    fn from(status: tonic::Status) -> Self {
        match status.code() {
            tonic::Code::InvalidArgument => {
                IndexingError::InvalidParams(anyhow!(status.message().to_string()))
            }
            tonic::Code::NotFound => IndexingError::MissingPipeline {
                index_id: "".to_string(),
                source_id: "".to_string(),
            },
            tonic::Code::AlreadyExists => IndexingError::PipelineAlreadyExists {
                index_id: "".to_string(),
                source_id: "".to_string(),
                pipeline_ord: 0,
            },
            tonic::Code::Unavailable => IndexingError::Unavailable,
            _ => IndexingError::InvalidParams(anyhow!(status.message().to_string())),
        }
    }
}

impl ServiceError for IndexingError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::MissingPipeline { .. } => ServiceErrorCode::NotFound,
            Self::PipelineAlreadyExists { .. } => ServiceErrorCode::BadRequest,
            Self::InvalidParams(_) => ServiceErrorCode::BadRequest,
            Self::SpawnPipelinesError { .. } => ServiceErrorCode::Internal,
            Self::Io(_) => ServiceErrorCode::Internal,
            Self::Internal(_) => ServiceErrorCode::Internal,
            Self::MetastoreError(_) => ServiceErrorCode::Internal,
            Self::StorageResolverError(_) => ServiceErrorCode::Internal,
            Self::Unavailable => ServiceErrorCode::Unavailable,
        }
    }
}

impl From<AskError<IndexingError>> for IndexingError {
    fn from(error: AskError<IndexingError>) -> Self {
        match error {
            AskError::ErrorReply(error) => error,
            AskError::MessageNotDelivered => IndexingError::Unavailable,
            AskError::ProcessMessageError => IndexingError::Internal(
                "an error occurred while processing the request".to_string(),
            ),
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct IndexingPipelineId {
    pub node_id: String,
    pub index_uid: IndexUid,
    pub source_id: SourceId,
    pub pipeline_ord: usize,
}

impl Display for IndexingPipelineId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.index_uid, &self.source_id)
    }
}

impl Display for IndexingTask {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.index_uid, &self.source_id)
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
    pub cpu_millis: CpuCapacity,
    pub throughput_mb_per_sec: u16,
}

impl Display for PipelineMetrics {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{},{}MB/s", self.cpu_millis, self.throughput_mb_per_sec)
    }
}

/// One full pipeline (including merging) is assumed to consume 4 CPU threads.
/// The actual number somewhere between 3 and 4.
pub const PIPELINE_FULL_CAPACITY: CpuCapacity = CpuCapacity::from_cpu_millis(4_000u32);

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
