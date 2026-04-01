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

//! Parquet split format replacing Tantivy segments.
//!
//! A ParquetSplit is the storage unit for metrics data, containing
//! Parquet file(s) and metadata for efficient query pruning.

mod format;
mod metadata;
pub mod postgres;

pub use format::{CURRENT_FORMAT_VERSION, ParquetSplit};
pub use metadata::{
    MetricsSplitMetadata, MetricsSplitMetadataBuilder, MetricsSplitState, SplitId, TAG_DATACENTER,
    TAG_ENV, TAG_HOST, TAG_REGION, TAG_SERVICE, TimeRange,
};
pub use postgres::{InsertableMetricsSplit, MetricsSplitRecord, MetricsSplits, PgMetricsSplit};
