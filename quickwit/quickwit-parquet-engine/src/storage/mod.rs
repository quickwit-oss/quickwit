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

//! Storage layer for Parquet files.

mod config;
pub mod inspect;
pub(crate) mod split_writer;
mod writer;

pub use config::{Compression, ParquetWriterConfig};
pub use inspect::{
    ColumnReport, PageReport, ParquetPageStatsReport, RowGroupReport, inspect_parquet_page_stats,
    verify_partition_prefix,
};
pub use split_writer::ParquetSplitWriter;
// Re-export metadata constants for use by the merge module and tests.
pub(crate) use writer::{
    PARQUET_META_NUM_MERGE_OPS, PARQUET_META_RG_PARTITION_PREFIX_LEN, PARQUET_META_ROW_KEYS,
    PARQUET_META_ROW_KEYS_JSON, PARQUET_META_SORT_FIELDS, PARQUET_META_WINDOW_DURATION,
    PARQUET_META_WINDOW_START, PARQUET_META_ZONEMAP_REGEXES,
};
pub use writer::{ParquetWriteError, ParquetWriter};
