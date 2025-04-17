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

#![warn(missing_docs)]
#![allow(clippy::bool_assert_comparison)]
#![deny(clippy::disallowed_methods)]
#![allow(rustdoc::invalid_html_tags)]

//! `quickwit-metastore` is the abstraction used in Quickwit to interface itself to different
//! metastore:
//! - file-backed metastore
//! - PostgreSQL metastore
//! - etc.

#[allow(missing_docs)]
pub mod checkpoint;
mod error;
mod metastore;
mod metastore_factory;
mod metastore_resolver;
mod split_metadata;
mod split_metadata_version;
#[cfg(test)]
pub(crate) mod tests;

use std::ops::Range;

pub use error::MetastoreResolverError;
pub use metastore::control_plane_metastore::ControlPlaneMetastore;
pub use metastore::file_backed::FileBackedMetastore;
pub(crate) use metastore::index_metadata::serialize::{IndexMetadataV0_8, VersionedIndexMetadata};
#[cfg(feature = "postgres")]
pub use metastore::postgres::PostgresqlMetastore;
pub use metastore::{
    AddSourceRequestExt, CreateIndexRequestExt, CreateIndexResponseExt, IndexMetadata,
    IndexMetadataResponseExt, IndexesMetadataResponseExt, ListIndexesMetadataResponseExt,
    ListSplitsQuery, ListSplitsRequestExt, ListSplitsResponseExt, MetastoreServiceExt,
    MetastoreServiceStreamSplitsExt, PublishSplitsRequestExt, StageSplitsRequestExt,
    UpdateIndexRequestExt, UpdateSourceRequestExt, file_backed,
};
pub use metastore_factory::{MetastoreFactory, UnsupportedMetastore};
pub use metastore_resolver::MetastoreResolver;
use quickwit_common::is_disjoint;
use quickwit_doc_mapper::tag_pruning::TagFilterAst;
pub use split_metadata::{Split, SplitInfo, SplitMaturity, SplitMetadata, SplitState};
pub(crate) use split_metadata_version::{SplitMetadataV0_8, VersionedSplitMetadata};

#[derive(utoipa::OpenApi)]
#[openapi(components(schemas(
    IndexMetadataV0_8,
    Split,
    SplitMetadataV0_8,
    SplitState,
    VersionedIndexMetadata,
    VersionedSplitMetadata,
)))]
/// Schema used for the OpenAPI generation which are apart of this crate.
pub struct MetastoreApiSchemas;

/// Returns `true` if the split time range is included in `time_range_opt`.
/// If `time_range_opt` is None, returns always true.
pub fn split_time_range_filter(
    split_metadata: &SplitMetadata,
    time_range_opt: Option<&Range<i64>>,
) -> bool {
    match (time_range_opt, split_metadata.time_range.as_ref()) {
        (Some(filter_time_range), Some(split_time_range)) => {
            !is_disjoint(filter_time_range, split_time_range)
        }
        _ => true, // Return `true` if `time_range` is omitted or the split has no time range.
    }
}
/// Returns `true` if the tags filter evaluation is true.
/// If `tags_filter_opt` is None, returns always true.
pub fn split_tag_filter(
    split_metadata: &SplitMetadata,
    tags_filter_opt: Option<&TagFilterAst>,
) -> bool {
    tags_filter_opt
        .map(|tags_filter_ast| tags_filter_ast.evaluate(&split_metadata.tags))
        .unwrap_or(true)
}

#[cfg(test)]
mod backward_compatibility_tests;

#[cfg(any(test, feature = "testsuite"))]
/// Returns a metastore backed by an "in-memory file" for testing.
pub fn metastore_for_test() -> quickwit_proto::metastore::MetastoreServiceClient {
    quickwit_proto::metastore::MetastoreServiceClient::new(FileBackedMetastore::for_test(
        std::sync::Arc::new(quickwit_storage::RamStorage::default()),
    ))
}
