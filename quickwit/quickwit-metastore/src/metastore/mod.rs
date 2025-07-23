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

pub mod file_backed;
pub(crate) mod index_metadata;
#[cfg(feature = "postgres")]
pub mod postgres;

pub mod control_plane_metastore;

use std::cmp::Ordering;
use std::ops::{Bound, RangeInclusive};

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
pub use index_metadata::IndexMetadata;
use itertools::Itertools;
use quickwit_common::thread_pool::run_cpu_intensive;
use quickwit_config::{
    DocMapping, FileSourceParams, IndexConfig, IndexingSettings, IngestSettings, RetentionPolicy,
    SearchSettings, SourceConfig, SourceParams,
};
use quickwit_doc_mapper::tag_pruning::TagFilterAst;
use quickwit_proto::metastore::{
    AddSourceRequest, CreateIndexRequest, CreateIndexResponse, DeleteTask, IndexMetadataFailure,
    IndexMetadataRequest, IndexMetadataResponse, IndexesMetadataResponse,
    ListIndexesMetadataResponse, ListSplitsRequest, ListSplitsResponse, MetastoreError,
    MetastoreResult, MetastoreService, MetastoreServiceClient, MetastoreServiceStream,
    PublishSplitsRequest, StageSplitsRequest, UpdateIndexRequest, UpdateSourceRequest, serde_utils,
};
use quickwit_proto::types::{IndexUid, NodeId, SplitId};
use time::OffsetDateTime;

use crate::checkpoint::IndexCheckpointDelta;
use crate::{Split, SplitMetadata, SplitState};

/// Splits batch size returned by the stream splits API
pub(crate) const STREAM_SPLITS_CHUNK_SIZE: usize = 100;

/// An extended trait for [`MetastoreService`].
#[async_trait]
pub trait MetastoreServiceExt: MetastoreService {
    /// Returns whether the index `index_id` exists in the metastore.
    async fn index_exists(&mut self, index_id: &str) -> MetastoreResult<bool> {
        let request = IndexMetadataRequest::for_index_id(index_id.to_string());
        match self.index_metadata(request).await {
            Ok(_) => Ok(true),
            Err(MetastoreError::NotFound { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }
}

impl MetastoreServiceExt for MetastoreServiceClient {}

/// Helper trait to collect splits from a [`MetastoreServiceStream<ListSplitsResponse>`].
#[async_trait]
pub trait MetastoreServiceStreamSplitsExt {
    /// Collects all splits from a [`MetastoreServiceStream<ListSplitsResponse>`].
    async fn collect_splits(mut self) -> MetastoreResult<Vec<Split>>;

    /// Collects all splits metadata from a [`MetastoreServiceStream<ListSplitsResponse>`].
    async fn collect_splits_metadata(mut self) -> MetastoreResult<Vec<SplitMetadata>>;

    /// Collects all splits IDs from a [`MetastoreServiceStream<ListSplitsResponse>`].
    async fn collect_split_ids(mut self) -> MetastoreResult<Vec<SplitId>>;
}

#[async_trait]
impl MetastoreServiceStreamSplitsExt for MetastoreServiceStream<ListSplitsResponse> {
    async fn collect_splits(mut self) -> MetastoreResult<Vec<Split>> {
        let mut all_splits = Vec::new();
        while let Some(list_splits_response) = self.try_next().await? {
            let splits = list_splits_response.deserialize_splits().await?;
            all_splits.extend(splits);
        }
        Ok(all_splits)
    }

    async fn collect_splits_metadata(mut self) -> MetastoreResult<Vec<SplitMetadata>> {
        let mut all_splits_metadata = Vec::new();
        while let Some(list_splits_response) = self.try_next().await? {
            let splits_metadata = list_splits_response.deserialize_splits_metadata().await?;
            all_splits_metadata.extend(splits_metadata);
        }
        Ok(all_splits_metadata)
    }

    async fn collect_split_ids(mut self) -> MetastoreResult<Vec<SplitId>> {
        let mut all_splits = Vec::new();
        while let Some(list_splits_response) = self.try_next().await? {
            let splits = list_splits_response.deserialize_split_ids().await?;
            all_splits.extend(splits);
        }
        Ok(all_splits)
    }
}

/// Helper trait to build a [`CreateIndexRequest`] and deserialize its payload.
pub trait CreateIndexRequestExt {
    /// Creates a new [`CreateIndexRequest`] from an [`IndexConfig`].
    fn try_from_index_config(index_config: &IndexConfig) -> MetastoreResult<CreateIndexRequest>;

    /// Creates a new [`CreateIndexRequest`] from an [`IndexConfig`] and a list of [`SourceConfig`].
    fn try_from_index_and_source_configs(
        index_config: &IndexConfig,
        source_configs: &[SourceConfig],
    ) -> MetastoreResult<CreateIndexRequest>;

    /// Deserializes the `index_config_json` field of a [`CreateIndexRequest`] into an
    /// [`IndexConfig`].
    fn deserialize_index_config(&self) -> MetastoreResult<IndexConfig>;

    /// Deserializes the `source_configs_json` field of a [`CreateIndexRequest`] into an
    /// `Vec` of [`SourceConfig`].
    fn deserialize_source_configs(&self) -> MetastoreResult<Vec<SourceConfig>>;
}

impl CreateIndexRequestExt for CreateIndexRequest {
    fn try_from_index_config(index_config: &IndexConfig) -> MetastoreResult<CreateIndexRequest> {
        let index_config_json = serde_utils::to_json_str(index_config)?;
        let source_configs_json = Vec::new();
        let request = Self {
            index_config_json,
            source_configs_json,
        };
        Ok(request)
    }

    fn try_from_index_and_source_configs(
        index_config: &IndexConfig,
        source_configs: &[SourceConfig],
    ) -> MetastoreResult<CreateIndexRequest> {
        let index_config_json = serde_utils::to_json_str(index_config)?;
        let source_configs_json: Vec<String> = source_configs
            .iter()
            .map(serde_utils::to_json_str)
            .collect::<MetastoreResult<_>>()?;
        let request = Self {
            index_config_json,
            source_configs_json,
        };
        Ok(request)
    }

    fn deserialize_index_config(&self) -> MetastoreResult<IndexConfig> {
        serde_utils::from_json_str(&self.index_config_json)
    }

    fn deserialize_source_configs(&self) -> MetastoreResult<Vec<SourceConfig>> {
        self.source_configs_json
            .iter()
            .map(|source_config_json| serde_utils::from_json_str(source_config_json))
            .collect()
    }
}

/// Helper trait to deserialize the payload of a [`CreateIndexResponse`].
pub trait CreateIndexResponseExt {
    /// Deserializes the `index_metadata_json` field of a [`CreateIndexResponse`] into an
    /// [`IndexMetadata`].
    fn deserialize_index_metadata(&self) -> MetastoreResult<IndexMetadata>;
}

impl CreateIndexResponseExt for CreateIndexResponse {
    fn deserialize_index_metadata(&self) -> MetastoreResult<IndexMetadata> {
        serde_utils::from_json_str(&self.index_metadata_json)
    }
}

/// Helper trait to build a [`UpdateIndexRequest`] and deserialize its payload.
pub trait UpdateIndexRequestExt {
    /// Creates a new [`UpdateIndexRequest`] from the different updated fields.
    fn try_from_updates(
        index_uid: impl Into<IndexUid>,
        doc_mapping: &DocMapping,
        indexing_settings: &IndexingSettings,
        ingest_settings: &IngestSettings,
        search_settings: &SearchSettings,
        retention_policy_opt: &Option<RetentionPolicy>,
    ) -> MetastoreResult<UpdateIndexRequest>;

    /// Deserializes the `doc_mapping_json` field of an `[UpdateIndexRequest]` into a
    /// [`DocMapping`] object.
    fn deserialize_doc_mapping(&self) -> MetastoreResult<DocMapping>;

    /// Deserializes the `indexing_settings_json` field of an [`UpdateIndexRequest`] into a
    /// [`IndexingSettings`] object.
    fn deserialize_indexing_settings(&self) -> MetastoreResult<IndexingSettings>;

    /// Deserializes the `ingest_settings_json` field of an [`UpdateIndexRequest`] into a
    /// [`IngestSettings`] object.
    fn deserialize_ingest_settings(&self) -> MetastoreResult<IngestSettings>;

    /// Deserializes the `search_settings_json` field of an [`UpdateIndexRequest`] into a
    /// [`SearchSettings`] object.
    fn deserialize_search_settings(&self) -> MetastoreResult<SearchSettings>;

    /// Deserializes the `retention_policy_json` field of an [`UpdateIndexRequest`] into a
    /// [`RetentionPolicy`] object.
    fn deserialize_retention_policy(&self) -> MetastoreResult<Option<RetentionPolicy>>;
}

impl UpdateIndexRequestExt for UpdateIndexRequest {
    fn try_from_updates(
        index_uid: impl Into<IndexUid>,
        doc_mapping: &DocMapping,
        indexing_settings: &IndexingSettings,
        ingest_settings: &IngestSettings,
        search_settings: &SearchSettings,
        retention_policy_opt: &Option<RetentionPolicy>,
    ) -> MetastoreResult<UpdateIndexRequest> {
        let doc_mapping_json = serde_utils::to_json_str(doc_mapping)?;
        let indexing_settings_json = serde_utils::to_json_str(indexing_settings)?;
        let ingest_settings_json = serde_utils::to_json_str(ingest_settings)?;
        let search_settings_json = serde_utils::to_json_str(search_settings)?;
        let retention_policy_json_opt = retention_policy_opt
            .as_ref()
            .map(serde_utils::to_json_str)
            .transpose()?;

        let update_request = UpdateIndexRequest {
            index_uid: Some(index_uid.into()),
            doc_mapping_json,
            indexing_settings_json,
            ingest_settings_json,
            search_settings_json,
            retention_policy_json_opt,
        };
        Ok(update_request)
    }
    fn deserialize_doc_mapping(&self) -> MetastoreResult<DocMapping> {
        serde_utils::from_json_str(&self.doc_mapping_json)
    }

    fn deserialize_indexing_settings(&self) -> MetastoreResult<IndexingSettings> {
        serde_utils::from_json_str(&self.indexing_settings_json)
    }

    fn deserialize_ingest_settings(&self) -> MetastoreResult<IngestSettings> {
        serde_utils::from_json_str(&self.ingest_settings_json)
    }

    fn deserialize_search_settings(&self) -> MetastoreResult<SearchSettings> {
        serde_utils::from_json_str(&self.search_settings_json)
    }

    fn deserialize_retention_policy(&self) -> MetastoreResult<Option<RetentionPolicy>> {
        self.retention_policy_json_opt
            .as_ref()
            .map(|policy_json| serde_utils::from_json_str(policy_json))
            .transpose()
    }
}

/// Helper trait to build a [`IndexMetadataResponse`] and deserialize its payload.
pub trait IndexMetadataResponseExt {
    /// Creates a new [`IndexMetadataResponse`] from an [`IndexMetadata`].
    fn try_from_index_metadata(
        index_metadata: &IndexMetadata,
    ) -> MetastoreResult<IndexMetadataResponse>;

    /// Deserializes the `index_metadata_serialized_json` field of a [`IndexMetadataResponse`] into
    /// an [`IndexMetadata`].
    fn deserialize_index_metadata(&self) -> MetastoreResult<IndexMetadata>;
}

impl IndexMetadataResponseExt for IndexMetadataResponse {
    fn try_from_index_metadata(index_metadata: &IndexMetadata) -> MetastoreResult<Self> {
        let index_metadata_serialized_json = serde_utils::to_json_str(index_metadata)?;
        let response = Self {
            index_metadata_serialized_json,
        };
        Ok(response)
    }

    fn deserialize_index_metadata(&self) -> MetastoreResult<IndexMetadata> {
        serde_utils::from_json_str(&self.index_metadata_serialized_json)
    }
}

/// Helper trait to build a [`IndexesMetadataResponse`] and deserialize its payload.
#[async_trait]
pub trait IndexesMetadataResponseExt {
    /// Creates a new `IndexesMetadataResponse` from a `Vec` of [`IndexMetadata`].
    async fn try_from_indexes_metadata(
        indexes_metadata: Vec<IndexMetadata>,
        failures: Vec<IndexMetadataFailure>,
    ) -> MetastoreResult<IndexesMetadataResponse>;

    /// Deserializes the payload of an `IndexesMetadataResponse` into a `Vec`` of [`IndexMetadata`].
    async fn deserialize_indexes_metadata(self) -> MetastoreResult<Vec<IndexMetadata>>;

    /// Creates a new `IndexesMetadataResponse` from a `Vec` of [`IndexMetadata`] synchronously.
    #[cfg(any(test, feature = "testsuite"))]
    fn for_test(
        indexes_metadata: Vec<IndexMetadata>,
        failures: Vec<IndexMetadataFailure>,
    ) -> IndexesMetadataResponse {
        use futures::executor;

        executor::block_on(Self::try_from_indexes_metadata(indexes_metadata, failures)).unwrap()
    }
}

#[async_trait]
impl IndexesMetadataResponseExt for IndexesMetadataResponse {
    async fn try_from_indexes_metadata(
        indexes_metadata: Vec<IndexMetadata>,
        failures: Vec<IndexMetadataFailure>,
    ) -> MetastoreResult<Self> {
        let indexes_metadata_json_zstd = run_cpu_intensive(move || {
            serde_utils::to_json_zstd(&indexes_metadata, 0).map(Bytes::from)
        })
        .await
        .map_err(|join_error| MetastoreError::Internal {
            message: "failed to serialize indexes metadata".to_string(),
            cause: join_error.to_string(),
        })??;
        let response = Self {
            indexes_metadata_json_zstd,
            failures,
        };
        Ok(response)
    }

    async fn deserialize_indexes_metadata(self) -> MetastoreResult<Vec<IndexMetadata>> {
        run_cpu_intensive(move || serde_utils::from_json_zstd(&self.indexes_metadata_json_zstd))
            .await
            .map_err(|join_error| MetastoreError::Internal {
                message: "failed to deserialize indexes metadata".to_string(),
                cause: join_error.to_string(),
            })?
    }
}

/// Helper trait to build a `ListIndexesResponse` and deserialize its payload.
#[async_trait]
pub trait ListIndexesMetadataResponseExt {
    /// Creates a new `ListIndexesMetadataResponse` from a `Vec` of [`IndexMetadata`].
    async fn try_from_indexes_metadata(
        indexes_metadata: Vec<IndexMetadata>,
    ) -> MetastoreResult<ListIndexesMetadataResponse>;

    /// Deserializes the payload of a `ListIndexesResponse` into a `Vec`` of [`IndexMetadata`].
    async fn deserialize_indexes_metadata(self) -> MetastoreResult<Vec<IndexMetadata>>;

    /// Creates a new `ListIndexesMetadataResponse` from a `Vec` of [`IndexMetadata`] synchronously.
    #[cfg(any(test, feature = "testsuite"))]
    fn for_test(indexes_metadata: Vec<IndexMetadata>) -> ListIndexesMetadataResponse {
        use futures::executor;

        executor::block_on(Self::try_from_indexes_metadata(indexes_metadata)).unwrap()
    }
}

#[async_trait]
impl ListIndexesMetadataResponseExt for ListIndexesMetadataResponse {
    async fn try_from_indexes_metadata(
        indexes_metadata: Vec<IndexMetadata>,
    ) -> MetastoreResult<Self> {
        let indexes_metadata_json_zstd = run_cpu_intensive(move || {
            serde_utils::to_json_zstd(&indexes_metadata, 0).map(Bytes::from)
        })
        .await
        .map_err(|join_error| MetastoreError::Internal {
            message: "failed to serialize indexes metadata".to_string(),
            cause: join_error.to_string(),
        })??;
        let response = Self {
            indexes_metadata_json_zstd,
            indexes_metadata_json_opt: None,
        };
        Ok(response)
    }

    async fn deserialize_indexes_metadata(self) -> MetastoreResult<Vec<IndexMetadata>> {
        run_cpu_intensive(move || {
            if let Some(indexes_metadata_json) = &self.indexes_metadata_json_opt {
                return serde_utils::from_json_str(indexes_metadata_json);
            };
            serde_utils::from_json_zstd(&self.indexes_metadata_json_zstd)
        })
        .await
        .map_err(|join_error| MetastoreError::Internal {
            message: "failed to deserialize indexes metadata".to_string(),
            cause: join_error.to_string(),
        })?
    }
}

/// Helper trait to build a [`AddSourceRequest`] and deserialize its payload.
pub trait AddSourceRequestExt {
    /// Creates a new [`AddSourceRequest`] from a [`SourceConfig`].
    fn try_from_source_config(
        index_uid: impl Into<IndexUid>,
        source_config: &SourceConfig,
    ) -> MetastoreResult<AddSourceRequest>;

    /// Deserializes the `source_config_json` field of a [`AddSourceRequest`] into a
    /// [`SourceConfig`].
    fn deserialize_source_config(&self) -> MetastoreResult<SourceConfig>;
}

impl AddSourceRequestExt for AddSourceRequest {
    fn try_from_source_config(
        index_uid: impl Into<IndexUid>,
        source_config: &SourceConfig,
    ) -> MetastoreResult<AddSourceRequest> {
        let source_config_json = serde_utils::to_json_str(&source_config)?;
        let request = Self {
            index_uid: Some(index_uid.into()),
            source_config_json,
        };
        Ok(request)
    }

    fn deserialize_source_config(&self) -> MetastoreResult<SourceConfig> {
        serde_utils::from_json_str(&self.source_config_json)
    }
}

/// Helper trait to build a [`UpdateSourceRequest`] and deserialize its payload.
pub trait UpdateSourceRequestExt {
    /// Creates a new [`UpdateSourceRequest`] from a [`SourceConfig`].
    fn try_from_source_config(
        index_uid: impl Into<IndexUid>,
        source_config: &SourceConfig,
    ) -> MetastoreResult<UpdateSourceRequest>;

    /// Deserializes the `source_config_json` field of a [`UpdateSourceRequest`] into a
    /// [`SourceConfig`].
    fn deserialize_source_config(&self) -> MetastoreResult<SourceConfig>;
}

impl UpdateSourceRequestExt for UpdateSourceRequest {
    fn try_from_source_config(
        index_uid: impl Into<IndexUid>,
        source_config: &SourceConfig,
    ) -> MetastoreResult<UpdateSourceRequest> {
        let source_config_json = serde_utils::to_json_str(&source_config)?;
        let request = Self {
            index_uid: Some(index_uid.into()),
            source_config_json,
        };
        Ok(request)
    }

    fn deserialize_source_config(&self) -> MetastoreResult<SourceConfig> {
        serde_utils::from_json_str(&self.source_config_json)
    }
}
/// Helper trait to build a [`DeleteTask`] and deserialize its payload.
pub trait StageSplitsRequestExt {
    /// Creates a new [`StageSplitsRequest`] from a [`SplitMetadata`].
    fn try_from_split_metadata(
        index_uid: impl Into<IndexUid>,
        split_metadata: &SplitMetadata,
    ) -> MetastoreResult<StageSplitsRequest>;

    /// Creates a new [`StageSplitsRequest`] from a list of [`SplitMetadata`].
    fn try_from_splits_metadata(
        index_uid: impl Into<IndexUid>,
        splits_metadata: impl IntoIterator<Item = SplitMetadata>,
    ) -> MetastoreResult<StageSplitsRequest>;

    /// Deserializes the `split_metadata_list_serialized_json` field of a [`StageSplitsRequest`]
    /// into a list of [`SplitMetadata`].
    fn deserialize_splits_metadata(&self) -> MetastoreResult<Vec<SplitMetadata>>;
}

impl StageSplitsRequestExt for StageSplitsRequest {
    fn try_from_split_metadata(
        index_uid: impl Into<IndexUid>,
        split_metadata: &SplitMetadata,
    ) -> MetastoreResult<StageSplitsRequest> {
        let split_metadata_list_serialized_json = serde_utils::to_json_str(&[split_metadata])?;
        let request = Self {
            index_uid: Some(index_uid.into()),
            split_metadata_list_serialized_json,
        };
        Ok(request)
    }

    fn try_from_splits_metadata(
        index_uid: impl Into<IndexUid>,
        splits_metadata: impl IntoIterator<Item = SplitMetadata>,
    ) -> MetastoreResult<StageSplitsRequest> {
        let splits_metadata: Vec<SplitMetadata> = splits_metadata.into_iter().collect();
        let split_metadata_list_serialized_json = serde_utils::to_json_str(&splits_metadata)?;
        let request = Self {
            index_uid: Some(index_uid.into()),
            split_metadata_list_serialized_json,
        };
        Ok(request)
    }

    fn deserialize_splits_metadata(&self) -> MetastoreResult<Vec<SplitMetadata>> {
        serde_utils::from_json_str(&self.split_metadata_list_serialized_json)
    }
}

/// Helper trait to build a [`ListSplitsRequest`] and deserialize its payload.
pub trait ListSplitsRequestExt {
    /// Creates a new [`ListSplitsRequest`] from an [`IndexUid`].
    fn try_from_index_uid(index_uid: IndexUid) -> MetastoreResult<ListSplitsRequest>;

    /// Creates a new [`ListSplitsRequest`] from a [`ListSplitsQuery`].
    fn try_from_list_splits_query(
        list_splits_query: &ListSplitsQuery,
    ) -> MetastoreResult<ListSplitsRequest>;

    /// Deserializes the `query_json` field of a [`ListSplitsRequest`] into a [`ListSplitsQuery`].
    fn deserialize_list_splits_query(&self) -> MetastoreResult<ListSplitsQuery>;
}

impl ListSplitsRequestExt for ListSplitsRequest {
    fn try_from_index_uid(index_uid: IndexUid) -> MetastoreResult<ListSplitsRequest> {
        let list_splits_query = ListSplitsQuery::for_index(index_uid);
        Self::try_from_list_splits_query(&list_splits_query)
    }

    fn try_from_list_splits_query(
        list_splits_query: &ListSplitsQuery,
    ) -> MetastoreResult<ListSplitsRequest> {
        let query_json = serde_utils::to_json_str(&list_splits_query)?;
        let request = Self { query_json };
        Ok(request)
    }

    fn deserialize_list_splits_query(&self) -> MetastoreResult<ListSplitsQuery> {
        let list_splits_query = serde_utils::from_json_str(&self.query_json)?;
        Ok(list_splits_query)
    }
}

/// Helper trait to build a [`ListSplitsResponse`] and deserialize its payload.
#[async_trait]
pub trait ListSplitsResponseExt {
    /// Creates a new [`ListSplitsResponse`] from a list of [`Split`].
    fn try_from_splits(
        splits: impl IntoIterator<Item = Split>,
    ) -> MetastoreResult<ListSplitsResponse>;

    /// Deserializes the `splits_serialized_json` field of a [`ListSplitsResponse`] into a list of
    /// [`Split`].
    async fn deserialize_splits(self) -> MetastoreResult<Vec<Split>>;

    /// Deserializes the `splits_serialized_json` field of a [`ListSplitsResponse`] into a list of
    /// [`SplitMetadata`].
    async fn deserialize_splits_metadata(self) -> MetastoreResult<Vec<SplitMetadata>>;

    /// Deserializes the `splits_serialized_json` field of a [`ListSplitsResponse`] into a list of
    /// [`SplitId`].
    async fn deserialize_split_ids(self) -> MetastoreResult<Vec<SplitId>>;

    /// Creates an empty [`ListSplitsResponse`].
    fn empty() -> Self;
}

/// Helper trait for [`PublishSplitsRequest`] to deserialize its payload.
pub trait PublishSplitsRequestExt {
    /// Deserializes the `index_checkpoint_delta_json_opt` field of a [`PublishSplitsRequest`] into
    /// an [`Option<IndexCheckpointDelta>`].
    fn deserialize_index_checkpoint(&self) -> MetastoreResult<Option<IndexCheckpointDelta>>;
}

impl PublishSplitsRequestExt for PublishSplitsRequest {
    fn deserialize_index_checkpoint(&self) -> MetastoreResult<Option<IndexCheckpointDelta>> {
        self.index_checkpoint_delta_json_opt
            .as_ref()
            .map(|value| serde_utils::from_json_str(value))
            .transpose()
    }
}

#[async_trait]
impl ListSplitsResponseExt for ListSplitsResponse {
    fn empty() -> Self {
        Self {
            splits_serialized_json: "[]".to_string(),
        }
    }

    fn try_from_splits(splits: impl IntoIterator<Item = Split>) -> MetastoreResult<Self> {
        let splits_serialized_json = serde_utils::to_json_str(&splits.into_iter().collect_vec())?;
        let response = Self {
            splits_serialized_json,
        };
        Ok(response)
    }

    async fn deserialize_splits(self) -> MetastoreResult<Vec<Split>> {
        run_cpu_intensive(move || serde_utils::from_json_str(&self.splits_serialized_json))
            .await
            .map_err(|join_error| MetastoreError::Internal {
                message: "failed to deserialize splits".to_string(),
                cause: join_error.to_string(),
            })?
    }

    async fn deserialize_splits_metadata(self) -> MetastoreResult<Vec<SplitMetadata>> {
        let splits = self.deserialize_splits().await?;
        let splits_metadata = splits
            .into_iter()
            .map(|split| split.split_metadata)
            .collect();
        Ok(splits_metadata)
    }

    async fn deserialize_split_ids(self) -> MetastoreResult<Vec<SplitId>> {
        let splits = self.deserialize_splits().await?;
        let split_ids = splits
            .into_iter()
            .map(|split| split.split_metadata.split_id)
            .collect();
        Ok(split_ids)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// A query builder for listing splits within the metastore.
pub struct ListSplitsQuery {
    /// A non-empty list of index UIDs for which to fetch the splits, or
    /// None if we want splits from all indexes.
    pub index_uids: Option<Vec<IndexUid>>,

    /// A specific node ID to filter by.
    pub node_id: Option<NodeId>,

    /// The maximum number of splits to retrieve.
    pub limit: Option<usize>,

    /// The number of splits to skip.
    pub offset: Option<usize>,

    /// A specific split state(s) to filter by.
    pub split_states: Vec<SplitState>,

    /// A specific set of tag(s) to filter by.
    pub tags: Option<TagFilterAst>,

    /// The time range to filter by.
    pub time_range: FilterRange<i64>,

    /// The maximum time range end to filter by.
    pub max_time_range_end: Option<i64>,

    /// The delete opstamp range to filter by.
    pub delete_opstamp: FilterRange<u64>,

    /// The update timestamp range to filter by.
    pub update_timestamp: FilterRange<i64>,

    /// The create timestamp range to filter by.
    pub create_timestamp: FilterRange<i64>,

    /// The datetime at which you include or exclude mature splits.
    pub mature: Bound<OffsetDateTime>,

    /// Sorts the splits by staleness, i.e. by delete opstamp and publish timestamp in ascending
    /// order.
    pub sort_by: SortBy,

    /// Only return splits whose (index_uid, split_id) are lexicographically after this split
    pub after_split: Option<(IndexUid, SplitId)>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SortBy {
    None,
    Staleness,
    IndexUid,
}

impl SortBy {
    fn compare(&self, left_split: &Split, right_split: &Split) -> Ordering {
        match self {
            SortBy::None => Ordering::Equal,
            SortBy::Staleness => left_split
                .split_metadata
                .delete_opstamp
                .cmp(&right_split.split_metadata.delete_opstamp)
                .then_with(|| {
                    left_split
                        .publish_timestamp
                        .cmp(&right_split.publish_timestamp)
                }),
            SortBy::IndexUid => left_split
                .split_metadata
                .index_uid
                .cmp(&right_split.split_metadata.index_uid)
                .then_with(|| {
                    left_split
                        .split_metadata
                        .split_id
                        .cmp(&right_split.split_metadata.split_id)
                }),
        }
    }
}

#[allow(unused_attributes)]
impl ListSplitsQuery {
    /// Creates a new [`ListSplitsQuery`] for the designated index.
    pub fn for_index(index_uid: IndexUid) -> Self {
        Self {
            index_uids: Some(vec![index_uid]),
            node_id: None,
            limit: None,
            offset: None,
            split_states: Vec::new(),
            tags: None,
            time_range: Default::default(),
            max_time_range_end: None,
            delete_opstamp: Default::default(),
            update_timestamp: Default::default(),
            create_timestamp: Default::default(),
            mature: Bound::Unbounded,
            sort_by: SortBy::None,
            after_split: None,
        }
    }

    /// Creates a new [`ListSplitsQuery`] from a non-empty list of index UIDs.
    /// Returns None if the list is empty.
    pub fn try_from_index_uids(index_uids: Vec<IndexUid>) -> Option<Self> {
        if index_uids.is_empty() {
            return None;
        }
        Some(Self {
            index_uids: Some(index_uids),
            node_id: None,
            limit: None,
            offset: None,
            split_states: Vec::new(),
            tags: None,
            time_range: Default::default(),
            max_time_range_end: None,
            delete_opstamp: Default::default(),
            update_timestamp: Default::default(),
            create_timestamp: Default::default(),
            mature: Bound::Unbounded,
            sort_by: SortBy::None,
            after_split: None,
        })
    }

    /// Creates a new [`ListSplitsQuery`] for all indexes.
    pub fn for_all_indexes() -> Self {
        Self {
            index_uids: None,
            node_id: None,
            limit: None,
            offset: None,
            split_states: Vec::new(),
            tags: None,
            time_range: Default::default(),
            max_time_range_end: None,
            delete_opstamp: Default::default(),
            update_timestamp: Default::default(),
            create_timestamp: Default::default(),
            mature: Bound::Unbounded,
            sort_by: SortBy::None,
            after_split: None,
        }
    }

    /// Selects splits produced by the specified node.
    pub fn with_node_id(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }

    /// Sets the maximum number of splits to retrieve.
    pub fn with_limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Sets the number of splits to skip.
    pub fn with_offset(mut self, n: usize) -> Self {
        self.offset = Some(n);
        self
    }

    /// Selects splits which have the given split state.
    pub fn with_split_state(mut self, state: SplitState) -> Self {
        self.split_states.push(state);
        self
    }

    /// Selects splits which have the any of the following split state.
    pub fn with_split_states(mut self, states: impl AsRef<[SplitState]>) -> Self {
        self.split_states.extend_from_slice(states.as_ref());
        self
    }

    /// Selects splits which match the given tag filter.
    pub fn with_tags_filter(mut self, tags: TagFilterAst) -> Self {
        self.tags = Some(tags);
        self
    }

    /// Sets the field's lower bound to match values that are
    /// *less than or equal to* the provided value.
    pub fn with_time_range_end_lte(mut self, v: i64) -> Self {
        self.time_range.end = Bound::Included(v);
        self
    }

    /// Sets the field's lower bound to match values that are
    /// *less than* the provided value.
    pub fn with_time_range_end_lt(mut self, v: i64) -> Self {
        self.time_range.end = Bound::Excluded(v);
        self
    }

    /// Sets the field's upper bound to match values that are
    /// *greater than or equal to* the provided value.
    pub fn with_time_range_start_gte(mut self, v: i64) -> Self {
        self.time_range.start = Bound::Included(v);
        self
    }

    /// Sets the field's upper bound to match values that are
    /// *greater than* the provided value.
    pub fn with_time_range_start_gt(mut self, v: i64) -> Self {
        self.time_range.start = Bound::Excluded(v);
        self
    }

    /// Retains only splits with a time range end that is
    /// *less than or equal to* the provided value.
    pub fn with_max_time_range_end(mut self, v: i64) -> Self {
        self.max_time_range_end = Some(v);
        self
    }

    /// Sets the field's lower bound to match values that are
    /// *less than or equal to* the provided value.
    pub fn with_delete_opstamp_lte(mut self, v: u64) -> Self {
        self.delete_opstamp.end = Bound::Included(v);
        self
    }

    /// Sets the field's lower bound to match values that are
    /// *less than* the provided value.
    pub fn with_delete_opstamp_lt(mut self, v: u64) -> Self {
        self.delete_opstamp.end = Bound::Excluded(v);
        self
    }

    /// Sets the field's upper bound to match values that are
    /// *greater than or equal to* the provided value.
    pub fn with_delete_opstamp_gte(mut self, v: u64) -> Self {
        self.delete_opstamp.start = Bound::Included(v);
        self
    }

    /// Sets the field's upper bound to match values that are
    /// *greater than* the provided value.
    pub fn with_delete_opstamp_gt(mut self, v: u64) -> Self {
        self.delete_opstamp.start = Bound::Excluded(v);
        self
    }

    /// Sets the field's lower bound to match values that are
    /// *less than or equal to* the provided value.
    pub fn with_update_timestamp_lte(mut self, v: i64) -> Self {
        self.update_timestamp.end = Bound::Included(v);
        self
    }

    /// Sets the field's lower bound to match values that are
    /// *less than* the provided value.
    pub fn with_update_timestamp_lt(mut self, v: i64) -> Self {
        self.update_timestamp.end = Bound::Excluded(v);
        self
    }

    /// Sets the field's upper bound to match values that are
    /// *greater than or equal to* the provided value.
    pub fn with_update_timestamp_gte(mut self, v: i64) -> Self {
        self.update_timestamp.start = Bound::Included(v);
        self
    }

    /// Sets the field's upper bound to match values that are
    /// *greater than* the provided value.
    pub fn with_update_timestamp_gt(mut self, v: i64) -> Self {
        self.update_timestamp.start = Bound::Excluded(v);
        self
    }

    /// Sets the field's lower bound to match values that are
    /// *less than or equal to* the provided value.
    pub fn with_create_timestamp_lte(mut self, v: i64) -> Self {
        self.create_timestamp.end = Bound::Included(v);
        self
    }

    /// Sets the field's lower bound to match values that are
    /// *less than* the provided value.
    pub fn with_create_timestamp_lt(mut self, v: i64) -> Self {
        self.create_timestamp.end = Bound::Excluded(v);
        self
    }

    /// Sets the field's upper bound to match values that are
    /// *greater than or equal to* the provided value.
    pub fn with_create_timestamp_gte(mut self, v: i64) -> Self {
        self.create_timestamp.start = Bound::Included(v);
        self
    }

    /// Sets the field's upper bound to match values that are
    /// *greater than* the provided value.
    pub fn with_create_timestamp_gt(mut self, v: i64) -> Self {
        self.create_timestamp.start = Bound::Excluded(v);
        self
    }

    /// Retains splits that are mature at the given datetime.
    pub fn retain_mature(mut self, now: OffsetDateTime) -> Self {
        self.mature = Bound::Included(now);
        self
    }

    /// Retains splits that are immature at the given datetime.
    pub fn retain_immature(mut self, now: OffsetDateTime) -> Self {
        self.mature = Bound::Excluded(now);
        self
    }

    /// Sorts the splits by staleness, i.e. by delete opstamp and publish timestamp in ascending
    /// order.
    pub fn sort_by_staleness(mut self) -> Self {
        self.sort_by = SortBy::Staleness;
        self
    }

    /// Sorts the splits by index_uid and split_id.
    pub fn sort_by_index_uid(mut self) -> Self {
        self.sort_by = SortBy::IndexUid;
        self
    }

    /// Only return splits whose (index_uid, split_id) are lexicographically after this split.
    /// This is only useful if results are sorted by index_uid and split_id.
    pub fn after_split(mut self, split_meta: &SplitMetadata) -> Self {
        self.after_split = Some((split_meta.index_uid.clone(), split_meta.split_id.clone()));
        self
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// A range containing the upper and lower bounds to filter documents by.
pub struct FilterRange<T> {
    /// The lower bound of the filter.
    pub start: Bound<T>,
    /// The upper bound of the filter.
    pub end: Bound<T>,
}

impl<T: PartialEq + PartialOrd> FilterRange<T> {
    /// Checks if both the upper and lower bound are `Bound::Unbounded`.
    pub fn is_unbounded(&self) -> bool {
        self.start == Bound::Unbounded && self.end == Bound::Unbounded
    }

    /// Checks if the provided value lies within the upper and lower bounds
    /// of the range.
    pub fn contains(&self, value: &T) -> bool {
        if self.is_unbounded() {
            return true;
        }

        let lower_check = match &self.start {
            Bound::Unbounded => true,
            Bound::Included(left) => left <= value,
            Bound::Excluded(left) => left < value,
        };

        let upper_check = match &self.end {
            Bound::Unbounded => true,
            Bound::Included(left) => left >= value,
            Bound::Excluded(left) => left > value,
        };

        lower_check && upper_check
    }

    /// Checks if the provided range overlaps with the range.
    pub fn overlaps_with(&self, range: RangeInclusive<T>) -> bool {
        if self.is_unbounded() {
            return true;
        }

        let lower_check = match &self.start {
            Bound::Unbounded => true,
            Bound::Included(left) => left <= range.end(),
            Bound::Excluded(left) => left < range.end(),
        };

        let upper_check = match &self.end {
            Bound::Unbounded => true,
            Bound::Included(left) => left >= range.start(),
            Bound::Excluded(left) => left > range.start(),
        };

        lower_check && upper_check
    }
}

// The `Default` derive implementation imposes a restriction
// for `T` to also implement Default when this is not required.
impl<T> Default for FilterRange<T> {
    fn default() -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }
}

/// Maps the given source params to whether checkpoints should be stored in the index metadata
/// (false) or the shard table (true)
fn use_shard_api(params: &SourceParams) -> bool {
    match params {
        SourceParams::File(FileSourceParams::Filepath(_)) => false,
        SourceParams::File(FileSourceParams::Notifications(_)) => true,
        SourceParams::Ingest => true,
        SourceParams::IngestApi => false,
        SourceParams::IngestCli => false,
        SourceParams::Kafka(_) => false,
        SourceParams::Kinesis(_) => false,
        SourceParams::PubSub(_) => false,
        SourceParams::Pulsar(_) => false,
        SourceParams::Stdin => panic!("stdin cannot be checkpointed"),
        SourceParams::Vec(_) => false,
        SourceParams::Void(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_contains() {
        let filter = FilterRange {
            start: Bound::Unbounded,
            end: Bound::Excluded(50),
        };
        assert!(!filter.contains(&50));
        assert!(filter.contains(&0));
        assert!(filter.contains(&49));

        let filter = FilterRange {
            start: Bound::Included(50),
            end: Bound::Unbounded,
        };
        assert!(filter.contains(&50));
        assert!(filter.contains(&51));
        assert!(!filter.contains(&0));

        let filter = FilterRange {
            start: Bound::Included(50),
            end: Bound::Excluded(75),
        };
        assert!(filter.contains(&50));
        assert!(filter.contains(&51));
        assert!(!filter.contains(&0));
        assert!(!filter.contains(&75));
        assert!(filter.contains(&74));
    }

    #[test]
    fn test_overlaps_with() {
        let filter = FilterRange {
            start: Bound::Unbounded,
            end: Bound::Excluded(50),
        };
        assert!(filter.overlaps_with(0..=50));
        assert!(filter.overlaps_with(0..=51));
        assert!(filter.overlaps_with(32..=63));
        assert!(filter.overlaps_with(32..=32));
        assert!(!filter.overlaps_with(51..=76));
        assert!(!filter.overlaps_with(50..=76));

        let filter = FilterRange {
            start: Bound::Unbounded,
            end: Bound::Included(50),
        };
        assert!(filter.overlaps_with(0..=50));
        assert!(filter.overlaps_with(0..=51));
        assert!(filter.overlaps_with(50..=76));
        assert!(!filter.overlaps_with(51..=76));

        let filter = FilterRange {
            start: Bound::Excluded(50),
            end: Bound::Unbounded,
        };
        assert!(filter.overlaps_with(51..=75));
        assert!(filter.overlaps_with(0..=51));
        assert!(filter.overlaps_with(51..=76));
        assert!(filter.overlaps_with(50..=76));
        assert!(!filter.overlaps_with(0..=49));
        assert!(!filter.overlaps_with(0..=50));

        let filter = FilterRange {
            start: Bound::Included(50),
            end: Bound::Unbounded,
        };
        assert!(filter.overlaps_with(51..=75));
        assert!(filter.overlaps_with(0..=51));
        assert!(filter.overlaps_with(51..=76));
        assert!(filter.overlaps_with(50..=76));
        assert!(filter.overlaps_with(0..=50));
        assert!(!filter.overlaps_with(0..=49));

        let filter = FilterRange {
            start: Bound::Included(50),
            end: Bound::Excluded(75),
        };
        assert!(filter.overlaps_with(51..=75));
        assert!(filter.overlaps_with(0..=51));
        assert!(filter.overlaps_with(45..=76));
        assert!(filter.overlaps_with(50..=76));
        assert!(filter.overlaps_with(0..=50));
        assert!(filter.overlaps_with(74..=124));
        assert!(!filter.overlaps_with(0..=49));
        assert!(!filter.overlaps_with(75..=124));
    }

    #[tokio::test]
    async fn test_list_splits_response_empty() {
        let response = ListSplitsResponse::empty();
        let splits = response.deserialize_splits().await.unwrap();
        assert!(splits.is_empty());
    }

    #[tokio::test]
    async fn test_list_indexes_metadata_response_serde() {
        let response = ListIndexesMetadataResponse::try_from_indexes_metadata(Vec::new())
            .await
            .unwrap();
        let indexes_metadata = response.deserialize_indexes_metadata().await.unwrap();
        assert!(indexes_metadata.is_empty());

        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let response = ListIndexesMetadataResponse::for_test(vec![index_metadata.clone()]);
        let indexes_metadata = response.deserialize_indexes_metadata().await.unwrap();
        assert_eq!(indexes_metadata.len(), 1);
        assert_eq!(indexes_metadata[0], index_metadata);
    }

    #[tokio::test]
    async fn test_list_indexes_metadata_backward_compatible_serde() {
        let indexes_metadata_json = serde_json::to_string(&Vec::<IndexMetadata>::new()).unwrap();
        let response = ListIndexesMetadataResponse {
            indexes_metadata_json_opt: Some(indexes_metadata_json),
            indexes_metadata_json_zstd: Bytes::from_static(b""),
        };
        let indexes_metadata = response.deserialize_indexes_metadata().await.unwrap();
        assert!(indexes_metadata.is_empty());

        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let indexes_metadata_json = serde_json::to_string(&vec![index_metadata.clone()]).unwrap();
        let response = ListIndexesMetadataResponse {
            indexes_metadata_json_opt: Some(indexes_metadata_json),
            indexes_metadata_json_zstd: Bytes::from_static(b""),
        };
        let indexes_metadata = response.deserialize_indexes_metadata().await.unwrap();
        assert_eq!(indexes_metadata.len(), 1);
        assert_eq!(indexes_metadata[0], index_metadata);
    }
}
