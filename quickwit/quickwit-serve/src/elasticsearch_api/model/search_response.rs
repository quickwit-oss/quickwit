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

use elasticsearch_dsl::{ClusterStatistics, HitsMetadata, ShardStatistics, Suggest};
use quickwit_search::AggregationResults;
use serde::Serialize;

type Map<K, V> = std::collections::BTreeMap<K, V>;

/// Search response
///
/// This is a fork of [`elasticsearch_dsl::SearchResponse`] with the
/// `aggregations` field using [`AggregationResults`] instead of
/// [`serde_json::Value`].
#[derive(Debug, Default, Serialize, PartialEq)]
pub struct ElasticsearchResponse {
    /// The time that it took Elasticsearch to process the query
    pub took: u32,

    /// The search has been cancelled and results are partial
    pub timed_out: bool,

    /// Indicates if search has been terminated early
    #[serde(default)]
    pub terminated_early: Option<bool>,

    /// Scroll Id
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "_scroll_id")]
    pub scroll_id: Option<String>,

    /// Dynamically fetched fields
    #[serde(default)]
    pub fields: Map<String, serde_json::Value>,

    /// Point in time Id
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pit_id: Option<String>,

    /// Number of reduce phases
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_reduce_phases: Option<u64>,

    /// Maximum document score. [None] when documents are implicitly sorted
    /// by a field other than `_score`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_score: Option<f32>,

    /// Number of clusters touched with their states
    #[serde(skip_serializing_if = "Option::is_none", rename = "_clusters")]
    pub clusters: Option<ClusterStatistics>,

    /// Number of shards touched with their states
    #[serde(rename = "_shards")]
    pub shards: ShardStatistics,

    /// Search hits
    pub hits: HitsMetadata,

    /// Search aggregations
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregations: Option<AggregationResults>,

    #[serde(skip_serializing_if = "Map::is_empty", default)]
    /// Suggest response
    pub suggest: Map<String, Vec<Suggest>>,
}
