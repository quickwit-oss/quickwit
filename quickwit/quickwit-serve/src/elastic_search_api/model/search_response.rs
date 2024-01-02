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

use std::collections::BTreeMap;

use elasticsearch_dsl::{ClusterStatistics, HitsMetadata, ShardStatistics, Suggest};
use quickwit_proto::search::MatchingKeywordsByFieldName;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Search response
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ElasticSearchResponse {
    /// The time that it took Elasticsearch to process the query
    pub took: u32,

    /// The search has been cancelled and results are partial
    pub timed_out: bool,

    /// Indicates if search has been terminated early
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminated_early: Option<bool>,

    /// Scroll Id
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "_scroll_id")]
    pub scroll_id: Option<String>,

    /// Dynamically fetched fields
    #[serde(default)]
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub fields: BTreeMap<String, Value>,

    /// Point in time Id
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pit_id: Option<String>,

    /// Number of reduce phases
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_reduce_phases: Option<u64>,

    /// Maximum document score. [None] when documents are implicitly sorted
    /// by a field other than `_score`
    #[serde(default)]
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
    pub aggregations: Option<Value>,

    /// Suggest response
    #[serde(default)]
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub suggest: BTreeMap<String, Vec<Suggest>>,

    /// Matching keywords by field name
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub matching_keywords_by_field_name: Vec<MatchingKeywordsByFieldName>,
}
