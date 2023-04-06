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

use std::collections::BTreeSet;

use quickwit_proto::SortOrder;
use quickwit_query::ElasticQueryDsl;
use serde::{Deserialize, Serialize};

use crate::elastic_search_api::TrackTotalHits;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldSort {
    field: String,
    order: Option<SortOrder>,
}

#[derive(Debug, Default, Clone, Deserialize, PartialEq)]
pub struct SearchBody {
    #[serde(default)]
    pub from: Option<u64>,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default)]
    pub query: Option<ElasticQueryDsl>,
    #[serde(default)]
    pub sort: Option<Vec<FieldSort>>,
    #[serde(default)]
    pub aggs: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    pub track_total_hits: Option<TrackTotalHits>,
    #[serde(default)]
    pub stored_fields: Option<BTreeSet<String>>,
}
