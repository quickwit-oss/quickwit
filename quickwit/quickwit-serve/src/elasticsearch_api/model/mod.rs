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

mod bulk_body;
mod bulk_query_params;
mod cat_indices;
mod error;
mod field_capability;
mod multi_search;
mod scroll;
mod search_body;
mod search_query_params;
mod search_response;
mod stats;

pub use bulk_body::BulkAction;
pub use bulk_query_params::ElasticBulkOptions;
pub use cat_indices::{
    CatIndexQueryParams, ElasticsearchCatIndexResponse, ElasticsearchResolveIndexEntryResponse,
    ElasticsearchResolveIndexResponse,
};
pub use error::{ElasticException, ElasticsearchError};
pub use field_capability::{
    FieldCapabilityQueryParams, FieldCapabilityRequestBody, FieldCapabilityResponse,
    build_list_field_request_for_es_api, convert_to_es_field_capabilities_response,
};
pub use multi_search::{
    MultiSearchHeader, MultiSearchQueryParams, MultiSearchResponse, MultiSearchSingleResponse,
};
use quickwit_proto::search::{SortDatetimeFormat, SortOrder};
pub use scroll::ScrollQueryParams;
pub use search_body::SearchBody;
pub use search_query_params::{DeleteQueryParams, SearchQueryParams, SearchQueryParamsCount};
pub use search_response::ElasticsearchResponse;
use serde::{Deserialize, Serialize};
pub use stats::{ElasticsearchStatsResponse, StatsResponseEntry};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SortField {
    pub field: String,
    pub order: SortOrder,
    pub date_format: Option<ElasticDateFormat>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ElasticDateFormat {
    /// Sort values are in milliseconds by default to ease migration from ES.
    /// We allow the user to specify nanoseconds if needed.
    /// We add `Int` to the name to avoid confusion ES variant `EpochMillis` which,
    /// returns milliseconds as strings.
    EpochNanosInt,
}

impl From<ElasticDateFormat> for SortDatetimeFormat {
    fn from(date_format: ElasticDateFormat) -> Self {
        match date_format {
            ElasticDateFormat::EpochNanosInt => SortDatetimeFormat::UnixTimestampNanos,
        }
    }
}

pub(crate) fn default_elasticsearch_sort_order(field_name: &str) -> SortOrder {
    if field_name == "_score" {
        SortOrder::Desc
    } else {
        SortOrder::Asc
    }
}
