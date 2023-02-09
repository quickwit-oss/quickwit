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

mod api_specs;
mod rest_handler;

use std::sync::Arc;

use elasticsearch_dsl::{Sort, SortCollection};
use quickwit_proto::SortOrder;
use quickwit_search::SearchService;
use serde::{Deserialize, Serialize};
use warp::{Filter, Rejection};

use self::rest_handler::{
    elastic_get_index_search_handler, elastic_get_search_handler,
    elastic_post_index_search_handler, elastic_post_search_handler,
};

/// Setup Elasticsearch API handlers
///
/// This is where all newly supported Elasticsearch handlers
/// should be registered.
pub fn elastic_api_handlers(
    search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_get_search_handler()
        .or(elastic_post_search_handler())
        .or(elastic_get_index_search_handler())
        .or(elastic_post_index_search_handler(search_service))
    // Register newly created handlers here.
}

/// Helper type needed by the Elasticsearch endpoints.
/// Control how the total number of hits should be tracked.
///
/// When set to `Track` with a value `true`, the response will always track the number of hits that
/// match the query accurately.
///
/// When set to `Count` with an integer value `n`, the response accurately tracks the total
/// hit count that match the query up to `n` documents.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TrackTotalHits {
    /// Track the number of hits that match the query accurately.
    Track(bool),
    /// Track the number of hits up to the specified value.
    Count(i64),
}

impl From<bool> for TrackTotalHits {
    fn from(b: bool) -> Self {
        TrackTotalHits::Track(b)
    }
}

impl From<i64> for TrackTotalHits {
    fn from(i: i64) -> Self {
        TrackTotalHits::Count(i)
    }
}

pub(crate) fn convert_elastic_sort_syntax(value: &str) -> Result<(i32, String), String> {
    let (field_str, order_str) = value.split_once(':').ok_or_else(|| "".to_string())?;
    let order = match order_str {
        "asc" => SortOrder::Asc as i32,
        "desc" => SortOrder::Desc as i32,
        any => return Err(format!("unknown SortOrder `{any}`")),
    };
    Ok((order, field_str.to_string()))
}

/// Extracts the sort parameter form the queryString or requestBody
///
/// - In ElasticSearch the queryString takes priority over the requestBody.
/// TODO: Currently we don't support sort on multiple fields.
fn extract_sort_by(
    query_param_sort: &Option<Vec<String>>,
    body_sort: SortCollection,
) -> Result<(Option<i32>, Option<String>), String> {
    if let Some(sort) = query_param_sort {
        return match sort.get(0) {
            Some(sort_str) => convert_elastic_sort_syntax(sort_str)
                .map(|(order, field)| (Some(order), Some(field))),
            None => Ok((None, None)),
        };
    }

    match body_sort.into_iter().next() {
        Some(sort) => {
            let (order, field) = match sort {
                Sort::Field(field_name) => (SortOrder::Asc as i32, field_name),
                Sort::FieldSort(field_sort) => (
                    field_sort
                        .order
                        .map(|order| order as i32)
                        .unwrap_or_default(),
                    field_sort.field,
                ),
                _ => {
                    return Err(
                        "Only `Sort::Field` and `Sort::FieldSort` are currently supported"
                            .to_string(),
                    )
                }
            };
            Ok((Some(order), Some(field)))
        }
        None => Ok((None, None)),
    }
}
