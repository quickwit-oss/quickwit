// Copyright (C) 2022 Quickwit, Inc.
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

use std::convert::Infallible;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
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
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_get_search_handler()
        .or(elastic_post_search_handler())
        .or(elastic_get_index_search_handler())
        .or(elastic_post_index_search_handler())
    // Register newly created handlers here.
}

/// A helper struct to serialize/deserialize a comma separated list.
#[derive(Debug, Deserialize)]
pub(crate) struct SimpleList(Vec<String>);

impl FromStr for SimpleList {
    type Err = Infallible;

    fn from_str(str_sequence: &str) -> Result<Self, Self::Err> {
        let items = str_sequence
            .trim_matches(',')
            .split(',')
            .map(|item| item.to_owned())
            .collect::<Vec<_>>();
        Ok(Self(items))
    }
}

/// Serializes an `Option<&[Serialize]>` with
/// `Some(value)` to a comma separated string of values.
/// Used to serialize values within the query string
pub(crate) fn to_simple_list<S, T>(
    value: &Option<Vec<T>>,
    serializer: S,
) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
where
    S: Serializer,
    T: ToString,
{
    let vec = &value
        .as_ref()
        .expect("attempt to serialize Option::None value");

    let serialized_str = vec
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>() // do not collect here
        .join(",");

    serializer.serialize_str(&serialized_str)
}

/// Deserializes a comma separated string of values
/// into a [`Vec<T>`].
/// Used to deserialize list of values from the query string.
pub(crate) fn from_simple_list<'de, D, T>(deserializer: D) -> Result<Option<Vec<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    <T as FromStr>::Err: ToString,
{
    let str_sequence = String::deserialize(deserializer)?;
    let list = str_sequence
        .trim_matches(',')
        .split(',')
        .map(|item| T::from_str(item))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| serde::de::Error::custom(err.to_string()))?;
    Ok(Some(list))
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
