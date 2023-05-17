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

use serde::{self, Deserialize, Serialize, Serializer};
use warp::{Filter, Rejection};

/// Body output format used for the REST API.
#[derive(Deserialize, Clone, Debug, Eq, PartialEq, Copy, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum BodyFormat {
    Json,
    #[default]
    PrettyJson,
}

impl BodyFormat {
    pub(crate) fn result_to_vec<T: serde::Serialize, E: serde::Serialize>(
        &self,
        result: &Result<T, E>,
    ) -> Result<Vec<u8>, ()> {
        match result {
            Ok(value) => self.value_to_vec(value),
            Err(err) => self.value_to_vec(err),
        }
    }

    fn value_to_vec(&self, value: &impl serde::Serialize) -> Result<Vec<u8>, ()> {
        match &self {
            Self::Json => serde_json::to_vec(value).map_err(|_| {
                tracing::error!("Error: the response serialization failed.");
            }),
            Self::PrettyJson => serde_json::to_vec_pretty(value).map_err(|_| {
                tracing::error!("Error: the response serialization failed.");
            }),
        }
    }
}

impl ToString for BodyFormat {
    fn to_string(&self) -> String {
        match &self {
            Self::Json => "json".to_string(),
            Self::PrettyJson => "pretty_json".to_string(),
        }
    }
}

impl Serialize for BodyFormat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_str(&self.to_string())
    }
}

/// This struct represents a QueryString passed to
/// the REST API.
#[derive(Deserialize, Debug, Eq, PartialEq, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
struct FormatQueryString {
    /// The output format requested.
    #[serde(default)]
    pub format: BodyFormat,
}

pub(crate) fn extract_format_from_qs(
) -> impl Filter<Extract = (BodyFormat,), Error = Rejection> + Clone {
    serde_qs::warp::query::<FormatQueryString>(serde_qs::Config::default())
        .map(|format_qs: FormatQueryString| format_qs.format)
}
