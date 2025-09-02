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

use std::fmt;

use quickwit_config::ConfigFormat;
use serde::{self, Deserialize, Serialize, Serializer};
use thiserror::Error;
use warp::hyper::header::CONTENT_TYPE;
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
            Self::Json => serde_json::to_vec(value),
            Self::PrettyJson => serde_json::to_vec_pretty(value),
        }
        .map_err(|_| {
            tracing::error!("response serialization failed");
        })
    }
}

impl fmt::Display for BodyFormat {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Self::Json => write!(formatter, "json"),
            Self::PrettyJson => write!(formatter, "pretty_json"),
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

pub(crate) fn extract_format_from_qs()
-> impl Filter<Extract = (BodyFormat,), Error = Rejection> + Clone {
    warp::query::<FormatQueryString>().map(|format_qs: FormatQueryString| format_qs.format)
}

#[derive(Debug, Error)]
#[error(
    "request's content-type is not supported: supported media types are `application/json`, \
     `application/toml`, and `application/yaml`"
)]
pub(crate) struct UnsupportedMediaType;

impl warp::reject::Reject for UnsupportedMediaType {}

pub(crate) fn extract_config_format()
-> impl Filter<Extract = (ConfigFormat,), Error = Rejection> + Copy {
    warp::filters::header::optional::<mime_guess::Mime>(CONTENT_TYPE.as_str()).and_then(
        |mime_opt: Option<mime_guess::Mime>| {
            if let Some(mime) = mime_opt {
                let config_format = match mime.subtype().as_str() {
                    "json" => ConfigFormat::Json,
                    "toml" => ConfigFormat::Toml,
                    "yaml" => ConfigFormat::Yaml,
                    _ => {
                        return futures::future::err(warp::reject::custom(UnsupportedMediaType));
                    }
                };
                return futures::future::ok(config_format);
            }
            futures::future::ok(ConfigFormat::Json)
        },
    )
}
