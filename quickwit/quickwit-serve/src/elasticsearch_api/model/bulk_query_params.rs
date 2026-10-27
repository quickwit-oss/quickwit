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

use quickwit_ingest::CommitType;
use quickwit_proto::ingest::CommitTypeV2;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer};

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq)]
pub struct ElasticBulkOptions {
    #[serde(default)]
    pub refresh: ElasticRefresh,
    #[serde(default)]
    pub use_legacy_ingest: bool,
}

/// ?refresh parameter for elasticsearch bulk request
///
/// The syntax for this parameter is a bit confusing for backward compatibility reasons.
/// - Absence of ?refresh parameter or ?refresh=false means no refresh
/// - Presence of ?refresh parameter without any values or ?refresh=true means force refresh
/// - ?refresh=wait_for means wait for refresh
#[derive(Clone, Copy, Debug, Default, PartialEq, utoipa::ToSchema)]
#[schema(rename_all = "snake_case")]
pub enum ElasticRefresh {
    // if the refresh parameter is not present it is false
    #[default]
    /// The request doesn't wait for commit
    False,
    /// The request forces an immediate commit after the last document in the batch and waits for
    /// it to finish.
    ///
    /// This is also what an empty string, i.e. a bare `?refresh` with no value, evaluates to.
    True,
    /// The request will wait for the next scheduled commit to finish.
    WaitFor,
}

const ELASTIC_REFRESH_VARIANTS: &[&str] = &["false", "", "true", "wait_for"];

// Custom `Deserialize` impl because the `?refresh` parameter without a value (e.g. `?refresh`,
// as opposed to `?refresh=`) must be treated as `true`. Depending on the query string
// deserializer, a valueless parameter is presented either as an empty string or as a unit value,
// so both must be handled here.
impl<'de> Deserialize<'de> for ElasticRefresh {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        struct ElasticRefreshVisitor;

        impl Visitor<'_> for ElasticRefreshVisitor {
            type Value = ElasticRefresh;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a refresh value (`false`, ``, `true`, or `wait_for`)")
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where E: de::Error {
                Ok(ElasticRefresh::True)
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where E: de::Error {
                match value {
                    "false" => Ok(ElasticRefresh::False),
                    "" | "true" => Ok(ElasticRefresh::True),
                    "wait_for" => Ok(ElasticRefresh::WaitFor),
                    other => Err(de::Error::unknown_variant(other, ELASTIC_REFRESH_VARIANTS)),
                }
            }
        }
        deserializer.deserialize_any(ElasticRefreshVisitor)
    }
}

impl From<ElasticRefresh> for CommitType {
    fn from(val: ElasticRefresh) -> Self {
        match val {
            ElasticRefresh::False => Self::Auto,
            ElasticRefresh::True => Self::Force,
            ElasticRefresh::WaitFor => Self::WaitFor,
        }
    }
}

impl From<ElasticRefresh> for CommitTypeV2 {
    fn from(val: ElasticRefresh) -> Self {
        match val {
            ElasticRefresh::False => Self::Auto,
            ElasticRefresh::True => Self::Force,
            ElasticRefresh::WaitFor => Self::WaitFor,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::elasticsearch_api::model::ElasticBulkOptions;
    use crate::elasticsearch_api::model::bulk_query_params::ElasticRefresh;

    #[test]
    fn test_elastic_refresh_parsing() {
        assert_eq!(
            serde_qs::from_str::<ElasticBulkOptions>("")
                .unwrap()
                .refresh,
            ElasticRefresh::False
        );
        assert_eq!(
            serde_qs::from_str::<ElasticBulkOptions>("refresh=true")
                .unwrap()
                .refresh,
            ElasticRefresh::True
        );
        assert_eq!(
            serde_qs::from_str::<ElasticBulkOptions>("refresh=false")
                .unwrap()
                .refresh,
            ElasticRefresh::False
        );
        assert_eq!(
            serde_qs::from_str::<ElasticBulkOptions>("refresh=wait_for")
                .unwrap()
                .refresh,
            ElasticRefresh::WaitFor
        );
        assert_eq!(
            serde_qs::from_str::<ElasticBulkOptions>("refresh")
                .unwrap()
                .refresh,
            ElasticRefresh::True
        );
        assert_eq!(
            serde_qs::from_str::<ElasticBulkOptions>("refresh=wait")
                .unwrap_err()
                .to_string(),
            "unknown variant `wait`, expected one of `false`, ``, `true`, `wait_for`"
        );
    }
}
