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
use std::str::FromStr;

use thiserror::Error;

/// Where a metric must be shipped.
///
/// Wire forms:
/// - integer (1=saas, 2=byoc, 3=dual) — matches the Go
///   `byoc-dualship-mgr` API mapping
/// - lowercase string (`"saas"`, `"byoc"`, `"dual"`) — matches the CSV format
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Destination {
    Saas,
    Byoc,
    Dual,
}

impl Destination {
    pub fn as_str(self) -> &'static str {
        match self {
            Destination::Saas => "saas",
            Destination::Byoc => "byoc",
            Destination::Dual => "dual",
        }
    }

    pub fn from_api_int(value: i32) -> Result<Self, DestinationParseError> {
        match value {
            1 => Ok(Destination::Saas),
            2 => Ok(Destination::Byoc),
            3 => Ok(Destination::Dual),
            other => Err(DestinationParseError::UnknownInt(other)),
        }
    }
}

impl fmt::Display for Destination {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for Destination {
    type Err = DestinationParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "saas" => Ok(Destination::Saas),
            "byoc" => Ok(Destination::Byoc),
            "dual" => Ok(Destination::Dual),
            other => Err(DestinationParseError::UnknownString(other.to_string())),
        }
    }
}

#[derive(Debug, Error)]
pub enum DestinationParseError {
    #[error("unknown destination integer: {0}")]
    UnknownInt(i32),
    #[error("unknown destination string: {0:?}")]
    UnknownString(String),
}

/// A single record returned by the metadata service describing where a metric
/// should be shipped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricRecord {
    pub name: String,
    pub destination: Destination,
    /// Unix-seconds timestamp of the last update for this metric.
    /// Used to advance the poller's watermark.
    pub last_updated_unix: i64,
}

/// Counts of mutations applied by [`crate::transforms::metric_dual_ship::store::DualShipStore::merge`]
/// or [`crate::transforms::metric_dual_ship::store::DualShipStore::replace`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ChangeSet {
    pub added: u32,
    pub updated: u32,
    pub removed: u32,
}

impl ChangeSet {
    pub fn total(self) -> u32 {
        self.added + self.updated + self.removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn destination_from_api_int_known_values() {
        assert_eq!(Destination::from_api_int(1).unwrap(), Destination::Saas);
        assert_eq!(Destination::from_api_int(2).unwrap(), Destination::Byoc);
        assert_eq!(Destination::from_api_int(3).unwrap(), Destination::Dual);
    }

    #[test]
    fn destination_from_api_int_rejects_unknown() {
        let err = Destination::from_api_int(0).unwrap_err();
        assert!(matches!(err, DestinationParseError::UnknownInt(0)));
    }

    #[test]
    fn destination_from_str_roundtrip() {
        for dest in [Destination::Saas, Destination::Byoc, Destination::Dual] {
            assert_eq!(dest.as_str().parse::<Destination>().unwrap(), dest);
        }
    }

    #[test]
    fn destination_from_str_rejects_unknown() {
        let err = "other".parse::<Destination>().unwrap_err();
        assert!(matches!(err, DestinationParseError::UnknownString(_)));
    }

    #[test]
    fn changeset_total_sums_components() {
        let cs = ChangeSet {
            added: 1,
            updated: 2,
            removed: 3,
        };
        assert_eq!(cs.total(), 6);
    }
}
