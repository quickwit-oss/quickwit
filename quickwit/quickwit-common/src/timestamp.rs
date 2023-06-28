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

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// A unix timestamp with millisecond precision for storing event timestamps such as create or
/// update timestamps.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Timestamp(i64);

impl Timestamp {
    pub fn from_datetime(datetime: OffsetDateTime) -> Self {
        let nanos = datetime.unix_timestamp_nanos();
        let millis = nanos / 1_000_000;
        Self(millis as i64)
    }

    pub fn from_secs(seconds: i64) -> Self {
        Self(seconds * 1000)
    }

    pub fn from_millis(milliseconds: i64) -> Self {
        Self(milliseconds)
    }

    pub fn now_utc() -> Self {
        Self::from_datetime(OffsetDateTime::now_utc())
    }

    pub fn as_datetime(&self) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp_nanos(self.0 as i128 * 1_000_000)
            .expect(" should be in range.")
    }

    pub fn as_secs(&self) -> i64 {
        self.0 / 1000
    }

    pub fn as_millis(&self) -> i64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp() {
        let timestamp = Timestamp(1_001);
        assert_eq!(timestamp.as_secs(), 1);
        assert_eq!(timestamp.as_millis(), 1_001);
    }

    #[test]
    fn test_timestamp_serde() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Foo {
            timestamp: Timestamp,
            timestamp_opt: Option<Timestamp>,
        }
        let foo = Foo {
            timestamp: Timestamp::from_millis(1001),
            timestamp_opt: Some(Timestamp::from_millis(2002)),
        };
        let serialized = serde_json::to_string(&foo).unwrap();
        assert_eq!(serialized, r#"{"timestamp":1001,"timestamp_opt":2002}"#);
        let deserialized: Foo = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, foo);
    }
}
