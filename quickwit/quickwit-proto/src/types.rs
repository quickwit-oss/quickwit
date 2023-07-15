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
use serde::{Deserialize, Serialize, Serializer, Deserializer};
use time::{OffsetDateTime, PrimitiveDateTime};

#[derive(Clone, Copy, PartialEq, ::prost::Message, utoipa::ToSchema)]
pub struct Timestamp {
    /// Represents seconds of UTC time since Unix epoch
    /// 1970-01-01T00:00:00Z. Must be from 0001-01-01T00:00:00Z to
    /// 9999-12-31T23:59:59Z inclusive.
    #[prost(int64, tag = "1")]
    pub seconds: i64,
    /// Non-negative fractions of a second at nanosecond resolution. Negative
    /// second values with fractions must still have non-negative nanos values
    /// that count forward in time. Must be from 0 to 999,999,999
    /// inclusive.
    #[prost(int32, tag = "2")]
    pub nanos: i32,
}

impl Timestamp {
    pub fn new(seconds: i64, nanos: i32) -> Self {
        Self { seconds, nanos }
    }

    pub fn from_secs(seconds: i64) -> Self {
        Self {
            seconds,
            nanos: 0,
        }
    }

    pub fn as_secs(&self) -> i64 {
        self.seconds
    }

    pub fn as_millis(&self) -> i64 {
        self.seconds * 1_000 + self.nanos as i64 / 1_000_000
    }

    pub fn as_nanos(&self) -> i64 {
        self.seconds * 1_000_000_000 + self.nanos as i64
    }

    pub fn from_datetime(datetime: OffsetDateTime) -> Self {
        let nanos = datetime.unix_timestamp_nanos();
        let seconds = (nanos / 1_000_000_000) as i64;
        let nanos = (nanos % 1_000_000_000) as i32;
        Self { seconds, nanos }
    }

    pub fn as_datetime(&self) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp_nanos(self.as_nanos() as i128).expect("Timestamp should be in range.")
    }

    pub fn now_utc() -> Self {
        Self::from_datetime(OffsetDateTime::now_utc())
    }
}

impl From<OffsetDateTime> for Timestamp {
    fn from(datetime: OffsetDateTime) -> Self {
        Self::from_datetime(datetime)
    }
}

impl From<PrimitiveDateTime> for Timestamp {
    fn from(datetime: PrimitiveDateTime) -> Self {
        Self::from_datetime(datetime.assume_utc())
    }
}

impl Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        let timestamp_nanos = self.seconds * 1_000_000_000 + self.nanos as i64;
        serializer.serialize_i64(timestamp_nanos)
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let timestamp_nanos = i64::deserialize(deserializer)?;
        let seconds = timestamp_nanos / 1_000_000_000;
        let nanos = (timestamp_nanos % 1_000_000_000) as i32;
        Ok(Timestamp { seconds, nanos })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp() {
        let timestamp = Timestamp::new(1, 1_000_000);
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
            timestamp: Timestamp::new(1, 1_000_000),
            timestamp_opt: Some(Timestamp::new(2, 2_000_000)),
        };
        let foo_serialized = serde_json::to_string(&foo).unwrap();
        assert_eq!(foo_serialized, r#"{"timestamp":1001000000,"timestamp_opt":2002000000}"#);
        let foo_deserialized: Foo = serde_json::from_str(&foo_serialized).unwrap();
        assert_eq!(foo_deserialized, foo);
    }
}
