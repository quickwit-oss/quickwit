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

use prost_types::Timestamp;
use time::OffsetDateTime;

pub trait TimestampExt {
    fn from_datetime(datetime: OffsetDateTime) -> Timestamp {
        Timestamp {
            seconds: datetime.unix_timestamp() as i64,
            nanos: datetime.nanosecond() as i32,
        }
    }

    fn from_seconds(seconds: u64) -> Timestamp {
        Timestamp {
            seconds: seconds as i64,
            nanos: 0,
        }
    }

    fn from_nanos(nanos: u64) -> Timestamp {
        let seconds = (nanos / 1_000_000_000) as i64;
        let nanos = (nanos % 1_000_000_000) as i32;
        Timestamp { seconds, nanos }
    }

    fn utc_now() -> Timestamp {
        Self::from_datetime(OffsetDateTime::now_utc())
    }

    fn as_datetime(&self) -> OffsetDateTime;
    fn as_seconds(&self) -> u64;
    fn as_millis(&self) -> u64;
    fn as_micros(&self) -> u64;
    fn as_nanos(&self) -> u64;
}

impl TimestampExt for Timestamp {
    fn as_datetime(&self) -> OffsetDateTime {
        let mut timestamp_nanos = self.nanos as i128;
        timestamp_nanos += self.seconds as i128 * 1_000_000_000;

        OffsetDateTime::from_unix_timestamp_nanos(timestamp_nanos)
            .expect("The second and nano components should be in range.")
    }

    fn as_seconds(&self) -> u64 {
        self.seconds as u64
    }

    fn as_millis(&self) -> u64 {
        self.as_seconds() * 1_000 + self.nanos as u64 / 1_000_000
    }

    fn as_micros(&self) -> u64 {
        self.as_seconds() * 1_000_000 + self.nanos as u64 / 1_000
    }

    fn as_nanos(&self) -> u64 {
        self.as_seconds() * 1_000_000_000 + self.nanos as u64
    }
}

pub mod timestamp_serde {
    use serde::{Deserialize, Deserializer, Serializer};

    use super::{Timestamp, TimestampExt};

    pub fn serialize<S>(timestamp: &Timestamp, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_u64(timestamp.as_nanos())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Timestamp, D::Error>
    where D: Deserializer<'de> {
        let nanos = u64::deserialize(deserializer)?;
        Ok(Timestamp::from_nanos(nanos))
    }
}

pub mod timestamp_opt_serde {
    use serde::{Deserialize, Deserializer, Serializer};

    use super::{Timestamp, TimestampExt};

    pub fn serialize<S>(timestamp: &Option<Timestamp>, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        match timestamp {
            Some(timestamp) => super::timestamp_serde::serialize(timestamp, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Timestamp>, D::Error>
    where D: Deserializer<'de> {
        let nanos = Option::<u64>::deserialize(deserializer)?;
        Ok(nanos.map(Timestamp::from_nanos))
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[test]
    fn test_timestamp_ext() {
        let timestamp = Timestamp {
            seconds: 1,
            nanos: 1_001_001,
        };
        assert_eq!(timestamp.as_seconds(), 1);
        assert_eq!(timestamp.as_millis(), 1_001);
        assert_eq!(timestamp.as_micros(), 1_001_001);
        assert_eq!(timestamp.as_nanos(), 1_001_001_001);
    }

    #[test]
    fn test_timestamp_serde() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Foo {
            #[serde(with = "timestamp_serde")]
            timestamp: Timestamp,
            #[serde(with = "timestamp_opt_serde")]
            timestamp_opt: Option<Timestamp>,
        }
        let foo = Foo {
            timestamp: Timestamp {
                seconds: 1,
                nanos: 1_001_001,
            },
            timestamp_opt: Some(Timestamp {
                seconds: 2,
                nanos: 2_002_002,
            }),
        };
        let serialized = serde_json::to_string(&foo).unwrap();
        assert_eq!(
            serialized,
            r#"{"timestamp":1001001001,"timestamp_opt":2002002002}"#
        );
        let deserialized: Foo = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, foo);
    }
}
