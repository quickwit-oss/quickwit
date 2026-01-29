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
use std::ops::Deref;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Custom serde module for ByteSize that serializes as raw byte count (u64).
/// This ensures perfect roundtrip consistency regardless of display format changes
/// in the bytesize crate. Deserialization still accepts human-readable strings
/// like "2 GB" via bytesize's default deserializer.
pub mod bytesize_serde {
    use bytesize::ByteSize;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(byte_size: &ByteSize, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_u64(byte_size.as_u64())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<ByteSize, D::Error>
    where D: Deserializer<'de> {
        ByteSize::deserialize(deserializer)
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(try_from = "String", into = "String")]
pub struct DurationAsStr {
    duration_str: String,
    duration: Duration,
}

impl TryFrom<String> for DurationAsStr {
    type Error = humantime::DurationError;

    fn try_from(duration_str: String) -> Result<Self, Self::Error> {
        let duration = humantime::parse_duration(&duration_str)?;
        Ok(DurationAsStr {
            duration_str,
            duration,
        })
    }
}

impl From<DurationAsStr> for String {
    fn from(duration_as_str: DurationAsStr) -> String {
        duration_as_str.duration_str
    }
}

impl Deref for DurationAsStr {
    type Target = Duration;

    fn deref(&self) -> &Self::Target {
        &self.duration
    }
}

impl From<DurationAsStr> for Duration {
    fn from(duration_as_str: DurationAsStr) -> Self {
        *duration_as_str
    }
}

impl fmt::Debug for DurationAsStr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.duration_str.fmt(f)
    }
}

impl PartialEq for DurationAsStr {
    fn eq(&self, other: &Self) -> bool {
        // We do not check for the chosen representation here
        self.duration == other.duration
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_duration_deserialize() {
        let duration: DurationAsStr = serde_json::from_str("\"10s\"").unwrap();
        assert_eq!(*duration, Duration::from_secs(10));
        let deser_error = serde_json::from_str::<DurationAsStr>("\"10\"").unwrap_err();
        assert_eq!(
            deser_error.to_string(),
            "time unit needed, for example 10sec or 10ms"
        );
    }
}
