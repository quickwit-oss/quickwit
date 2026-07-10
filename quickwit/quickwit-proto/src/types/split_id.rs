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

use std::borrow::Borrow;
use std::convert::Infallible;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// Identifies a split.
///
/// Joined with the index URI (`<index URI>/<split ID>`), this ID is enough to
/// uniquely identify a split.
///
/// Split IDs are currently generated as ULIDs (see
/// `quickwit_indexing::new_split_id`), but the rest of the codebase must treat
/// them as **opaque** strings: no part of the system may assume the id is a
/// ULID or extract information (such as a creation timestamp) from it. This
/// indirection is what allows the generator to prepend a random prefix for S3
/// load distribution without rippling through the codebase.
///
/// The inner storage is an [`Arc<str>`] so that cloning is cheap (a split id is
/// frequently used as a cache key and copied around the search and indexing
/// pipelines).
#[derive(Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SplitId(Arc<str>);

impl SplitId {
    /// Generates a new split id.
    ///
    /// When the `QW_RANDOM_SPLIT_PREFIX` environment variable is set to `true`,
    /// the ID is prefixed with 4 random characters taken from the tail of the
    /// ULID. This spreads S3 key prefixes across partitions and avoids hot-spots
    /// when writes concentrate on the same time window.
    ///
    /// Layout with prefix enabled: `<4 random chars>_<22 remaining ULID chars>`
    /// (27 chars total vs the usual 26).
    pub fn new() -> Self {
        static RANDOM_PREFIX_ENABLED: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| {
            quickwit_common::get_bool_from_env("QW_RANDOM_SPLIT_PREFIX", false)
        });
        Self::from_ulid(ulid::Ulid::new(), *RANDOM_PREFIX_ENABLED)
    }

    /// Constructs a `SplitId` from a `Ulid`, optionally prepending a random prefix.
    ///
    /// The prefix is the last 4 characters of the ULID string (pure random bits),
    /// separated from the remaining 22 characters by `_`.
    pub fn from_ulid(ulid: ulid::Ulid, random_prefix: bool) -> Self {
        let ulid_str = ulid.to_string(); // 26 Crockford base32 chars
        if random_prefix {
            // The last 4 characters of a ULID are random bits.
            // Moving them to a prefix spreads S3 key prefixes while keeping the
            // full ULID entropy in the string.
            let prefix = &ulid_str[22..]; // chars 22-25: random section
            let body = &ulid_str[..22]; // chars 0-21: timestamp + most of random
            SplitId::from(format!("{prefix}_{body}"))
        } else {
            SplitId::from(ulid_str)
        }
    }

    /// Returns the id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// `Debug` delegates to the inner string so a split id renders identically to the
// `String` it replaced (e.g. `"01HAV29D4XY3D462FS3D8K5Q2H"`, not
// `SplitId("01HAV29D4XY3D462FS3D8K5Q2H")`).
impl fmt::Debug for SplitId {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&self.0, formatter)
    }
}

impl fmt::Display for SplitId {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<String> for SplitId {
    fn from(split_id: String) -> Self {
        SplitId(split_id.into())
    }
}

impl From<SplitId> for String {
    fn from(split_id: SplitId) -> Self {
        split_id.0.as_ref().to_owned()
    }
}

impl From<&SplitId> for String {
    fn from(split_id: &SplitId) -> Self {
        split_id.0.as_ref().to_owned()
    }
}

impl From<&str> for SplitId {
    fn from(split_id: &str) -> Self {
        SplitId(Arc::from(split_id))
    }
}

impl FromStr for SplitId {
    // A split id imposes no constraint on its content, so parsing never fails.
    type Err = Infallible;

    fn from_str(split_id_str: &str) -> Result<Self, Self::Err> {
        Ok(SplitId::from(split_id_str))
    }
}

impl AsRef<str> for SplitId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

// Enables looking up a `HashMap<SplitId, _>` or `BTreeMap<SplitId, _>` with a `&str` key.
impl Borrow<str> for SplitId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for SplitId {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<String> for SplitId {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<&str> for SplitId {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_id_serde_is_transparent() {
        let split_id = SplitId::from("01HAV29D4XY3D462FS3D8K5Q2H");
        let serialized = serde_json::to_string(&split_id).unwrap();
        assert_eq!(serialized, "\"01HAV29D4XY3D462FS3D8K5Q2H\"");
        let deserialized: SplitId = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, split_id);
    }

    #[test]
    fn test_split_id_borrow_as_str() {
        use std::collections::HashMap;

        let mut map: HashMap<SplitId, u32> = HashMap::new();
        map.insert(SplitId::from("split-1"), 42);
        assert_eq!(map.get("split-1"), Some(&42));
    }

    #[test]
    fn test_split_id_ordering_is_lexicographic() {
        let split_id_a = SplitId::from("aaa");
        let split_id_b = SplitId::from("bbb");
        assert!(split_id_a < split_id_b);
    }

    #[test]
    fn test_split_id_from_ulid_without_prefix() {
        let ulid = ulid::Ulid::from_string("01HAV29D4XY3D462FS3D8K5Q2H").unwrap();
        let split_id = SplitId::from_ulid(ulid, false);
        assert_eq!(split_id.as_str(), "01HAV29D4XY3D462FS3D8K5Q2H");
    }

    #[test]
    fn test_split_id_from_ulid_with_prefix() {
        // "01HAV29D4XY3D462FS3D8K5Q2H": last 4 chars = "5Q2H", first 22 = "01HAV29D4XY3D462FS3D8K".
        let ulid = ulid::Ulid::from_string("01HAV29D4XY3D462FS3D8K5Q2H").unwrap();
        let split_id = SplitId::from_ulid(ulid, true);
        assert_eq!(split_id.as_str(), "5Q2H_01HAV29D4XY3D462FS3D8K");
        assert_eq!(split_id.as_str().len(), 27);
    }
}
