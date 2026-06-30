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
use std::path::Path;
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
    pub fn new() -> Self {
        SplitId::from(ulid::Ulid::new().to_string())
    }

    /// Returns the id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<Path> for SplitId {
    fn as_ref(&self) -> &Path {
        Path::new(self.as_str())
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
}
