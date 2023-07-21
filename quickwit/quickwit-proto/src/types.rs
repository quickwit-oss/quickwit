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

use std::fmt;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

pub type SourceId = String;
pub type SplitId = String;

/// Index identifiers that uniquely identify not only the index, but also
/// its incarnation allowing to distinguish between deleted and recreated indexes.
/// It is represented as a stiring in index_id:incarnation_id format.
#[derive(Clone, Default, PartialEq, Eq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct IndexUid(String);

impl IndexUid {
    /// Creates a new index UID from an `index_id` and `incarnation_id`.
    pub fn new(index_id: impl Into<String>) -> Self {
        Self::from_parts(index_id, Ulid::new().to_string())
    }

    pub fn from_parts(index_id: impl Into<String>, incarnation_id: impl Into<String>) -> Self {
        let incarnation_id = incarnation_id.into();
        let index_id = index_id.into();
        if incarnation_id.is_empty() {
            Self(index_id)
        } else {
            Self(format!("{index_id}:{incarnation_id}"))
        }
    }

    pub fn index_id(&self) -> &str {
        self.0.split(':').next().unwrap()
    }

    pub fn incarnation_id(&self) -> &str {
        if let Some(incarnation_id) = self.0.split(':').nth(1) {
            incarnation_id
        } else {
            ""
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Display for IndexUid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for IndexUid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexUid")
            .field("index_id", &self.index_id())
            .field("incarnation_id", &self.incarnation_id())
            .finish()
    }
}

impl From<IndexUid> for String {
    fn from(val: IndexUid) -> Self {
        val.0
    }
}

impl From<String> for IndexUid {
    fn from(index_uid: String) -> Self {
        IndexUid(index_uid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_uid_parsing() {
        assert_eq!("foo", IndexUid::from("foo".to_string()).index_id());
        assert_eq!("foo", IndexUid::from("foo:bar".to_string()).index_id());
        assert_eq!("", IndexUid::from("foo".to_string()).incarnation_id());
        assert_eq!(
            "bar",
            IndexUid::from("foo:bar".to_string()).incarnation_id()
        );
    }

    #[test]
    fn test_index_uid_roundtrip() {
        assert_eq!("foo", IndexUid::from("foo".to_string()).to_string());
        assert_eq!("foo:bar", IndexUid::from("foo:bar".to_string()).to_string());
    }

    #[test]
    fn test_index_uid_roundtrip_using_parts() {
        assert_eq!("foo", index_uid_roundtrip_using_parts("foo"));
        assert_eq!("foo:bar", index_uid_roundtrip_using_parts("foo:bar"));
    }

    fn index_uid_roundtrip_using_parts(index_uid: &str) -> String {
        let index_uid = IndexUid::from(index_uid.to_string());
        let index_id = index_uid.index_id();
        let incarnation_id = index_uid.incarnation_id();
        let index_uid_from_parts = IndexUid::from_parts(index_id, incarnation_id);
        index_uid_from_parts.to_string()
    }
}
