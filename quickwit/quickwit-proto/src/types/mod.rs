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

use std::borrow::Borrow;
use std::convert::Infallible;
use std::fmt;
use std::fmt::Display;
use std::ops::Deref;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
pub use ulid::Ulid;

mod position;

pub use position::Position;

pub type IndexId = String;

pub type SourceId = String;

pub type SplitId = String;

pub type ShardId = u64;

/// See the file `ingest.proto` for more details.
pub type PublishToken = String;

/// Uniquely identifies a shard and its underlying mrecordlog queue.
pub type QueueId = String; // <index_uid>/<source_id>/<shard_id>

pub fn queue_id(index_uid: &str, source_id: &str, shard_id: u64) -> QueueId {
    format!("{}/{}/{}", index_uid, source_id, shard_id)
}

pub fn split_queue_id(queue_id: &str) -> Option<(IndexUid, SourceId, ShardId)> {
    let mut parts = queue_id.split('/');
    let index_uid = parts.next()?;
    let source_id = parts.next()?;
    let shard_id = parts.next()?.parse::<u64>().ok()?;
    Some((index_uid.into(), source_id.to_string(), shard_id))
}

/// Index identifiers that uniquely identify not only the index, but also
/// its incarnation allowing to distinguish between deleted and recreated indexes.
/// It is represented as a stiring in index_id:incarnation_id format.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct IndexUid(String);

impl fmt::Display for IndexUid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl IndexUid {
    /// Creates a new index uid from index_id.
    /// A random UUID will be used as incarnation
    pub fn new(index_id: impl Into<String>) -> Self {
        Self::from_parts(index_id, Ulid::new().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
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

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<IndexUid> for String {
    fn from(val: IndexUid) -> Self {
        val.0
    }
}

impl From<&str> for IndexUid {
    fn from(index_uid: &str) -> Self {
        Self(index_uid.to_string())
    }
}

impl From<String> for IndexUid {
    fn from(index_uid: String) -> Self {
        Self(index_uid)
    }
}

impl PartialEq<&str> for IndexUid {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(String);

// #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
// pub struct GenerationId(u64);

// #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
// pub struct NodeUid(NodeId, GenerationId);

impl NodeId {
    /// Constructs a new [`NodeId`].
    pub const fn new(node_id: String) -> Self {
        Self(node_id)
    }

    /// Takes ownership of the underlying [`String`], consuming `self`.
    pub fn take(self) -> String {
        self.0
    }
}

impl AsRef<str> for NodeId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<NodeIdRef> for NodeId {
    fn as_ref(&self) -> &NodeIdRef {
        self.deref()
    }
}

impl Borrow<str> for NodeId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl Borrow<NodeIdRef> for NodeId {
    fn borrow(&self) -> &NodeIdRef {
        self.deref()
    }
}

impl Deref for NodeId {
    type Target = NodeIdRef;

    fn deref(&self) -> &Self::Target {
        NodeIdRef::from_str(&self.0)
    }
}

impl Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&'_ str> for NodeId {
    fn from(node_id: &str) -> Self {
        Self::new(node_id.to_string())
    }
}

impl From<String> for NodeId {
    fn from(node_id: String) -> Self {
        Self::new(node_id)
    }
}

impl From<NodeId> for String {
    fn from(node_id: NodeId) -> Self {
        node_id.0
    }
}

impl From<&'_ NodeIdRef> for NodeId {
    fn from(node_id: &NodeIdRef) -> Self {
        node_id.to_owned()
    }
}

impl FromStr for NodeId {
    type Err = Infallible;

    fn from_str(node_id: &str) -> Result<Self, Self::Err> {
        Ok(NodeId::new(node_id.to_string()))
    }
}

impl PartialEq<&str> for NodeId {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

#[repr(transparent)]
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeIdRef(str);

impl NodeIdRef {
    /// Transparently reinterprets the string slice as a strongly-typed [`NodeIdRef`].
    pub const fn from_str(node_id: &str) -> &Self {
        let ptr: *const str = node_id;
        // SAFETY: `NodeIdRef` is `#[repr(transparent)]` around a single `str` field, so a `*const
        // str` can be safely reinterpreted as a `*const NodeIdRef`
        unsafe { &*(ptr as *const Self) }
    }

    /// Transparently reinterprets the static string slice as a strongly-typed [`NodeIdRef`].
    pub const fn from_static(node_id: &'static str) -> &'static Self {
        Self::from_str(node_id)
    }

    /// Provides access to the underlying value as a string slice.
    pub const fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for NodeIdRef {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<String> for NodeId {
    fn borrow(&self) -> &String {
        &self.0
    }
}

impl Borrow<str> for NodeIdRef {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl<'a> From<&'a str> for &'a NodeIdRef {
    fn from(node_id: &'a str) -> &'a NodeIdRef {
        NodeIdRef::from_str(node_id)
    }
}

impl PartialEq<NodeIdRef> for NodeId {
    fn eq(&self, other: &NodeIdRef) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<&'_ NodeIdRef> for NodeId {
    fn eq(&self, other: &&NodeIdRef) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<NodeId> for NodeIdRef {
    fn eq(&self, other: &NodeId) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<NodeId> for &'_ NodeIdRef {
    fn eq(&self, other: &NodeId) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<NodeId> for String {
    fn eq(&self, other: &NodeId) -> bool {
        self.as_str() == other.as_str()
    }
}

impl ToOwned for NodeIdRef {
    type Owned = NodeId;

    fn to_owned(&self) -> Self::Owned {
        NodeId(self.0.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_id() {
        assert_eq!(
            queue_id("test-index:0", "test-source", 1),
            "test-index:0/test-source/1"
        );
    }

    #[test]
    fn test_split_queue_id() {
        let splits = split_queue_id("test-index:0");
        assert!(splits.is_none());

        let splits = split_queue_id("test-index:0/test-source");
        assert!(splits.is_none());

        let splits = split_queue_id("test-index:0/test-source/a");
        assert!(splits.is_none());

        let (index_uid, source_id, shard_id) =
            split_queue_id("test-index:0/test-source/1").unwrap();
        assert_eq!(index_uid, "test-index:0");
        assert_eq!(source_id, "test-source");
        assert_eq!(shard_id, 1);
    }

    #[test]
    fn test_node_id() {
        let node_id = NodeId::new("test-node".to_string());
        assert_eq!(node_id.as_str(), "test-node");
        assert_eq!(node_id, NodeIdRef::from_str("test-node"));
    }

    #[test]
    fn test_node_serde() {
        #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
        struct Node {
            node_id: NodeId,
        }
        let node = Node {
            node_id: NodeId::from("test-node"),
        };
        let serialized = serde_json::to_string(&node).unwrap();
        assert_eq!(serialized, r#"{"node_id":"test-node"}"#);

        let deserialized = serde_json::from_str::<Node>(&serialized).unwrap();
        assert_eq!(deserialized, node);
    }
}
