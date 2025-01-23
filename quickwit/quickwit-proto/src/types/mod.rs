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
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use tracing::warn;
pub use ulid::Ulid;

mod doc_mapping_uid;
mod doc_uid;
mod index_uid;
mod pipeline_uid;
mod position;
mod shard_id;

pub use doc_mapping_uid::DocMappingUid;
pub use doc_uid::{DocUid, DocUidGenerator};
pub use index_uid::IndexUid;
pub use pipeline_uid::PipelineUid;
pub use position::Position;
pub use shard_id::ShardId;

/// The size of an ULID in bytes. Use `ULID_LEN` for the length of Base32 encoded ULID strings.
pub(crate) const ULID_SIZE: usize = 16;

pub type IndexId = String;

pub type SourceId = String;

pub type SplitId = String;

pub type SubrequestId = u32;

/// See the file `ingest.proto` for more details.
pub type PublishToken = String;

/// Uniquely identifies a shard and its underlying mrecordlog queue.
pub type QueueId = String; // <index_uid>/<source_id>/<shard_id>

pub fn queue_id(index_uid: &IndexUid, source_id: &str, shard_id: &ShardId) -> QueueId {
    format!("{index_uid}/{source_id}/{shard_id}")
}

pub fn split_queue_id(queue_id: &str) -> Option<(IndexUid, SourceId, ShardId)> {
    let parts_opt = split_queue_id_inner(queue_id);

    if parts_opt.is_none() {
        warn!("failed to parse queue ID `{queue_id}`: this should never happen, please report");
    }
    parts_opt
}

fn split_queue_id_inner(queue_id: &str) -> Option<(IndexUid, SourceId, ShardId)> {
    let mut parts = queue_id.split('/');
    let index_uid = parts.next()?;
    let source_id = parts.next()?;
    let shard_id = parts.next()?;
    Some((
        index_uid.parse().ok()?,
        source_id.to_string(),
        ShardId::from(shard_id),
    ))
}

/// It can however appear only once in a given index.
/// In itself, `SourceId` is not unique, but the pair `(IndexUid, SourceId)` is.
#[derive(PartialEq, Eq, Debug, PartialOrd, Ord, Hash, Clone)]
pub struct SourceUid {
    pub index_uid: IndexUid,
    pub source_id: SourceId,
}

impl Display for SourceUid {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.index_uid, self.source_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(String);

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

impl Borrow<String> for NodeId {
    fn borrow(&self) -> &String {
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

impl PartialEq<String> for NodeId {
    fn eq(&self, other: &String) -> bool {
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

impl Borrow<str> for NodeIdRef {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl Display for NodeIdRef {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", &self.0)
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

#[cfg(feature = "postgres")]
impl From<&NodeId> for sea_query::Value {
    fn from(node_id: &NodeId) -> Self {
        node_id.to_string().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_id() {
        assert_eq!(
            queue_id(
                &IndexUid::for_test("test-index", 0),
                "test-source",
                &ShardId::from(1u64)
            ),
            "test-index:00000000000000000000000000/test-source/00000000000000000001"
        );
    }

    #[test]
    fn test_split_queue_id() {
        let splits = split_queue_id("test-index:00000000000000000000000000");
        assert!(splits.is_none());

        let splits = split_queue_id("test-index:00000000000000000000000000/test-source");
        assert!(splits.is_none());

        let (index_uid, source_id, shard_id) = split_queue_id(
            "test-index:00000000000000000000000000/test-source/00000000000000000001",
        )
        .unwrap();
        assert_eq!(
            &index_uid.to_string(),
            "test-index:00000000000000000000000000"
        );
        assert_eq!(source_id, "test-source");
        assert_eq!(shard_id, ShardId::from(1u64));
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
