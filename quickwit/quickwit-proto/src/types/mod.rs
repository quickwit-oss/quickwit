// Copyright (C) 2024 Quickwit, Inc.
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
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::str::FromStr;

use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;
pub use ulid::Ulid;

mod pipeline_uid;
mod position;
mod shard_id;

pub use pipeline_uid::PipelineUid;
pub use position::Position;
pub use shard_id::ShardId;

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

/// Index identifiers that uniquely identify not only the index, but also
/// its incarnation allowing to distinguish between deleted and recreated indexes.
/// It is represented as a string in index_id:incarnation_id format.
#[derive(Clone, Debug, Default, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct IndexUid {
    index_id: String,
    incarnation_id: Ulid,
}

impl FromStr for IndexUid {
    type Err = InvalidIndexUid;

    fn from_str(index_uid_str: &str) -> Result<Self, Self::Err> {
        let (index_id, incarnation_id) = match index_uid_str.split_once(':') {
            Some((index_id, "")) => (index_id, Ulid::nil()), // TODO reject
            Some((index_id, ulid)) => {
                let ulid = Ulid::from_string(ulid).map_err(|_| InvalidIndexUid {
                    invalid_index_uid_str: index_uid_str.to_string(),
                })?;
                (index_id, ulid)
            }
            None => (index_uid_str, Ulid::nil()), // TODO reject
        };
        Ok(IndexUid {
            index_id: index_id.to_string(),
            incarnation_id,
        })
    }
}

// It is super lame, but for backward compatibility reasons we accept having a missing ulid part.
// TODO DEPRECATED ME and remove
impl<'de> Deserialize<'de> for IndexUid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let index_uid_str: String = String::deserialize(deserializer)?;
        let index_uid = IndexUid::from_str(&index_uid_str).map_err(D::Error::custom)?;
        Ok(index_uid)
    }
}

impl Serialize for IndexUid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_str(&self.to_string())
    }
}

impl ::prost::Message for IndexUid {
    fn encode_raw<B>(&self, buf: &mut B)
    where B: ::prost::bytes::BufMut {
        if !self.index_id.is_empty() {
            ::prost::encoding::string::encode(1u32, &self.index_id, buf);
        }
        let (ulid_high, ulid_low): (u64, u64) = self.incarnation_id.into();

        if ulid_high != 0u64 {
            ::prost::encoding::uint64::encode(2u32, &ulid_high, buf);
        }
        if ulid_low != 0u64 {
            ::prost::encoding::uint64::encode(3u32, &ulid_low, buf);
        }
    }

    fn merge_field<B>(
        &mut self,
        tag: u32,
        wire_type: ::prost::encoding::WireType,
        buf: &mut B,
        ctx: ::prost::encoding::DecodeContext,
    ) -> ::core::result::Result<(), ::prost::DecodeError>
    where
        B: ::prost::bytes::Buf,
    {
        const STRUCT_NAME: &str = "IndexUid";

        match tag {
            1u32 => {
                let value = &mut self.index_id;
                ::prost::encoding::string::merge(wire_type, value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "index_id");
                    error
                })
            }
            2u32 => {
                let (mut ulid_high, ulid_low) = self.incarnation_id.into();
                ::prost::encoding::uint64::merge(wire_type, &mut ulid_high, buf, ctx).map_err(
                    |mut error| {
                        error.push(STRUCT_NAME, "incarnation_id_high");
                        error
                    },
                )?;
                self.incarnation_id = (ulid_high, ulid_low).into();
                Ok(())
            }
            3u32 => {
                let (ulid_high, mut ulid_low) = self.incarnation_id.into();
                ::prost::encoding::uint64::merge(wire_type, &mut ulid_low, buf, ctx).map_err(
                    |mut error| {
                        error.push(STRUCT_NAME, "incarnation_id_low");
                        error
                    },
                )?;
                self.incarnation_id = (ulid_high, ulid_low).into();
                Ok(())
            }
            _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        let mut len = 0;

        if !self.index_id.is_empty() {
            len += ::prost::encoding::string::encoded_len(1u32, &self.index_id);
        }
        let (ulid_high, ulid_low): (u64, u64) = self.incarnation_id.into();

        if ulid_high != 0u64 {
            len += ::prost::encoding::uint64::encoded_len(2u32, &ulid_high);
        }
        if ulid_low != 0u64 {
            len += ::prost::encoding::uint64::encoded_len(3u32, &ulid_low);
        }
        len
    }

    fn clear(&mut self) {
        self.index_id.clear();
        self.incarnation_id = Ulid::nil();
    }
}

impl fmt::Display for IndexUid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.index_id, self.incarnation_id)
    }
}

impl IndexUid {
    /// Creates a new index uid from index_id.
    /// A random ULID will be used as incarnation
    pub fn new_with_random_ulid(index_id: &str) -> Self {
        Self::from_parts(index_id, Ulid::new())
    }

    pub fn from_parts(index_id: &str, incarnation_id: impl Into<Ulid>) -> Self {
        assert!(!index_id.contains(':'), "index ID may not contain `:`");
        let incarnation_id = incarnation_id.into();
        IndexUid {
            index_id: index_id.to_string(),
            incarnation_id,
        }
    }

    pub fn parse(string: &str) -> Result<Self, InvalidIndexUid> {
        string.parse()
    }

    pub fn index_id(&self) -> &str {
        &self.index_id
    }

    pub fn incarnation_id(&self) -> &Ulid {
        &self.incarnation_id
    }

    pub fn is_empty(&self) -> bool {
        self.index_id.is_empty()
    }
}

impl From<IndexUid> for String {
    fn from(val: IndexUid) -> Self {
        val.to_string()
    }
}

#[derive(Error, Debug)]
#[error("invalid index uid `{invalid_index_uid_str}`")]
pub struct InvalidIndexUid {
    pub invalid_index_uid_str: String,
}

impl TryFrom<String> for IndexUid {
    type Error = InvalidIndexUid;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
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
            queue_id(
                &"test-index:00000000000000000000000000".parse().unwrap(),
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
