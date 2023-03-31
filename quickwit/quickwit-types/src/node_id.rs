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
use std::fmt::Display;
use std::ops::Deref;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
        self.as_str()
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
