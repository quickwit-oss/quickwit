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

//! Read-only native JSON access shared by mapping and document fingerprinting.
//! Borrowed trees used for serde-compatible traversal must first be normalized with
//! [`crate::borrowed_json::parse_json_object`]. This view does not reinterpret strings as dates.

use std::borrow::Cow;

/// A JSON scalar, retaining raw strings and canonical serde number semantics.
pub enum Scalar<'a> {
    /// JSON null.
    Null,
    /// A boolean.
    Bool(bool),
    /// Owned input lends its native number; borrowed input converts only this scalar.
    Number(Cow<'a, serde_json::Number>),
    /// The original JSON string, without date conversion or tokenization.
    Str(&'a str),
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for serde_json::Value {}
    impl Sealed for serde_json_borrow::Value<'_> {}
}

/// Sealed read access to the two native JSON containers used in ingestion.
///
/// No trees, keys, or containers are copied; only borrowed numeric scalars are converted.
/// Object iteration retains native order;
/// callers requiring serde map semantics on borrowed input must normalize first.
/// The consuming mapper remains private and accepts a normalized borrowed root only.
pub trait JsonRead: sealed::Sealed + Sized {
    /// The native object container, also used by the private consuming mapper.
    type ObjectMap: Default;

    /// Read a scalar, or return `None` for a container.
    fn scalar(&self) -> Option<Scalar<'_>>;
    /// Read the native array storage.
    fn array(&self) -> Option<&[Self]>;
    /// Read the native object storage.
    fn object(&self) -> Option<&Self::ObjectMap>;
    /// Iterate native entries. `len()` is constant time, not a traversal.
    fn entries<'a>(
        object: &'a Self::ObjectMap,
    ) -> impl ExactSizeIterator<Item = (&'a str, &'a Self)>
    where Self: 'a;
    /// Look up a native key. Owned maps keep `Map::get` complexity (tree/hash lookup).
    /// Borrowed vector-backed objects use linear lookup; normalized objects have unique keys.
    fn get<'a>(object: &'a Self::ObjectMap, key: &str) -> Option<&'a Self>;

    /// Whether this value is JSON null.
    fn is_null(&self) -> bool {
        matches!(self.scalar(), Some(Scalar::Null))
    }
    /// Read a raw JSON string.
    fn as_str(&self) -> Option<&str> {
        match self.scalar() {
            Some(Scalar::Str(text)) => Some(text),
            _ => None,
        }
    }
    /// Read a boolean.
    fn as_bool(&self) -> Option<bool> {
        match self.scalar() {
            Some(Scalar::Bool(value)) => Some(value),
            _ => None,
        }
    }
}

impl JsonRead for serde_json::Value {
    type ObjectMap = serde_json::Map<String, Self>;

    fn scalar(&self) -> Option<Scalar<'_>> {
        match self {
            Self::Null => Some(Scalar::Null),
            Self::Bool(value) => Some(Scalar::Bool(*value)),
            Self::Number(value) => Some(Scalar::Number(Cow::Borrowed(value))),
            Self::String(value) => Some(Scalar::Str(value)),
            _ => None,
        }
    }
    fn array(&self) -> Option<&[Self]> {
        self.as_array().map(Vec::as_slice)
    }
    fn object(&self) -> Option<&Self::ObjectMap> {
        self.as_object()
    }
    fn entries<'a>(
        object: &'a Self::ObjectMap,
    ) -> impl ExactSizeIterator<Item = (&'a str, &'a Self)>
    where Self: 'a {
        object.iter().map(|(key, value)| (key.as_str(), value))
    }
    fn get<'a>(object: &'a Self::ObjectMap, key: &str) -> Option<&'a Self> {
        object.get(key)
    }
}

impl<'de> JsonRead for serde_json_borrow::Value<'de> {
    type ObjectMap = serde_json_borrow::Map<'de>;

    fn scalar(&self) -> Option<Scalar<'_>> {
        match self {
            Self::Null => Some(Scalar::Null),
            Self::Bool(value) => Some(Scalar::Bool(*value)),
            Self::Number(value) => {
                Some(Scalar::Number(Cow::Owned(serde_json::Number::from(*value))))
            }
            Self::Str(value) => Some(Scalar::Str(value)),
            _ => None,
        }
    }
    fn array(&self) -> Option<&[Self]> {
        self.as_array()
    }
    fn object(&self) -> Option<&Self::ObjectMap> {
        self.as_object()
    }
    fn entries<'a>(
        object: &'a Self::ObjectMap,
    ) -> impl ExactSizeIterator<Item = (&'a str, &'a Self)>
    where Self: 'a {
        object
            .as_vec()
            .iter()
            .map(|(key, value)| (key.as_ref(), value))
    }
    fn get<'a>(object: &'a Self::ObjectMap, key: &str) -> Option<&'a Self> {
        object.get(key)
    }
}
