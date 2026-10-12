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

//! Native containers are consumed directly. Only filtered dynamic object shells are rebuilt.
use std::borrow::Cow;
use std::ops::Deref;

use serde::Serialize;
use tantivy::TantivyDocument;
use tantivy::schema::Field;

pub(crate) enum Scalar<'a> {
    Null,
    Bool(bool),
    Number(Cow<'a, serde_json::Number>),
    Str(&'a str),
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for serde_json::Value {}
    impl Sealed for serde_json_borrow::Value<'_> {}
}

/// Mapping-only operations, not a JSON conversion framework. Implementations preserve native
/// containers, key/string storage, and iteration order. Borrowed inputs must be normalized before
/// entering the mapper; `append_unique` receives only unique keys in native encounter order.
pub(crate) trait MappingJson: sealed::Sealed + Serialize + Sized {
    type Key: Deref<Target = str>;
    type ObjectMap: Default;

    fn scalar(&self) -> Option<Scalar<'_>>;
    fn array(&self) -> Option<&[Self]>;
    fn object(&self) -> Option<&Self::ObjectMap>;
    fn into_array(self) -> Result<Vec<Self>, Self>;
    fn into_object(self) -> Result<Self::ObjectMap, Self>;
    fn from_object(object: Self::ObjectMap) -> Self;
    fn entries<'a>(object: &'a Self::ObjectMap) -> impl Iterator<Item = (&'a str, &'a Self)>
    where Self: 'a;
    fn into_entries(object: Self::ObjectMap) -> impl Iterator<Item = (Self::Key, Self)>;
    fn append_unique(object: &mut Self::ObjectMap, key: Self::Key, value: Self);
    fn add_json(&self, field: Field, document: &mut TantivyDocument);

    fn is_null(&self) -> bool {
        matches!(self.scalar(), Some(Scalar::Null))
    }
    fn as_str(&self) -> Option<&str> {
        match self.scalar() {
            Some(Scalar::Str(text)) => Some(text),
            _ => None,
        }
    }
    fn as_bool(&self) -> Option<bool> {
        match self.scalar() {
            Some(Scalar::Bool(value)) => Some(value),
            _ => None,
        }
    }
    fn diagnostic(&self) -> String {
        // Only normalized mapper values use direct serialization. Legacy unnormalized ingest
        // validators deliberately keep the dependency's Display semantics.
        serde_json::to_string(self)
            .expect("normalized JSON values must serialize to diagnostic text")
    }
}

impl MappingJson for serde_json::Value {
    type Key = String;
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
    fn into_array(self) -> Result<Vec<Self>, Self> {
        match self {
            Self::Array(values) => Ok(values),
            other => Err(other),
        }
    }
    fn into_object(self) -> Result<Self::ObjectMap, Self> {
        match self {
            Self::Object(object) => Ok(object),
            other => Err(other),
        }
    }
    fn from_object(object: Self::ObjectMap) -> Self {
        Self::Object(object)
    }
    fn entries<'a>(object: &'a Self::ObjectMap) -> impl Iterator<Item = (&'a str, &'a Self)>
    where Self: 'a {
        object.iter().map(|(key, value)| (key.as_str(), value))
    }
    fn into_entries(object: Self::ObjectMap) -> impl Iterator<Item = (Self::Key, Self)> {
        object.into_iter()
    }
    fn append_unique(object: &mut Self::ObjectMap, key: Self::Key, value: Self) {
        object.insert(key, value);
    }
    fn add_json(&self, field: Field, document: &mut TantivyDocument) {
        document.add_field_value(field, self);
    }
}

impl<'de> MappingJson for serde_json_borrow::Value<'de> {
    type Key = serde_json_borrow::KeyStrType<'de>;
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
    fn into_array(self) -> Result<Vec<Self>, Self> {
        match self {
            Self::Array(values) => Ok(values),
            other => Err(other),
        }
    }
    fn into_object(self) -> Result<Self::ObjectMap, Self> {
        match self {
            Self::Object(object) => Ok(object),
            other => Err(other),
        }
    }
    fn from_object(object: Self::ObjectMap) -> Self {
        Self::Object(object)
    }
    fn entries<'a>(object: &'a Self::ObjectMap) -> impl Iterator<Item = (&'a str, &'a Self)>
    where Self: 'a {
        object
            .as_vec()
            .iter()
            .map(|(key, value)| (key.as_ref(), value))
    }
    fn into_entries(mut object: Self::ObjectMap) -> impl Iterator<Item = (Self::Key, Self)> {
        // The only API addition needed beyond published 0.9 is as_vec_mut. Do not use
        // scratch-only FromIterator/constructors or normalize key storage to another container.
        std::mem::take(object.as_vec_mut()).into_iter()
    }
    fn append_unique(object: &mut Self::ObjectMap, key: Self::Key, value: Self) {
        object.as_vec_mut().push((key, value));
    }
    fn add_json(&self, field: Field, document: &mut TantivyDocument) {
        document.add_field_value(field, super::borrowed_value::BorrowedValue(self));
    }
}

#[cfg(test)]
#[path = "mapping_json_tests.rs"]
mod tests;
