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
use std::ops::Deref;

use serde::Serialize;
use tantivy::TantivyDocument;
use tantivy::schema::Field;

pub(crate) use crate::json_read::{JsonRead, Scalar};

/// Mapping-only operations, not a JSON conversion framework. Implementations preserve native
/// containers, key/string storage, and iteration order. Borrowed inputs must be normalized before
/// entering the mapper; `append_unique` receives only unique keys in native encounter order.
pub(crate) trait MappingJson: JsonRead + Serialize {
    type Key: Deref<Target = str>;

    fn into_array(self) -> Result<Vec<Self>, Self>;
    fn into_object(self) -> Result<Self::ObjectMap, Self>;
    fn from_object(object: Self::ObjectMap) -> Self;
    fn into_entries(object: Self::ObjectMap) -> impl Iterator<Item = (Self::Key, Self)>;
    fn append_unique(object: &mut Self::ObjectMap, key: Self::Key, value: Self);
    fn add_json(&self, field: Field, document: &mut TantivyDocument);

    fn diagnostic(&self) -> String {
        // Only normalized mapper values use direct serialization. Legacy unnormalized ingest
        // validators deliberately keep the dependency's Display semantics.
        serde_json::to_string(self)
            .expect("normalized JSON values must serialize to diagnostic text")
    }
}

impl MappingJson for serde_json::Value {
    type Key = String;

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
