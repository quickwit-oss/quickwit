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

//! Routing expression evaluation on borrowed JSON documents.
//!
//! The partition of a document must not depend on the conversion path, so this mirrors exactly the
//! `serde_json::Map` implementation of the parent module.

use std::hash::{Hash, Hasher};

use super::RoutingExprContext;
use crate::doc_mapper::BorrowedJsonDoc;
use crate::doc_mapper::borrowed_json::{BorrowedValue, get_in_object};

/// Mirrors `hash_json_val`. Relies on objects being sorted by key with unique keys, like
/// `serde_json::Map`.
fn hash_borrowed_json_val<H: Hasher>(json_val: &BorrowedValue, hasher: &mut H) {
    match json_val {
        BorrowedValue::Null => {
            hasher.write_u8(0u8);
        }
        BorrowedValue::Bool(bool_val) => {
            hasher.write_u8(1u8);
            bool_val.hash(hasher);
        }
        BorrowedValue::Number(num) => {
            hasher.write_u8(2u8);
            num.hash(hasher);
        }
        BorrowedValue::Str(text) => {
            hasher.write_u8(3u8);
            hasher.write_u64(text.len() as u64);
            hasher.write(text.as_bytes());
        }
        BorrowedValue::Array(elements) => {
            hasher.write_u8(4u8);
            hasher.write_u64(elements.len() as u64);
            for element in elements {
                hash_borrowed_json_val(element, hasher);
            }
        }
        BorrowedValue::Object(entries) => {
            hasher.write_u8(5u8);
            hasher.write_u64(entries.len() as u64);
            for (key, value) in entries {
                hasher.write_u64(key.len() as u64);
                hasher.write(key.as_bytes());
                hash_borrowed_json_val(value, hasher);
            }
        }
    }
}

/// Mirrors `find_value_in_map`. `keys` is never empty.
fn find_value_in_doc<'b, 'a>(
    json_doc: &'b BorrowedJsonDoc<'a>,
    keys: &[String],
) -> Option<&'b BorrowedValue<'a>> {
    let mut value = get_in_object(json_doc.root(), &keys[0])?;
    for key in &keys[1..] {
        let BorrowedValue::Object(entries) = value else {
            return None;
        };
        value = get_in_object(entries, key)?;
    }
    Some(value)
}

impl RoutingExprContext for BorrowedJsonDoc<'_> {
    fn hash_attribute<H: Hasher>(&self, attr_name: &[String], hasher: &mut H) {
        if let Some(json_val) = find_value_in_doc(self, attr_name) {
            hasher.write_u8(1u8);
            hash_borrowed_json_val(json_val, hasher);
        } else {
            hasher.write_u8(0u8);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::RoutingExpr;
    use crate::doc_mapper::BorrowedJsonDoc;

    #[test]
    fn test_borrowed_routing_hash_same_as_serde_json() {
        let expressions = [
            "service",
            "service,tenant_id",
            "resource.attributes.host",
            "hash_mod(tenant_id, 10)",
            "resource",
            "missing,service",
            "tags",
        ];
        let docs = [
            r#"{"service": "api", "tenant_id": 12}"#,
            r#"{"tenant_id": -3, "service": "api", "service": "web"}"#,
            r#"{"resource": {"attributes": {"host": "h1", "zone": 1.5}, "a": null}}"#,
            r#"{"resource": {"attributes": "not an object"}}"#,
            r#"{"tags": ["a", 1, true, null, {"z": 1, "a": 2, "z": 3}]}"#,
            r#"{"service": "esc\"aped", "tenant_id": 18446744073709551615}"#,
            r#"{}"#,
        ];
        for expression in expressions {
            let routing_expr = RoutingExpr::new(expression).unwrap();
            for doc in docs {
                let json_obj: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_str(doc).unwrap();
                let borrowed_doc = BorrowedJsonDoc::parse(doc.as_bytes()).unwrap();
                assert_eq!(
                    routing_expr.eval_hash(&json_obj),
                    routing_expr.eval_hash(&borrowed_doc),
                    "expression: {expression}, doc: {doc}"
                );
            }
        }
    }
}
