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

//! Conversion of a borrowed JSON object into a tantivy document, following the mapping tree.
//!
//! Every function here mirrors a function of the owned conversion path (`doc_from_json` and
//! friends in the parent module) and must produce the same field values, in the same order, and
//! the same errors. The equivalence is checked by the differential tests in
//! `doc_mapper/borrowed_doc_tests.rs`. Any change to one path must be reflected in the other.

use std::any::type_name;
use std::net::IpAddr;
use std::str::FromStr;

use tantivy::TantivyDocument as Document;
use tantivy::schema::document::ReferenceValueLeaf;
use tantivy::schema::{Field, IntoIpv6Addr};

use super::{LeafType, MappingLeaf, MappingNode, MappingTree, NumVal};
use crate::doc_mapper::borrowed_json::{BorrowedObject, BorrowedValue};
use crate::doc_mapper::borrowed_value_view::{
    DynamicEntry, DynamicObject, add_borrowed_value, add_concatenate_leaves,
};
use crate::{Cardinality, DocParsingError, ModeType};

impl MappingNode {
    /// Mirrors [`MappingNode::doc_from_json`].
    pub(crate) fn doc_from_borrowed_json<'b, 'a>(
        &self,
        json_obj: &'b BorrowedObject<'a>,
        mode: ModeType,
        document: &mut Document,
        path: &mut Vec<&'b str>,
        dynamic_obj: &mut DynamicObject<'b, 'a>,
    ) -> Result<(), DocParsingError> {
        for (field_name, json_val) in json_obj {
            let field_name: &'b str = field_name;
            let Some(child_tree) = self.branches.get(field_name) else {
                match mode {
                    ModeType::Lenient => {
                        // In lenient mode we simply ignore these unmapped fields.
                    }
                    ModeType::Dynamic => {
                        dynamic_obj.push((field_name, DynamicEntry::Value(json_val)));
                    }
                    ModeType::Strict => {
                        path.push(field_name);
                        return Err(DocParsingError::NoSuchFieldInSchema(path.join(".")));
                    }
                }
                continue;
            };
            path.push(field_name);
            match child_tree {
                MappingTree::Leaf(mapping_leaf) => {
                    mapping_leaf.doc_from_borrowed_json(json_val, document, path)?;
                }
                MappingTree::Node(mapping_node) => {
                    let BorrowedValue::Object(child_obj) = json_val else {
                        return Err(DocParsingError::ValueError(
                            path.join("."),
                            format!("expected an JSON object, got {json_val}"),
                        ));
                    };
                    let mut child_dynamic_obj = DynamicObject::new();
                    mapping_node.doc_from_borrowed_json(
                        child_obj,
                        mode,
                        document,
                        path,
                        &mut child_dynamic_obj,
                    )?;
                    if !child_dynamic_obj.is_empty() {
                        dynamic_obj.push((field_name, DynamicEntry::Object(child_dynamic_obj)));
                    }
                }
            }
            path.pop();
        }
        Ok(())
    }
}

impl MappingLeaf {
    /// Mirrors [`MappingLeaf::doc_from_json`].
    fn doc_from_borrowed_json(
        &self,
        json_val: &BorrowedValue,
        document: &mut Document,
        path: &[&str],
    ) -> Result<(), DocParsingError> {
        if json_val.is_null() {
            // We just ignore `null`.
            return Ok(());
        }
        let BorrowedValue::Array(elements) = json_val else {
            return self.add_borrowed_value(json_val, document, path);
        };
        if self.cardinality == Cardinality::SingleValued {
            return Err(DocParsingError::MultiValuesNotSupported(path.join(".")));
        }
        for element in elements {
            if element.is_null() {
                // We just ignore `null`.
                continue;
            }
            self.add_borrowed_value(element, document, path)?;
        }
        Ok(())
    }

    fn add_borrowed_value(
        &self,
        json_val: &BorrowedValue,
        document: &mut Document,
        path: &[&str],
    ) -> Result<(), DocParsingError> {
        let to_value_error = |err_msg| DocParsingError::ValueError(path.join("."), err_msg);
        if !self.concatenate.is_empty() {
            self.typ
                .add_borrowed_concatenate_values(json_val, &self.concatenate, document)
                .map_err(to_value_error)?;
        }
        self.typ
            .add_borrowed_value(self.field, json_val, document)
            .map_err(to_value_error)
    }
}

impl LeafType {
    /// Mirrors [`LeafType::value_from_json`] followed by adding the value to the document.
    fn add_borrowed_value(
        &self,
        field: Field,
        json_val: &BorrowedValue,
        document: &mut Document,
    ) -> Result<(), String> {
        match self {
            LeafType::Text(_) => {
                let BorrowedValue::Str(text) = json_val else {
                    return Err(format!("expected string, got `{json_val}`"));
                };
                document.add_text(field, text);
            }
            LeafType::I64(numeric_options) => {
                let value = num_from_borrowed_json::<i64>(json_val, numeric_options.coerce)?;
                document.add_i64(field, value);
            }
            LeafType::U64(numeric_options) => {
                let value = num_from_borrowed_json::<u64>(json_val, numeric_options.coerce)?;
                document.add_u64(field, value);
            }
            LeafType::F64(numeric_options) => {
                let value = num_from_borrowed_json::<f64>(json_val, numeric_options.coerce)?;
                document.add_f64(field, value);
            }
            LeafType::Bool(_) => {
                let BorrowedValue::Bool(value) = json_val else {
                    return Err(format!("expected boolean, got `{json_val}`"));
                };
                document.add_bool(field, *value);
            }
            LeafType::IpAddr(_) => {
                let BorrowedValue::Str(ip_address) = json_val else {
                    return Err(format!("expected string, got `{json_val}`"));
                };
                let ipv6_value = IpAddr::from_str(ip_address)
                    .map_err(|err| format!("failed to parse IP address `{ip_address}`: {err}"))?
                    .into_ipv6_addr();
                document.add_ip_addr(field, ipv6_value);
            }
            LeafType::DateTime(date_time_options) => {
                let date_time = date_time_options.parse_borrowed_json(json_val)?;
                document.add_date(field, date_time);
            }
            LeafType::Bytes(binary_options) => {
                let BorrowedValue::Str(byte_str) = json_val else {
                    return Err(format!(
                        "expected {} string, got `{json_val}`",
                        binary_options.input_format.as_str()
                    ));
                };
                let payload = binary_options.input_format.parse_str(byte_str)?;
                document.add_bytes(field, &payload);
            }
            LeafType::Json(_) => {
                let BorrowedValue::Object(_) = json_val else {
                    return Err(format!("expected object, got `{json_val}`"));
                };
                add_borrowed_value(document, field, json_val);
            }
        }
        Ok(())
    }

    /// Mirrors [`LeafType::concatenate_values_from_json`] followed by adding the values to each
    /// of the `concatenate_fields`.
    ///
    /// Errors are only returned before any value is added.
    fn add_borrowed_concatenate_values(
        &self,
        json_val: &BorrowedValue,
        concatenate_fields: &[Field],
        document: &mut Document,
    ) -> Result<(), String> {
        let leaf: ReferenceValueLeaf = match self {
            LeafType::Text(_) => {
                let BorrowedValue::Str(text) = json_val else {
                    return Err(format!("expected string, got `{json_val}`"));
                };
                ReferenceValueLeaf::Str(text)
            }
            LeafType::I64(numeric_options) => {
                num_from_borrowed_json::<i64>(json_val, numeric_options.coerce)?.into()
            }
            LeafType::U64(numeric_options) => {
                num_from_borrowed_json::<u64>(json_val, numeric_options.coerce)?.into()
            }
            LeafType::F64(numeric_options) => {
                num_from_borrowed_json::<f64>(json_val, numeric_options.coerce)?.into()
            }
            LeafType::Bool(_) => {
                let BorrowedValue::Bool(value) = json_val else {
                    return Err(format!("expected boolean, got `{json_val}`"));
                };
                (*value).into()
            }
            LeafType::IpAddr(_) => return Err("unsupported concat type: IpAddr".to_string()),
            LeafType::DateTime(_) => return Err("unsupported concat type: DateTime".to_string()),
            LeafType::Bytes(_) => return Err("unsupported concat type: Bytes".to_string()),
            LeafType::Json(_) => {
                let BorrowedValue::Object(json_obj) = json_val else {
                    return Err(format!("expected object, got `{json_val}`"));
                };
                for (_key, child_json_val) in json_obj {
                    add_concatenate_leaves(document, concatenate_fields, child_json_val);
                }
                return Ok(());
            }
        };
        for field in concatenate_fields {
            document.add_leaf_field_value(*field, leaf.clone());
        }
        Ok(())
    }
}

/// Mirrors [`NumVal::from_json_to_self`].
fn num_from_borrowed_json<T: NumVal>(json_val: &BorrowedValue, coerce: bool) -> Result<T, String> {
    match json_val {
        BorrowedValue::Number(num_val) => T::from_json_number(num_val).ok_or_else(|| {
            format!(
                "expected {}, got inconvertible JSON number `{}`",
                type_name::<T>(),
                num_val
            )
        }),
        BorrowedValue::Str(str_val) => {
            if !coerce {
                return Err(format!(
                    "expected JSON number, got string `\"{str_val}\"`. enable coercion to {} with \
                     the `coerce` parameter in the field mapping",
                    type_name::<T>()
                ));
            }
            str_val.parse::<T>().map_err(|_| {
                format!(
                    "failed to coerce JSON string `\"{str_val}\"` to {}",
                    type_name::<T>()
                )
            })
        }
        _ => {
            if coerce {
                Err(format!("expected JSON number or string, got `{json_val}`"))
            } else {
                Err(format!("expected JSON number, got `{json_val}`"))
            }
        }
    }
}
