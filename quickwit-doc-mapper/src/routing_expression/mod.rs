// Copyright (C) 2022 Quickwit, Inc.
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

use std::collections::hash_map::DefaultHasher;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

pub trait RoutingExprContext {
    // TODO see if we can get rid of the alloc in some specific case
    fn hash_attribute<H: Hasher>(&self, attr_name: &str, hasher: &mut H);
}

/// This is a bit overkill but this function has the merit of
/// ensuring that the data that is sent to the hasher is unique
/// to the value, so we do not lose injectivity there.
fn hash_json_val<H: Hasher>(json_val: &serde_json::Value, hasher: &mut H) {
    match json_val {
        serde_json::Value::Null => {
            hasher.write_u8(0u8);
        }
        serde_json::Value::Bool(bool_val) => {
            hasher.write_u8(1u8);
            bool_val.hash(hasher);
        }
        serde_json::Value::Number(num) => {
            hasher.write_u8(2u8);
            num.hash(hasher);
        }
        serde_json::Value::String(s) => {
            hasher.write_u8(3u8);
            hasher.write_usize(s.len());
            hasher.write(s.as_bytes());
        }
        serde_json::Value::Array(arr) => {
            hasher.write_u8(4u8);
            hasher.write_usize(arr.len());
            for el in arr {
                hash_json_val(el, hasher);
            }
        }
        serde_json::Value::Object(obj) => {
            hasher.write_u8(5u8);
            hasher.write_usize(obj.len());
            for (key, val) in obj.iter() {
                hasher.write_usize(key.len());
                hasher.write(key.as_bytes());
                hash_json_val(val, hasher);
            }
        }
    }
}

impl RoutingExprContext for serde_json::Map<String, serde_json::Value> {
    fn hash_attribute<H: Hasher>(&self, attr_name: &str, hasher: &mut H) {
        if let Some(json_val) = self.get(attr_name) {
            hasher.write_u8(1u8);
            hash_json_val(json_val, hasher);
        } else {
            hasher.write_u8(0u8);
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RoutingExpr {
    Field(String),
    Composite(Vec<RoutingExpr>),
    // TODO Enrich me! Map / Modulo
}

impl FromStr for RoutingExpr {
    type Err = anyhow::Error;

    fn from_str(expr_dsl_str: &str) -> anyhow::Result<Self> {
        if expr_dsl_str.is_empty() {
            return Ok(RoutingExpr::Composite(Vec::new()));
        }
        Ok(RoutingExpr::Field(expr_dsl_str.to_string()))
    }
}

// The display implementation should be consistent with `FromString`.
impl Display for RoutingExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self {
            RoutingExpr::Field(field) => {
                write!(f, "{}", field)?;
            }
            RoutingExpr::Composite(children) => {
                if children.is_empty() {
                    return Ok(());
                }
                write!(f, "{}", &children[0])?;
                for child in &children[1..] {
                    write!(f, ",{}", child)?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Hash)]
#[repr(u8)]
enum ExprType {
    Field,
    Composite,
}

impl RoutingExpr {
    /// Evaluates the expression applied to the given
    /// context and returns a u64 hash.
    ///
    /// Obviously this function is not perfectly injective.
    pub fn eval_hash<Ctx: RoutingExprContext>(&self, ctx: &Ctx) -> u64 {
        let mut hasher = DefaultHasher::default();
        self.hash(ctx, &mut hasher);
        hasher.finish()
    }

    fn hash<Ctx: RoutingExprContext, H: Hasher>(&self, ctx: &Ctx, hasher: &mut H) {
        match self {
            RoutingExpr::Field(field_name) => {
                ExprType::Field.hash(hasher);
                ctx.hash_attribute(field_name, hasher);
            }
            RoutingExpr::Composite(children) => {
                ExprType::Composite.hash(hasher);
                for child in children {
                    child.hash(ctx, hasher);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ser_deser(expr: &RoutingExpr) {
        let ser = expr.to_string();
        assert_eq!(&RoutingExpr::from_str(&ser).unwrap(), expr);
    }

    fn deser_util(expr_dsl: &str) -> RoutingExpr {
        let expr = RoutingExpr::from_str(expr_dsl).unwrap();
        test_ser_deser(&expr);
        expr
    }

    #[test]
    fn test_routing_expr_empty() {
        let routing_expr = deser_util("");
        assert!(matches!(routing_expr, RoutingExpr::Composite(leaves) if leaves.is_empty()));
    }

    #[test]
    fn test_routing_expr_single_field() {
        let routing_expr = deser_util("tenant_id");
        assert!(matches!(routing_expr, RoutingExpr::Field(attr_name) if attr_name == "tenant_id"));
    }
}
