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

use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;

use serde_json::Value as JsonValue;
use siphasher::sip::SipHasher;

pub trait RoutingExprContext {
    fn hash_attribute<H: Hasher, F>(&self, attr_name: &str, hasher: &mut H, tranform: &F)
    where F: Fn(&JsonValue) -> JsonValue;
}

/// This is a bit overkill but this function has the merit of
/// ensuring that the data that is sent to the hasher is unique
/// to the value, so we do not lose injectivity there.
fn hash_json_val<H: Hasher>(json_val: &JsonValue, hasher: &mut H) {
    match json_val {
        JsonValue::Null => {
            hasher.write_u8(0u8);
        }
        JsonValue::Bool(bool_val) => {
            hasher.write_u8(1u8);
            bool_val.hash(hasher);
        }
        JsonValue::Number(num) => {
            hasher.write_u8(2u8);
            num.hash(hasher);
        }
        JsonValue::String(s) => {
            hasher.write_u8(3u8);
            hasher.write_usize(s.len());
            hasher.write(s.as_bytes());
        }
        JsonValue::Array(arr) => {
            hasher.write_u8(4u8);
            hasher.write_usize(arr.len());
            for el in arr {
                hash_json_val(el, hasher);
            }
        }
        JsonValue::Object(obj) => {
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

impl RoutingExprContext for serde_json::Map<String, JsonValue> {
    fn hash_attribute<H: Hasher, F>(&self, attr_name: &str, hasher: &mut H, transform: &F)
    where F: Fn(&JsonValue) -> JsonValue {
        if let Some(json_val) = self.get(attr_name) {
            hasher.write_u8(1u8);
            hash_json_val(&transform(json_val), hasher);
        } else {
            hasher.write_u8(0u8);
        }
    }
}

#[derive(Clone, Default)]
pub struct RoutingExpr {
    inner_opt: Option<Arc<InnerRoutingExpr>>,
    salted_hasher: SipHasher,
}

impl RoutingExpr {
    pub fn new(expr_dsl_str: &str) -> anyhow::Result<Self> {
        let expr_dsl_str = expr_dsl_str.trim();
        if expr_dsl_str.is_empty() {
            return Ok(RoutingExpr::default());
        }

        let mut salted_hasher: SipHasher = SipHasher::new();

        let inner: InnerRoutingExpr = InnerRoutingExpr::from_str(expr_dsl_str)?;
        // We hash the expression tree here instead of hashing the str, or
        // hash the display of the tree, in order to make the partition id less brittle to
        // a minor change in formatting, or a change in the DSL itself.
        //
        // We do not use the standard library DefaultHasher to make sure we
        // get the same hash values.
        inner.hash(&mut salted_hasher);

        Ok(RoutingExpr {
            inner_opt: Some(Arc::new(inner)),
            salted_hasher,
        })
    }

    /// Evaluates the expression applied to the given
    /// context and returns a u64 hash.
    ///
    /// Obviously this function is not perfectly injective.
    pub fn eval_hash<Ctx: RoutingExprContext>(&self, ctx: &Ctx) -> u64 {
        if let Some(inner) = self.inner_opt.as_ref() {
            let mut hasher: SipHasher = self.salted_hasher;
            inner.eval_hash(ctx, &mut hasher);
            hasher.finish()
        } else {
            0u64
        }
    }
}

impl Display for RoutingExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(inner_expr) = self.inner_opt.as_ref() {
            inner_expr.fmt(f)
        } else {
            write!(f, "EmptyRoutingExpr")
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum InnerRoutingExpr {
    Field(String),
    Composite(Vec<InnerRoutingExpr>),
    Modulo(String, u64),
    // TODO Enrich me! Map / ...
}

impl InnerRoutingExpr {
    fn eval_hash<Ctx: RoutingExprContext, H: Hasher>(&self, ctx: &Ctx, hasher: &mut H) {
        match self {
            InnerRoutingExpr::Field(field_name) => {
                ExprType::Field.hash(hasher);
                ctx.hash_attribute(field_name, hasher, &Clone::clone);
            }
            InnerRoutingExpr::Composite(children) => {
                ExprType::Composite.hash(hasher);
                for child in children {
                    child.eval_hash(ctx, hasher);
                }
            }
            InnerRoutingExpr::Modulo(field_name, modulo) => {
                ExprType::Modulo.hash(hasher);
                ctx.hash_attribute(field_name, hasher, &|value| {
                    if let Some(unsigned) = value.as_u64() {
                        unsigned.rem_euclid(*modulo).into()
                    } else if let Some(signed) = value.as_i64() {
                        signed.rem_euclid(*modulo as i64).into()
                    } else if let Some(float) = value.as_f64() {
                        float.rem_euclid(*modulo as f64).into()
                    } else {
                        value.clone()
                    }
                })
            }
        }
    }
}

// We don't rely on Derive here to make it easier to keep the
// implementation stable.
#[allow(clippy::derive_hash_xor_eq)]
impl Hash for InnerRoutingExpr {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        match self {
            InnerRoutingExpr::Field(field_name) => {
                ExprType::Field.hash(hasher);
                // TODO is it okay to write_usize? The result is probably platform dependant, if a
                // node is 32b, it might not agree with 64b nodes.
                hasher.write_usize(field_name.len());
                hasher.write(field_name.as_bytes());
            }
            InnerRoutingExpr::Composite(children) => {
                ExprType::Composite.hash(hasher);
                for child in children {
                    child.hash(hasher);
                }
            }
            InnerRoutingExpr::Modulo(field_name, modulo) => {
                ExprType::Modulo.hash(hasher);
                // TODO is it okay to write_usize? The result is probably platform dependant, if a
                // node is 32b, it might not agree with 64b nodes.
                hasher.write_usize(field_name.len());
                hasher.write(field_name.as_bytes());
                hasher.write_u64(*modulo)
            }
        }
    }
}

impl Default for InnerRoutingExpr {
    fn default() -> InnerRoutingExpr {
        InnerRoutingExpr::Composite(Vec::new())
    }
}

impl FromStr for InnerRoutingExpr {
    type Err = anyhow::Error;

    fn from_str(expr_dsl_str: &str) -> anyhow::Result<Self> {
        let expr_dsl_str = expr_dsl_str.trim();
        if expr_dsl_str.is_empty() {
            return Ok(Default::default());
        }

        if expr_dsl_str.contains(',') {
            Ok(InnerRoutingExpr::Composite(
                expr_dsl_str
                    .split(',')
                    .map(routing_expression_parse_single_elem)
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        } else {
            routing_expression_parse_single_elem(expr_dsl_str)
        }
    }
}

fn routing_expression_parse_single_elem(expr_dsl_str: &str) -> anyhow::Result<InnerRoutingExpr> {
    let validate_field_name = |field_name: &str| {
        if !field_name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            anyhow::bail!("'{field_name}' is not a valid fieldname");
        }
        if field_name.is_empty() {
            anyhow::bail!("empty fieldname");
        }
        Ok(())
    };

    if let Some((field_name, modulo)) = expr_dsl_str.split_once('%') {
        let field_name = field_name.trim();
        validate_field_name(field_name)?;
        let modulo = modulo.trim().parse()?;

        Ok(InnerRoutingExpr::Modulo(field_name.to_string(), modulo))
    } else {
        let field_name = expr_dsl_str.trim();
        validate_field_name(field_name)?;

        Ok(InnerRoutingExpr::Field(field_name.to_string()))
    }
}

// The display implementation should be consistent with `FromString`.
impl Display for InnerRoutingExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            InnerRoutingExpr::Field(field) => {
                f.write_str(field)?;
            }
            InnerRoutingExpr::Composite(children) => {
                if children.is_empty() {
                    return Ok(());
                }
                children[0].fmt(f)?;
                for child in &children[1..] {
                    write!(f, ",{child}")?;
                }
            }
            InnerRoutingExpr::Modulo(field, modulo) => {
                write!(f, "{}%{}", field, modulo)?;
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
    Modulo,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ser_deser(expr: &InnerRoutingExpr) {
        let ser = expr.to_string();
        assert_eq!(&InnerRoutingExpr::from_str(&ser).unwrap(), expr);
    }

    fn deser_util(expr_dsl: &str) -> InnerRoutingExpr {
        let expr = InnerRoutingExpr::from_str(expr_dsl).unwrap();
        test_ser_deser(&expr);
        expr
    }

    #[test]
    fn test_routing_expr_empty() {
        let routing_expr = deser_util("");
        assert!(matches!(routing_expr, InnerRoutingExpr::Composite(leaves) if leaves.is_empty()));
    }

    #[test]
    fn test_routing_expr_empty_hashes_to_0() {
        let expr = RoutingExpr::new("").unwrap();
        let ctx: serde_json::Map<String, JsonValue> = Default::default();
        assert_eq!(expr.eval_hash(&ctx), 0u64);
    }

    #[test]
    fn test_routing_expr_single_field() {
        let routing_expr = deser_util("tenant_id");
        assert_eq!(
            routing_expr,
            InnerRoutingExpr::Field("tenant_id".to_owned())
        );
    }

    #[test]
    fn test_routing_expr_single_field_modulo() {
        let routing_expr = deser_util("tenant_id%16");
        assert_eq!(
            routing_expr,
            InnerRoutingExpr::Modulo("tenant_id".to_owned(), 16)
        );
    }

    #[test]
    fn test_routing_expr_multiple_field() {
        let routing_expr = deser_util("tenant_id,app_id%4");

        assert_eq!(
            routing_expr,
            InnerRoutingExpr::Composite(vec![
                InnerRoutingExpr::Field("tenant_id".to_owned()),
                InnerRoutingExpr::Modulo("app_id".to_owned(), 4),
            ])
        );
    }

    // This unit test is here to ensure that the routing expr hash depends on
    // the expression itself as well as the expression value.
    #[test]
    fn test_routing_expr_depends_on_both_expr_and_value() {
        let routing_expr = RoutingExpr::new("tenant_id").unwrap();
        let routing_expr2 = RoutingExpr::new("app").unwrap();
        let ctx: serde_json::Map<String, JsonValue> =
            serde_json::from_str(r#"{"tenant_id": "happy", "app": "happy"}"#).unwrap();
        let ctx2: serde_json::Map<String, JsonValue> =
            serde_json::from_str(r#"{"tenant_id": "happy2"}"#).unwrap();
        // This assert is important.
        assert_ne!(routing_expr.eval_hash(&ctx), routing_expr2.eval_hash(&ctx),);
        assert_ne!(routing_expr.eval_hash(&ctx), routing_expr.eval_hash(&ctx2),);
    }

    // This unit test is here to detect a change in the hash logic.
    // Breaking it is not catastrophic but it should not happen too often.
    #[test]
    fn test_routing_expr_change_detection() {
        let routing_expr = RoutingExpr::new("tenant_id").unwrap();
        let ctx: serde_json::Map<String, JsonValue> =
            serde_json::from_str(r#"{"tenant_id": "happy-tenant", "app": "happy"}"#).unwrap();
        assert_eq!(routing_expr.eval_hash(&ctx), 12428134591152806029);
    }

    #[test]
    fn test_routing_expr_missing_value_does_not_panic() {
        let routing_expr = RoutingExpr::new("tenant_id").unwrap();
        let ctx: serde_json::Map<String, JsonValue> = Default::default();
        assert_eq!(routing_expr.eval_hash(&ctx), 9054185009885066538);
    }

    #[test]
    fn test_routing_expr_uses_modulo() {
        let routing_expr = RoutingExpr::new("tenant_id%4").unwrap();
        let ctx: serde_json::Map<String, JsonValue> =
            serde_json::from_str(r#"{"tenant_id": 1}"#).unwrap();
        let ctx2: serde_json::Map<String, JsonValue> =
            serde_json::from_str(r#"{"tenant_id": 5}"#).unwrap();
        let ctx3: serde_json::Map<String, JsonValue> =
            serde_json::from_str(r#"{"tenant_id": -3}"#).unwrap();
        let ctx4: serde_json::Map<String, JsonValue> =
            serde_json::from_str(r#"{"tenant_id": 4}"#).unwrap();

        assert_eq!(routing_expr.eval_hash(&ctx), routing_expr.eval_hash(&ctx2));
        assert_eq!(routing_expr.eval_hash(&ctx), routing_expr.eval_hash(&ctx3));
        assert_ne!(routing_expr.eval_hash(&ctx), routing_expr.eval_hash(&ctx4));
    }
}
