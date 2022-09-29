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
use std::num::NonZeroU64;
use std::str::FromStr;
use std::sync::Arc;

use siphasher::sip::SipHasher;

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

#[derive(Clone)]
pub struct RoutingExpr {
    inner_opt: Option<Arc<InnerRoutingExpr>>,
    max_num_partitions: NonZeroU64,
    salted_hasher: SipHasher,
}

impl Default for RoutingExpr {
    fn default() -> Self {
        Self {
            inner_opt: Default::default(),
            max_num_partitions: NonZeroU64::new(1).unwrap(),
            salted_hasher: Default::default(),
        }
    }
}

impl RoutingExpr {
    pub fn new(expr_dsl_str: &str, max_num_partitions: NonZeroU64) -> anyhow::Result<Self> {
        let expr_dsl_str = expr_dsl_str.trim();
        if max_num_partitions.get() <= 1 {
            return Ok(RoutingExpr::default());
        }

        let mut salted_hasher: SipHasher = SipHasher::new();

        if expr_dsl_str.is_empty() {
            return Ok(RoutingExpr {
                inner_opt: None,
                max_num_partitions,
                salted_hasher: SipHasher::new(),
            });
        }

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
            max_num_partitions,
            salted_hasher,
        })
    }

    pub fn max_num_partitions(&self) -> NonZeroU64 {
        self.max_num_partitions
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
    // TODO Enrich me! Map / Modulo
}

impl InnerRoutingExpr {
    fn eval_hash<Ctx: RoutingExprContext, H: Hasher>(&self, ctx: &Ctx, hasher: &mut H) {
        match self {
            InnerRoutingExpr::Field(field_name) => {
                ExprType::Field.hash(hasher);
                ctx.hash_attribute(field_name, hasher);
            }
            InnerRoutingExpr::Composite(children) => {
                ExprType::Composite.hash(hasher);
                for child in children {
                    child.eval_hash(ctx, hasher);
                }
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
                hasher.write_usize(field_name.len());
                hasher.write(field_name.as_bytes());
            }
            InnerRoutingExpr::Composite(children) => {
                ExprType::Composite.hash(hasher);
                for child in children {
                    child.hash(hasher);
                }
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
        if expr_dsl_str.is_empty() {
            return Ok(Default::default());
        }
        Ok(InnerRoutingExpr::Field(expr_dsl_str.to_string()))
    }
}

// The display implementation should be consistent with `FromString`.
impl Display for InnerRoutingExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            InnerRoutingExpr::Field(field) => {
                write!(f, "{}", field)?;
            }
            InnerRoutingExpr::Composite(children) => {
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
        if let Some(inner) = self.inner_opt.as_ref() {
            let mut hasher: SipHasher = self.salted_hasher;
            inner.eval_hash(ctx, &mut hasher);
            hasher.finish() % self.max_num_partitions.get()
        } else {
            0u64
        }
    }
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
        let expr = RoutingExpr::new("", NonZeroU64::new(10).unwrap()).unwrap();
        let ctx: serde_json::Map<String, serde_json::Value> = Default::default();
        assert_eq!(expr.eval_hash(&ctx), 0u64);
    }

    #[test]
    fn test_routing_expr_single_field() {
        let routing_expr = deser_util("tenant_id");
        assert!(
            matches!(routing_expr, InnerRoutingExpr::Field(attr_name) if attr_name == "tenant_id")
        );
    }

    const MAX_NUM_PARTITIONS: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(10) };

    // This unit test is here to ensure that the routing expr hash depends on
    // the expression itself as well as the expression value.
    #[test]
    fn test_routing_expr_depends_on_both_expr_and_value() {
        let routing_expr = RoutingExpr::new("tenant_id", MAX_NUM_PARTITIONS).unwrap();
        let routing_expr2 = RoutingExpr::new("app", MAX_NUM_PARTITIONS).unwrap();
        let ctx: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"tenant_id": "happy", "app": "happy"}"#).unwrap();
        let ctx2: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"tenant_id": "happy2"}"#).unwrap();
        // This assert is important.
        assert_ne!(routing_expr.eval_hash(&ctx), routing_expr2.eval_hash(&ctx),);
        assert_ne!(routing_expr.eval_hash(&ctx), routing_expr.eval_hash(&ctx2),);
    }

    // This unit test is here to detect a change in the hash logic.
    // Breaking it is not catastrophic but it should not happen too often.
    #[test]
    fn test_routing_expr_change_detection() {
        let routing_expr = RoutingExpr::new("tenant_id", MAX_NUM_PARTITIONS).unwrap();
        let ctx: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"tenant_id": "happy-tenant", "app": "happy"}"#).unwrap();
        assert_eq!(routing_expr.eval_hash(&ctx), 9u64);
    }
}
