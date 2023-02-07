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
    fn hash_attribute<H: Hasher>(&self, attr_name: &str, hasher: &mut H);
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
            hasher.write_u64(s.len() as u64);
            hasher.write(s.as_bytes());
        }
        JsonValue::Array(arr) => {
            hasher.write_u8(4u8);
            hasher.write_u64(arr.len() as u64);
            for el in arr {
                hash_json_val(el, hasher);
            }
        }
        JsonValue::Object(obj) => {
            hasher.write_u8(5u8);
            hasher.write_u64(obj.len() as u64);
            for (key, val) in obj.iter() {
                hasher.write_u64(key.len() as u64);
                hasher.write(key.as_bytes());
                hash_json_val(val, hasher);
            }
        }
    }
}

impl RoutingExprContext for serde_json::Map<String, JsonValue> {
    fn hash_attribute<H: Hasher>(&self, attr_name: &str, hasher: &mut H) {
        if let Some(json_val) = self.get(attr_name) {
            hasher.write_u8(1u8);
            hash_json_val(json_val, hasher);
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
    Modulo(Box<InnerRoutingExpr>, u64),
    // TODO Enrich me! Map / ...
}

impl InnerRoutingExpr {
    fn eval_hash<Ctx: RoutingExprContext, H: Hasher + Default>(&self, ctx: &Ctx, hasher: &mut H) {
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
            InnerRoutingExpr::Modulo(inner_expr, modulo) => {
                ExprType::Modulo.hash(hasher);

                let mut sub_hasher = H::default();
                inner_expr.eval_hash(ctx, &mut sub_hasher);
                hasher.write_u64(sub_hasher.finish() % modulo);
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
                hasher.write_u64(field_name.len() as u64);
                hasher.write(field_name.as_bytes());
            }
            InnerRoutingExpr::Composite(children) => {
                ExprType::Composite.hash(hasher);
                for child in children {
                    child.hash(hasher);
                }
            }
            InnerRoutingExpr::Modulo(inner_expr, modulo) => {
                ExprType::Modulo.hash(hasher);
                inner_expr.hash(hasher);
                hasher.write_u64(*modulo);
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
        let ast = expression_dsl::parse_expression(expr_dsl_str)?;

        convert_ast(ast)
    }
}

fn convert_ast(ast: Vec<expression_dsl::ExpressionAst>) -> anyhow::Result<InnerRoutingExpr> {
    use expression_dsl::{Argument, ExpressionAst};

    let mut result = ast.into_iter().map(|ast_elem|
        match ast_elem {
            ExpressionAst::Field(field_name) => Ok(InnerRoutingExpr::Field(field_name)),
            ExpressionAst::Function { name, mut args } => {
                match &*name {
                    "hash_mod" => {
                        if args.len() !=2 {
                            anyhow::bail!("Invalid arguments for `hash_mod`: expected 2 arguments, found {}", args.len());
                        }

                        let Argument::Expression(fields) = args.remove(0) else {
                            anyhow::bail!("Invalid 1st argument for `hash_mod`: expected expression");
                        };

                        let Argument::Number(modulo) = args.remove(0) else {
                            anyhow::bail!("Invalid 2nd argument for `hash_mod`: expected number");
                        };

                        Ok(InnerRoutingExpr::Modulo(Box::new(convert_ast(fields)?), modulo))
                    },
                    _ => anyhow::bail!("Unknown function `{}`", name),
                }
            },
        }
    ).collect::<Result<Vec<_>, _>>()?;
    if result.is_empty() {
        Ok(InnerRoutingExpr::default())
    } else if result.len() == 1 {
        Ok(result.remove(0))
    } else {
        Ok(InnerRoutingExpr::Composite(result))
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
            InnerRoutingExpr::Modulo(inner_expr, modulo) => {
                write!(f, "hash_mod(({inner_expr}), {modulo})")?;
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

mod expression_dsl {
    use nom::bytes::complete::tag;
    use nom::character::complete::multispace0;
    use nom::combinator::{eof, opt};
    use nom::error::ErrorKind;
    use nom::multi::separated_list0;
    use nom::sequence::{delimited, tuple};
    use nom::{AsChar, Finish, IResult, InputTakeAtPosition};

    // this is a RoutingSubExpr in our DSL.
    #[derive(Debug, PartialEq, Eq, Clone)]
    pub(crate) enum ExpressionAst {
        Field(String),
        Function { name: String, args: Vec<Argument> },
    }

    #[derive(Debug, PartialEq, Eq, Clone)]
    pub(crate) enum Argument {
        Expression(Vec<ExpressionAst>),
        Number(u64),
    }

    pub(crate) fn parse_expression(expr_dsl_str: &str) -> anyhow::Result<Vec<ExpressionAst>> {
        let (i, res) = routing_expr(expr_dsl_str)
            .finish()
            .map_err(|e| anyhow::anyhow!("error parsing routing expression: {e}"))?;
        eof::<_, ()>(i)?;

        Ok(res)
    }

    // DSL:
    //
    // RoutingExpr := RoutingSubExpr [ , RoutingExpr ]
    // RougingSubExpr := Identifier [ \( Arguments \) ]
    // Identifier := FieldChar [ Identifier ]
    // FieldChar := { a..z | A..Z | 0..9 | _ }
    // Arguments := Argument [ , Arguments ]
    // Argument := { \( RoutingExpr \) | RoutingSubExpr | DirectValue }
    // # We may want other DirectValue in the future
    // DirectValue := Number
    // Number := { 0..9 } [ Number ]

    fn routing_expr(input: &str) -> IResult<&str, Vec<ExpressionAst>> {
        separated_list0(wtag(","), routing_sub_expr)(input)
    }

    fn routing_sub_expr(input: &str) -> IResult<&str, ExpressionAst> {
        let (input, identifier) = identifier(input)?;
        let (input, args) = opt(tuple((wtag("("), arguments, wtag(")"))))(input)?;
        let res = if let Some((_, args, _)) = args {
            ExpressionAst::Function {
                name: identifier.to_owned(),
                args,
            }
        } else {
            ExpressionAst::Field(identifier.to_owned())
        };
        Ok((input, res))
    }

    fn identifier(input: &str) -> IResult<&str, &str> {
        input.split_at_position1_complete(
            |item| !(item.is_alphanum() || item == '_'),
            ErrorKind::AlphaNumeric,
        )
    }

    fn arguments(input: &str) -> IResult<&str, Vec<Argument>> {
        separated_list0(wtag(","), argument)(input)
    }

    fn argument(input: &str) -> IResult<&str, Argument> {
        if let Ok((input, number)) = number(input) {
            Ok((input, Argument::Number(number)))
        } else if let Ok((input, (_, arg, _))) = tuple((wtag("("), routing_expr, wtag(")")))(input)
        {
            Ok((input, Argument::Expression(arg)))
        } else {
            routing_sub_expr(input).map(|(input, arg)| (input, Argument::Expression(vec![arg])))
        }
    }

    fn number(input: &str) -> IResult<&str, u64> {
        nom::character::complete::u64(input)
    }

    // tag, but ignore leading and trailing whitespaces
    pub fn wtag<'a, Error: nom::error::ParseError<&'a str>>(
        t: &'a str,
    ) -> impl FnMut(&'a str) -> IResult<&'a str, &'a str, Error> {
        delimited(multispace0, tag(t), multispace0)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

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
    fn test_routing_expr_modulo_field() {
        let routing_expr = deser_util("hash_mod(tenant_id, 4)");
        assert_eq!(
            routing_expr,
            InnerRoutingExpr::Modulo(Box::new(InnerRoutingExpr::Field("tenant_id".to_owned())), 4)
        );
    }

    #[test]
    fn test_routing_expr_modulo_complexe() {
        let routing_expr = deser_util("hash_mod((tenant_id,hash_mod(app_id, 3)), 8),cluster_id");
        assert_eq!(
            routing_expr,
            InnerRoutingExpr::Composite(vec![
                InnerRoutingExpr::Modulo(
                    Box::new(InnerRoutingExpr::Composite(vec![
                        InnerRoutingExpr::Field("tenant_id".to_owned()),
                        InnerRoutingExpr::Modulo(
                            Box::new(InnerRoutingExpr::Field("app_id".to_owned()),),
                            3
                        ),
                    ])),
                    8
                ),
                InnerRoutingExpr::Field("cluster_id".to_owned()),
            ])
        );
    }

    #[test]
    fn test_routing_expr_multiple_field() {
        let routing_expr = deser_util("tenant_id,app_id");

        assert_eq!(
            routing_expr,
            InnerRoutingExpr::Composite(vec![
                InnerRoutingExpr::Field("tenant_id".to_owned()),
                InnerRoutingExpr::Field("app_id".to_owned()),
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
    fn test_routing_expr_mod() {
        let mut seen = HashSet::new();
        let routing_expr = RoutingExpr::new("hash_mod(tenant_id, 10)").unwrap();

        for i in 0..1000 {
            let ctx: serde_json::Map<String, JsonValue> =
                serde_json::from_str(&format!(r#"{{"tenant_id": "happy{i}"}}"#)).unwrap();
            seen.insert(routing_expr.eval_hash(&ctx));
        }

        assert_eq!(seen.len(), 10);
    }
}
