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

use std::borrow::Cow;
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;

pub(crate) use expression_dsl::parse_field_name;
use serde_json::Value as JsonValue;
use siphasher::sip::SipHasher;

pub trait RoutingExprContext {
    fn hash_attribute<H: Hasher>(&self, attr_name: &[String], hasher: &mut H);
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

fn find_value<'a>(mut root: &'a JsonValue, keys: &[String]) -> Option<&'a JsonValue> {
    for key in keys {
        match root {
            JsonValue::Object(obj) => {
                root = obj.get(key)?;
            }
            _ => return None,
        }
    }
    Some(root)
}

fn find_value_in_map<'a>(
    obj: &'a serde_json::Map<String, JsonValue>,
    keys: &[String],
) -> Option<&'a JsonValue> {
    // we can't have an empty path and this is used only for the root map, so there is no risk of
    // out of bound
    if let Some(value) = obj.get(&keys[0]) {
        find_value(value, &keys[1..])
    } else {
        None
    }
}

impl RoutingExprContext for serde_json::Map<String, JsonValue> {
    fn hash_attribute<H: Hasher>(&self, attr_name: &[String], hasher: &mut H) {
        if let Some(json_val) = find_value_in_map(self, attr_name) {
            hasher.write_u8(1u8);
            hash_json_val(json_val, hasher);
        } else {
            hasher.write_u8(0u8);
        }
    }
}

/// which defines a routing expression
#[derive(Clone, Default)]
pub struct RoutingExpr {
    inner_opt: Option<Arc<InnerRoutingExpr>>,
    salted_hasher: SipHasher,
}

impl RoutingExpr {
    /// Construct a routing expression from a expression dsl string
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

    /// return all fields in a vector
    pub fn field_names(&self) -> Vec<String> {
        if let Some(inner) = self.inner_opt.as_ref() {
            inner.field_names()
        } else {
            Vec::new()
        }
    }
}

impl Display for RoutingExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(inner_expr) = self.inner_opt.as_ref() {
            inner_expr.fmt(f)
        } else {
            write!(f, "")
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum InnerRoutingExpr {
    Field(Vec<String>),
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

    // return all fields in a vector
    fn field_names(&self) -> Vec<String> {
        match self {
            InnerRoutingExpr::Field(field_name) => vec![field_name.join(".")],
            InnerRoutingExpr::Composite(children) => {
                let mut fields = Vec::new();
                for child in children {
                    fields.extend(child.field_names());
                }
                fields
            }
            InnerRoutingExpr::Modulo(inner_expr, _) => inner_expr.field_names(),
        }
    }
}

// We don't rely on Derive here to make it easier to keep the
// implementation stable.
#[allow(clippy::derived_hash_with_manual_eq)]
impl Hash for InnerRoutingExpr {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        match self {
            InnerRoutingExpr::Field(field_name) => {
                ExprType::Field.hash(hasher);
                hasher.write_u64(field_name.len() as u64);
                for (index, field) in field_name.iter().enumerate() {
                    if index != 0 {
                        hasher.write_u8(b'.');
                    }
                    hasher.write(field.as_bytes());
                }
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

    let mut result = ast
        .into_iter()
        .map(|ast_elem| match ast_elem {
            ExpressionAst::Field(field_name) => {
                let field_path = expression_dsl::parse_field_name(&field_name)?
                    .into_iter()
                    .map(Cow::into_owned)
                    .collect();
                Ok(InnerRoutingExpr::Field(field_path))
            }
            ExpressionAst::Function { name, mut args } => match &*name {
                "hash_mod" => {
                    if args.len() != 2 {
                        anyhow::bail!(
                            "invalid arguments for `hash_mod`: expected 2 arguments, found {}",
                            args.len()
                        );
                    }

                    let Argument::Expression(fields) = args.remove(0) else {
                        anyhow::bail!("invalid 1st argument for `hash_mod`: expected expression");
                    };

                    let Argument::Number(modulo) = args.remove(0) else {
                        anyhow::bail!("invalid 2nd argument for `hash_mod`: expected number");
                    };

                    Ok(InnerRoutingExpr::Modulo(
                        Box::new(convert_ast(fields)?),
                        modulo,
                    ))
                }
                _ => anyhow::bail!("unknown function `{}`", name),
            },
        })
        .collect::<Result<Vec<_>, _>>()?;
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
                for (index, part) in field.iter().enumerate() {
                    if index != 0 {
                        f.write_str(".")?;
                    }
                    f.write_str(&part.replace('.', r"\."))?;
                }
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
    use std::borrow::Cow;

    use nom::bytes::complete::{escaped, tag};
    use nom::character::complete::multispace0;
    use nom::combinator::{eof, map, opt};
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

    // tag, but ignore leading and trailing whitespaces
    pub fn wtag<'a, Error: nom::error::ParseError<&'a str>>(
        t: &'a str,
    ) -> impl FnMut(&'a str) -> IResult<&'a str, &'a str, Error> {
        delimited(multispace0, tag(t), multispace0)
    }

    // DSL:
    //
    // RoutingExpr := RoutingSubExpr [ , RoutingExpr ]
    // RougingSubExpr := Identifier [ \( Arguments \) ]
    // Identifier := FieldChar [ Identifier ]
    // FieldChar := { a..z | A..Z | 0..9 | _ | . | \ | / | @ | $ }
    // Arguments := Argument [ , Arguments ]
    // Argument := { \( RoutingExpr \) | RoutingSubExpr | DirectValue }
    // # We may want other DirectValue in the future
    // DirectValue := Number
    // Number := { 0..9 } [ Number ]

    /// An entire routing expression, containing comma separated routing sub-expressions
    fn routing_expr(input: &str) -> IResult<&str, Vec<ExpressionAst>> {
        separated_list0(wtag(","), routing_sub_expr)(input)
    }

    /// A sub-part of a routing expression
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

    /// An identifier, it can be either a field name, or a function name. It's returned as is,
    /// without de-escaping.
    fn identifier(input: &str) -> IResult<&str, &str> {
        input.split_at_position1_complete(
            |item| !(item.is_alphanum() || ['_', '-', '.', '\\', '/', '@', '$'].contains(&item)),
            ErrorKind::AlphaNumeric,
        )
    }

    /// Arguments for a function
    fn arguments(input: &str) -> IResult<&str, Vec<Argument>> {
        separated_list0(wtag(","), argument)(input)
    }

    /// A single argument for a function
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

    /// A number
    fn number(input: &str) -> IResult<&str, u64> {
        nom::character::complete::u64(input)
    }

    // functions after this are meant to parse a field into its path component, de-escaping where
    // appropriate

    /// Parse part of a path component, stop at the first . or \
    fn key_identifier(input: &str) -> IResult<&str, &str> {
        input.split_at_position1_complete(
            |item| !(item.is_alphanum() || ['_', '-', '/', '@', '$'].contains(&item)),
            ErrorKind::Fail,
        )
    }

    /// Parse a single path component, separated by dots. De-escape any escaped dot it may contain.
    fn escaped_key(input: &str) -> IResult<&str, Cow<'_, str>> {
        map(escaped(key_identifier, '\\', tag(".")), |s: &str| {
            if s.contains("\\.") {
                Cow::Owned(s.replace("\\.", "."))
            } else {
                Cow::Borrowed(s)
            }
        })(input)
    }

    /// Parse a field name into a path, de-escaping where appropriate.
    pub(crate) fn parse_field_name(input: &str) -> anyhow::Result<Vec<Cow<'_, str>>> {
        let (i, res) = separated_list0(tag("."), escaped_key)(input)
            .finish()
            .map_err(|e| anyhow::anyhow!("error parsing key expression: {e}"))?;
        eof::<_, ()>(i)?;
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[track_caller]
    fn test_ser_deser(expr: &InnerRoutingExpr) {
        let ser = expr.to_string();
        assert_eq!(&InnerRoutingExpr::from_str(&ser).unwrap(), expr);
    }

    #[track_caller]
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
            InnerRoutingExpr::Field(vec!["tenant_id".to_owned()])
        );
    }

    #[test]
    fn test_routing_expr_single_field_special_char() {
        let routing_expr = deser_util(r"abCD01-_/@$\.a.bc");
        assert_eq!(
            routing_expr,
            InnerRoutingExpr::Field(vec![r"abCD01-_/@$.a".to_owned(), "bc".to_string()])
        );
    }

    #[test]
    fn test_routing_expr_single_field_with_dot() {
        let routing_expr = deser_util("app.id");
        assert_eq!(
            routing_expr,
            InnerRoutingExpr::Field(vec!["app".to_owned(), "id".to_owned()])
        );
    }

    #[test]
    fn test_routing_expr_modulo_field() {
        let routing_expr = deser_util("hash_mod(tenant_id, 4)");
        assert_eq!(
            routing_expr,
            InnerRoutingExpr::Modulo(
                Box::new(InnerRoutingExpr::Field(vec!["tenant_id".to_owned()])),
                4
            )
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
                        InnerRoutingExpr::Field(vec!["tenant_id".to_owned()]),
                        InnerRoutingExpr::Modulo(
                            Box::new(InnerRoutingExpr::Field(vec!["app_id".to_owned()]),),
                            3
                        ),
                    ])),
                    8
                ),
                InnerRoutingExpr::Field(vec!["cluster_id".to_owned()]),
            ])
        );
    }

    #[test]
    fn test_routing_expr_multiple_field() {
        let routing_expr = deser_util("tenant_id,app_id");

        assert_eq!(
            routing_expr,
            InnerRoutingExpr::Composite(vec![
                InnerRoutingExpr::Field(vec!["tenant_id".to_owned()]),
                InnerRoutingExpr::Field(vec!["app_id".to_owned()]),
            ])
        );
    }

    #[test]
    fn test_routing_expr_multiple_field_with_dot() {
        let routing_expr = deser_util("tenant.id,app.id");

        assert_eq!(
            routing_expr,
            InnerRoutingExpr::Composite(vec![
                InnerRoutingExpr::Field(vec!["tenant".to_owned(), "id".to_owned()]),
                InnerRoutingExpr::Field(vec!["app".to_owned(), "id".to_owned()]),
            ])
        );
    }

    #[test]
    fn test_parse_field_name() {
        let keys = expression_dsl::parse_field_name("abc").unwrap();
        assert_eq!(keys, vec![String::from("abc")]);
    }

    #[test]
    fn test_parse_field_name_multiple() {
        let keys = expression_dsl::parse_field_name("abc.def").unwrap();
        assert_eq!(keys, vec![String::from("abc"), String::from("def")]);
    }

    #[test]
    fn test_parse_field_name_with_escaped_dot() {
        let keys = expression_dsl::parse_field_name("abc\\.def.hij").unwrap();
        assert_eq!(keys, vec![String::from("abc.def"), String::from("hij")]);
    }

    #[test]
    fn test_parse_field_name_with_special_char() {
        let keys = expression_dsl::parse_field_name("abCD01-_/@$").unwrap();
        assert_eq!(keys, vec![String::from("abCD01-_/@$")]);
    }

    #[test]
    fn test_find_value_with_escaped_dot() {
        let ctx = serde_json::from_str(r#"{"tenant.id": "happy", "app": "happy"}"#).unwrap();
        let keys: Vec<_> = expression_dsl::parse_field_name("tenant\\.id")
            .unwrap()
            .into_iter()
            .map(Cow::into_owned)
            .collect();
        assert_eq!(keys, vec![String::from("tenant.id")]);
        let value = find_value(&ctx, &keys).unwrap();
        assert_eq!(value, &JsonValue::String(String::from("happy")));
    }

    #[test]
    fn test_find_value_with_nested_keys() {
        let ctx = serde_json::from_str(
            r#"{"tenant_id": "happy", "app": {"name": "happy", "id": "123"}}"#,
        )
        .unwrap();
        let keys: Vec<_> = expression_dsl::parse_field_name("app.id")
            .unwrap()
            .into_iter()
            .map(Cow::into_owned)
            .collect();
        assert_eq!(keys, vec!["app", "id"]);
        let value = find_value(&ctx, &keys).unwrap();
        assert_eq!(value, &JsonValue::String(String::from("123")));
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
        assert_eq!(routing_expr.eval_hash(&ctx), 13914409176935416182);
    }

    #[test]
    fn test_routing_expr_missing_value_does_not_panic() {
        let routing_expr = RoutingExpr::new("tenant_id").unwrap();
        let ctx: serde_json::Map<String, JsonValue> = Default::default();
        assert_eq!(routing_expr.eval_hash(&ctx), 12482849403534986143);
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
