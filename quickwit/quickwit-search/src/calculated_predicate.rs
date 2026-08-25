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

use std::collections::HashSet;

use anyhow::{Context, bail};
use quickwit_proto::search::calculated_predicate_expr::Node;
use quickwit_proto::search::calculated_predicate_func_call::Function as ProtoFunction;
use quickwit_proto::search::calculated_predicate_literal::Value;
use quickwit_proto::search::{
    CalculatedPredicate, CalculatedPredicateExpr, CalculatedPredicateFuncCall,
    CalculatedPredicateLiteral,
};
use tantivy::jitexpr::ast::{Function, Literal, UntypedExpr};
use tantivy::query::CalculatedPredicateQuery;

pub(crate) fn calculated_predicate_query(
    predicate: &CalculatedPredicate,
) -> anyhow::Result<(CalculatedPredicateQuery, HashSet<String>)> {
    let expression = predicate
        .expr
        .as_ref()
        .context("calculated predicate is missing its root expression")?;
    let mut variables = HashSet::new();
    let expression = to_untyped_expr(expression, &mut variables)?;
    let query =
        CalculatedPredicateQuery::new(expression).context("invalid calculated predicate")?;
    Ok((query, variables))
}

fn to_untyped_expr(
    expression: &CalculatedPredicateExpr,
    variables: &mut HashSet<String>,
) -> anyhow::Result<UntypedExpr> {
    match expression.node.as_ref() {
        Some(Node::Literal(literal)) => literal_to_untyped_expr(literal),
        Some(Node::Variable(variable)) => {
            variables.insert(variable.clone());
            Ok(UntypedExpr::variable(variable))
        }
        Some(Node::FuncCall(func_call)) => func_call_to_untyped_expr(func_call, variables),
        None => bail!("calculated predicate expression is missing a node"),
    }
}

fn literal_to_untyped_expr(literal: &CalculatedPredicateLiteral) -> anyhow::Result<UntypedExpr> {
    let literal = match literal.value.as_ref() {
        Some(Value::IntValue(value)) => Literal::I64(*value),
        Some(Value::UintValue(value)) => Literal::U64(*value),
        Some(Value::DoubleValueBits(value)) => Literal::F64(f64::from_bits(*value)),
        Some(Value::StringValue(value)) => Literal::from(value.as_str()),
        Some(Value::BoolValue(value)) => Literal::Bool(*value),
        None => bail!("calculated predicate literal is missing a value"),
    };
    Ok(UntypedExpr::Literal(literal))
}

fn func_call_to_untyped_expr(
    func_call: &CalculatedPredicateFuncCall,
    variables: &mut HashSet<String>,
) -> anyhow::Result<UntypedExpr> {
    let function = ProtoFunction::try_from(func_call.function)
        .context("unknown calculated predicate function")?;
    let function = to_jitexpr_function(function)?;
    let mut args = Vec::with_capacity(func_call.args.len());
    for arg in &func_call.args {
        args.push(to_untyped_expr(arg, variables)?);
    }
    Ok(UntypedExpr::Call { function, args })
}

fn to_jitexpr_function(function: ProtoFunction) -> anyhow::Result<Function> {
    match function {
        ProtoFunction::Unspecified => bail!("calculated predicate function is unspecified"),
        ProtoFunction::Abs => Ok(Function::Abs),
        ProtoFunction::And => Ok(Function::And),
        ProtoFunction::Ceil => Ok(Function::Ceil),
        ProtoFunction::Concat => Ok(Function::Concat),
        ProtoFunction::Add => Ok(Function::Add),
        ProtoFunction::Divide => Ok(Function::Divide),
        ProtoFunction::Eq => Ok(Function::Eq),
        ProtoFunction::Floor => Ok(Function::Floor),
        ProtoFunction::Gt => Ok(Function::Gt),
        ProtoFunction::GtEq => Ok(Function::GtEq),
        ProtoFunction::If => Ok(Function::If),
        ProtoFunction::IntMod => Ok(Function::IntMod),
        ProtoFunction::Left => Ok(Function::Left),
        ProtoFunction::Lt => Ok(Function::Lt),
        ProtoFunction::LtEq => Ok(Function::LtEq),
        ProtoFunction::IsNotNull => Ok(Function::IsNotNull),
        ProtoFunction::IsNull => Ok(Function::IsNull),
        ProtoFunction::Lower => Ok(Function::Lower),
        ProtoFunction::Max => Ok(Function::Max),
        ProtoFunction::Min => Ok(Function::Min),
        ProtoFunction::Multiply => Ok(Function::Multiply),
        ProtoFunction::Neq => Ok(Function::Neq),
        ProtoFunction::Not => Ok(Function::Not),
        ProtoFunction::Or => Ok(Function::Or),
        ProtoFunction::Pow => Ok(Function::Pow),
        ProtoFunction::Sqrt => Ok(Function::Sqrt),
        ProtoFunction::RegexpExtract => Ok(Function::RegexpExtract),
        ProtoFunction::RegexpLike => Ok(Function::RegexpLike),
        ProtoFunction::Right => Ok(Function::Right),
        ProtoFunction::Round => Ok(Function::Round),
        ProtoFunction::SplitAfter => Ok(Function::SplitAfter),
        ProtoFunction::SplitBefore => Ok(Function::SplitBefore),
        ProtoFunction::Subtract => Ok(Function::Subtract),
        ProtoFunction::Substring => Ok(Function::Substring),
        ProtoFunction::SubstringCount => Ok(Function::SubstringCount),
        ProtoFunction::TextJoin => Ok(Function::TextJoin),
        ProtoFunction::Trim => Ok(Function::Trim),
        ProtoFunction::Upper => Ok(Function::Upper),
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::search::calculated_predicate_expr::Node;
    use quickwit_proto::search::calculated_predicate_func_call::Function as ProtoFunction;
    use quickwit_proto::search::calculated_predicate_literal::Value;
    use quickwit_proto::search::{
        CalculatedPredicate, CalculatedPredicateExpr, CalculatedPredicateFuncCall,
        CalculatedPredicateLiteral,
    };

    use super::calculated_predicate_query;

    fn variable(name: &str) -> CalculatedPredicateExpr {
        CalculatedPredicateExpr {
            node: Some(Node::Variable(name.to_string())),
        }
    }

    fn literal(value: Value) -> CalculatedPredicateExpr {
        CalculatedPredicateExpr {
            node: Some(Node::Literal(CalculatedPredicateLiteral {
                value: Some(value),
            })),
        }
    }

    fn call(
        function: ProtoFunction,
        args: Vec<CalculatedPredicateExpr>,
    ) -> CalculatedPredicateExpr {
        CalculatedPredicateExpr {
            node: Some(Node::FuncCall(CalculatedPredicateFuncCall {
                function: function as i32,
                args,
            })),
        }
    }

    #[test]
    fn test_calculated_predicate_query_accepts_boolean_expression() {
        let predicate = CalculatedPredicate {
            expr: Some(call(
                ProtoFunction::Gt,
                vec![variable("latency_ms"), literal(Value::IntValue(100))],
            )),
        };

        let (_query, variables) = calculated_predicate_query(&predicate).unwrap();
        assert_eq!(variables, ["latency_ms".to_string()].into_iter().collect());
    }

    #[test]
    fn test_calculated_predicate_query_rejects_missing_root() {
        let predicate = CalculatedPredicate { expr: None };

        assert!(
            calculated_predicate_query(&predicate)
                .unwrap_err()
                .to_string()
                .contains("root expression")
        );
    }

    #[test]
    fn test_calculated_predicate_query_rejects_non_boolean_expression() {
        let predicate = CalculatedPredicate {
            expr: Some(literal(Value::StringValue("hello".to_string()))),
        };

        assert!(
            calculated_predicate_query(&predicate)
                .unwrap_err()
                .to_string()
                .contains("invalid calculated predicate")
        );
    }

    #[test]
    fn test_calculated_predicate_query_rejects_unspecified_function() {
        let predicate = CalculatedPredicate {
            expr: Some(call(ProtoFunction::Unspecified, Vec::new())),
        };

        assert!(
            calculated_predicate_query(&predicate)
                .unwrap_err()
                .to_string()
                .contains("unspecified")
        );
    }

    #[test]
    fn test_double_literals_are_encoded_as_bits() {
        let predicate = CalculatedPredicate {
            expr: Some(call(
                ProtoFunction::LtEq,
                vec![
                    variable("score"),
                    literal(Value::DoubleValueBits(12.5f64.to_bits())),
                ],
            )),
        };

        calculated_predicate_query(&predicate).unwrap();
    }
}
