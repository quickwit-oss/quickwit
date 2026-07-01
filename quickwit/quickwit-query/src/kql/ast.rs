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

/// Parsed KQL expression. This is an internal representation that lowers to
/// `QueryAst` via `lower_kql_ast`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KqlAst {
    /// Conjunction of subqueries. Empty vector is not produced by the parser.
    And(Vec<KqlAst>),
    /// Disjunction of subqueries.
    Or(Vec<KqlAst>),
    /// Negation of a subquery.
    Not(Box<KqlAst>),
    /// `field:value` clause.
    FieldValue { field: String, value: KqlValue },
    /// `field:<op><value>` numeric/datetime range bound.
    FieldRange {
        field: String,
        op: RangeOp,
        value: String,
    },
    /// `field:*` — checks whether the field is present on the document.
    FieldExists { field: String },
    /// Bare value with no field qualifier — matches against default fields.
    DefaultValue(KqlValue),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KqlValue {
    /// Unquoted token. May contain `*` and `?` wildcards.
    Literal(String),
    /// Double-quoted phrase. Wildcards inside are treated literally.
    Phrase(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RangeOp {
    Gt,
    Gte,
    Lt,
    Lte,
}
