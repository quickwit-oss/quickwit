// Copyright (C) 2023 Quickwit, Inc.
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

use std::fmt;
use std::fmt::{Debug, Formatter};

use serde::{Deserialize, Serialize};
use tantivy_query_grammar::UserInputBound;

use crate::Occur;

#[derive(PartialEq, Serialize, Deserialize)]
pub enum SearchInputLeaf {
    Literal(SearchInputLiteral),
    All,
    Range {
        field_opt: Option<String>,
        lower: SearchInputBound,
        upper: SearchInputBound,
    },
    Set {
        field_opt: Option<String>,
        elements: Vec<String>,
    },
}

impl SearchInputLeaf {
    pub fn literal(field_name_opt: Option<String>, phrase: String, slop: u32) -> Self {
        Self::Literal(SearchInputLiteral {
            field_name_opt,
            phrase,
            slop,
        })
    }

    pub fn all() -> Self {
        Self::All
    }

    pub fn range(
        field_opt: Option<String>,
        lower: SearchInputBound,
        upper: SearchInputBound,
    ) -> Self {
        Self::Range {
            field_opt,
            lower,
            upper,
        }
    }

    pub fn set(field_opt: Option<String>, elements: Vec<String>) -> Self {
        Self::Set {
            field_opt,
            elements,
        }
    }
}

impl Debug for SearchInputLeaf {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            SearchInputLeaf::Literal(literal) => literal.fmt(formatter),
            SearchInputLeaf::Range {
                ref field_opt,
                ref lower,
                ref upper,
            } => {
                if let Some(ref field) = field_opt {
                    write!(formatter, "\"{field}\":")?;
                }
                lower.display_lower(formatter)?;
                write!(formatter, " TO ")?;
                upper.display_upper(formatter)?;
                Ok(())
            }
            SearchInputLeaf::Set {
                field_opt,
                elements,
            } => {
                if let Some(ref field) = field_opt {
                    write!(formatter, "\"{field}\": ")?;
                }
                write!(formatter, "IN [")?;
                for (i, element) in elements.iter().enumerate() {
                    if i != 0 {
                        write!(formatter, " ")?;
                    }
                    write!(formatter, "\"{element}\"")?;
                }
                write!(formatter, "]")
            }
            SearchInputLeaf::All => write!(formatter, "*"),
        }
    }
}

#[derive(PartialEq, Serialize, Deserialize)]
pub struct SearchInputLiteral {
    pub field_name_opt: Option<String>,
    pub phrase: String,
    pub slop: u32,
}

impl fmt::Debug for SearchInputLiteral {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        if let Some(ref field) = self.field_name_opt {
            write!(formatter, "\"{field}\":")?;
        }
        write!(formatter, "\"{}\"", self.phrase)?;
        if self.slop > 0 {
            write!(formatter, "~{}", self.slop)?;
        }
        Ok(())
    }
}

#[derive(PartialEq, Serialize, Deserialize)]
pub enum SearchInputBound {
    Inclusive(String),
    Exclusive(String),
    Unbounded,
}

impl SearchInputBound {
    fn display_lower(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            SearchInputBound::Inclusive(ref word) => write!(formatter, "[\"{word}\""),
            SearchInputBound::Exclusive(ref word) => write!(formatter, "{{\"{word}\""),
            SearchInputBound::Unbounded => write!(formatter, "{{\"*\""),
        }
    }

    fn display_upper(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            SearchInputBound::Inclusive(ref word) => write!(formatter, "\"{word}\"]"),
            SearchInputBound::Exclusive(ref word) => write!(formatter, "\"{word}\"}}"),
            SearchInputBound::Unbounded => write!(formatter, "\"*\"}}"),
        }
    }

    pub fn term_str(&self) -> &str {
        match *self {
            SearchInputBound::Inclusive(ref contents) => contents,
            SearchInputBound::Exclusive(ref contents) => contents,
            SearchInputBound::Unbounded => "*",
        }
    }
}

impl From<SearchInputBound> for UserInputBound {
    fn from(bound: SearchInputBound) -> Self {
        match bound {
            SearchInputBound::Inclusive(value) => UserInputBound::Inclusive(value),
            SearchInputBound::Exclusive(value) => UserInputBound::Exclusive(value),
            SearchInputBound::Unbounded => UserInputBound::Unbounded,
        }
    }
}

impl From<UserInputBound> for SearchInputBound {
    fn from(bound: UserInputBound) -> Self {
        match bound {
            UserInputBound::Inclusive(value) => SearchInputBound::Inclusive(value),
            UserInputBound::Exclusive(value) => SearchInputBound::Exclusive(value),
            UserInputBound::Unbounded => SearchInputBound::Unbounded,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum SearchInputAst {
    Clause(Vec<(Option<Occur>, SearchInputAst)>),
    Leaf(Box<SearchInputLeaf>),
    Boost(Box<SearchInputAst>, f64),
}

impl SearchInputAst {
    #[must_use]
    pub fn unary(self, occur: Occur) -> Self {
        Self::Clause(vec![(Some(occur), self)])
    }

    pub fn boost(self, boost: f64) -> Self {
        Self::Boost(Box::new(self), boost)
    }

    fn compose(occur: Occur, asts: Vec<Self>) -> Self {
        assert_ne!(occur, Occur::MustNot);
        assert!(!asts.is_empty());
        if asts.len() == 1 {
            asts.into_iter().next().unwrap() //< safe
        } else {
            Self::Clause(
                asts.into_iter()
                    .map(|ast: Self| (Some(occur), ast))
                    .collect::<Vec<_>>(),
            )
        }
    }

    pub fn empty_query() -> Self {
        Self::Clause(Vec::default())
    }

    pub fn leaf(leaf: SearchInputLeaf) -> Self {
        Self::Leaf(Box::new(leaf))
    }

    pub fn and(asts: Vec<Self>) -> Self {
        Self::compose(Occur::Must, asts)
    }

    pub fn or(asts: Vec<Self>) -> Self {
        Self::compose(Occur::Should, asts)
    }
}

impl From<SearchInputLiteral> for SearchInputLeaf {
    fn from(literal: SearchInputLiteral) -> SearchInputLeaf {
        SearchInputLeaf::Literal(literal)
    }
}

impl From<SearchInputLeaf> for SearchInputAst {
    fn from(leaf: SearchInputLeaf) -> SearchInputAst {
        SearchInputAst::Leaf(Box::new(leaf))
    }
}

fn print_occur_ast(
    occur_opt: Option<Occur>,
    ast: &SearchInputAst,
    formatter: &mut fmt::Formatter,
) -> fmt::Result {
    if let Some(occur) = occur_opt {
        write!(formatter, "{occur}{ast:?}")?;
    } else {
        write!(formatter, "*{ast:?}")?;
    }
    Ok(())
}

impl fmt::Debug for SearchInputAst {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SearchInputAst::Clause(ref subqueries) => {
                if subqueries.is_empty() {
                    write!(formatter, "<emptyclause>")?;
                } else {
                    write!(formatter, "(")?;
                    print_occur_ast(subqueries[0].0, &subqueries[0].1, formatter)?;
                    for subquery in &subqueries[1..] {
                        write!(formatter, " ")?;
                        print_occur_ast(subquery.0, &subquery.1, formatter)?;
                    }
                    write!(formatter, ")")?;
                }
                Ok(())
            }
            SearchInputAst::Leaf(ref subquery) => write!(formatter, "{subquery:?}"),
            SearchInputAst::Boost(ref leaf, boost) => write!(formatter, "({leaf:?})^{boost}"),
        }
    }
}
