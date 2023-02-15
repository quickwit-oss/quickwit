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

use tantivy_query_grammar::{UserInputAst, UserInputLeaf};

use crate::{SearchInputAst, Occur, SearchInputLeaf, SearchInputBound, SearchInputLiteral};

/// parses a query to and intermediate SearchInputAst
/// 
/// Given SearchInputAst is a direct copy of tantivy_query_grammar::UserInputAst,
/// We could just make serializable tantivy_query_grammar::UserInputAst to avoid this step.
/// 
/// However, SearchInputAst could evolve if kept separate.
/// 
pub fn parse_tantivy_dsl(query: &str) -> anyhow::Result<SearchInputAst> {
    let user_input_ast = tantivy_query_grammar::parse_query(query)
        .map_err(|_| anyhow::anyhow!("Query parsing error.".to_string()))?;
    Ok(convert_user_input_ast_to_search_ast(user_input_ast))
}

fn convert_user_input_ast_to_search_ast(user_input_ast: UserInputAst) -> SearchInputAst {
    match user_input_ast {
        UserInputAst::Clause(clauses) => {
            let converted_clauses = clauses.into_iter().map(|(occur_opt, sub_ast)| {
                (occur_opt.map(|occur| Occur::from(occur)), convert_user_input_ast_to_search_ast(sub_ast))
            }).collect::<Vec<_>>();
            SearchInputAst::Clause(converted_clauses)
        },
        UserInputAst::Leaf(leaf) => {
            SearchInputAst::Leaf(Box::new(convert_to_user_input_leaf(*leaf)))
        },
        UserInputAst::Boost(ast, boost) => {
            SearchInputAst::Boost(Box::new(convert_user_input_ast_to_search_ast(*ast)), boost)
        },
    }
}

fn convert_to_user_input_leaf(input_leaf: UserInputLeaf) -> SearchInputLeaf {
    match input_leaf {
        UserInputLeaf::Literal(literal) => SearchInputLeaf::Literal(SearchInputLiteral{
            field_name_opt: literal.field_name,
            phrase: literal.phrase,
            slop: literal.slop,
        }),
        UserInputLeaf::All => SearchInputLeaf::All,
        UserInputLeaf::Range { field, lower, upper } => SearchInputLeaf::Range { 
            field_opt: field, 
            lower: SearchInputBound::from(lower), 
            upper: SearchInputBound::from(upper),
        },
        UserInputLeaf::Set { field, elements } => SearchInputLeaf::Set{ field_opt: field, elements},
    }
}
