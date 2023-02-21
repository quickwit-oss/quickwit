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

use tantivy::query::{Query, QueryParser, QueryParserError};
use tantivy::schema::{Field, Schema};
use tantivy::tokenizer::TokenizerManager;
use tantivy_query_grammar::{
    Occur as TantivyOccur, UserInputAst, UserInputBound, UserInputLeaf, UserInputLiteral,
};

use crate::{SearchInputAst, SearchInputLeaf};

pub fn build_query_from_search_input_ast(
    schema: Schema,
    search_fields: Vec<Field>,
    input_ast: SearchInputAst,
    tokenizer_manager: TokenizerManager,
) -> Result<Box<dyn Query>, QueryParserError> {
    let user_input_ast = convert_to_user_input_ast(input_ast);
    let mut query_parser = QueryParser::new(schema, search_fields, tokenizer_manager);
    query_parser.set_conjunction_by_default();
    query_parser.build_query(user_input_ast)
}

fn convert_to_user_input_ast(input_ast: SearchInputAst) -> UserInputAst {
    match input_ast {
        SearchInputAst::Clause(clauses) => {
            let converted_clauses = clauses
                .into_iter()
                .map(|(occur_opt, sub_ast)| {
                    (
                        occur_opt.map(TantivyOccur::from),
                        convert_to_user_input_ast(sub_ast),
                    )
                })
                .collect::<Vec<_>>();
            UserInputAst::Clause(converted_clauses)
        }
        SearchInputAst::Leaf(leaf) => {
            UserInputAst::Leaf(Box::new(convert_to_user_input_leaf(*leaf)))
        }
        SearchInputAst::Boost(ast, boost) => {
            UserInputAst::Boost(Box::new(convert_to_user_input_ast(*ast)), boost)
        }
    }
}

fn convert_to_user_input_leaf(input_leaf: SearchInputLeaf) -> UserInputLeaf {
    match input_leaf {
        SearchInputLeaf::Literal(literal) => UserInputLeaf::Literal(UserInputLiteral {
            field_name: literal.field_name_opt,
            phrase: literal.phrase,
            slop: literal.slop,
        }),
        SearchInputLeaf::All => UserInputLeaf::All,
        SearchInputLeaf::Range {
            field_opt,
            lower,
            upper,
        } => UserInputLeaf::Range {
            field: field_opt,
            lower: UserInputBound::from(lower),
            upper: UserInputBound::from(upper),
        },
        SearchInputLeaf::Set {
            field_opt,
            elements,
        } => UserInputLeaf::Set {
            field: field_opt,
            elements,
        },
    }
}
