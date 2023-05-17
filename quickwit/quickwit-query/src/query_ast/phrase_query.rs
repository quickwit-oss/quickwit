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

use serde::{Deserialize, Serialize};
use tantivy::schema::Schema as TantivySchema;

use crate::query_ast::tantivy_query_ast::TantivyQueryAst;
use crate::query_ast::utils::full_text_query;
use crate::query_ast::{BuildTantivyAst, QueryAst};
use crate::InvalidQuery;

fn is_zero(slop: &u32) -> bool {
    *slop == 0u32
}

/// The PhraseQuery node is meant to be tokenized and searched.
///
/// If after tokenization, a single term is emitted, it will naturally be
/// produce a tantivy TermQuery.
/// If not terms is emitted, it will produce a query that match no documents..
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PhraseQuery {
    pub field: String,
    pub phrase: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub slop: u32,
}

impl From<PhraseQuery> for QueryAst {
    fn from(phrase_query: PhraseQuery) -> Self {
        QueryAst::Phrase(phrase_query)
    }
}

impl BuildTantivyAst for PhraseQuery {
    fn build_tantivy_ast_impl(
        &self,
        schema: &TantivySchema,
        _search_fields: &[String],
        _with_validation: bool,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        full_text_query(&self.field, &self.phrase, self.slop, true, schema)
    }
}
