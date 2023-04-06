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
use tantivy::schema::Schema;

use crate::quickwit_query_ast::tantivy_query_ast::TantivyQueryAst;
use crate::quickwit_query_ast::utils::compute_query;
use crate::quickwit_query_ast::{IntoTantivyAst, QueryAst};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PhraseQuery {
    pub field: String,
    pub phrase: String,
    pub slop: u32,
}

impl From<PhraseQuery> for QueryAst {
    fn from(phrase_query: PhraseQuery) -> Self {
        QueryAst::Phrase(phrase_query)
    }
}

impl IntoTantivyAst for PhraseQuery {
    fn into_tantivy_ast(&self, schema: &Schema) -> anyhow::Result<TantivyQueryAst> {
        Ok(compute_query(&self.field, &self.phrase, true, schema))
    }
}
