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

use std::ops::Bound;

use serde::{Deserialize, Serialize};

use crate::elastic_query_dsl::one_field_map::OneFieldMap;
use crate::elastic_query_dsl::ConvertableToQueryAst;
use crate::not_nan_f32::NotNaNf32;
use crate::query_ast::QueryAst;
use crate::JsonLiteral;

#[derive(Serialize, Deserialize, Debug, Default, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct RangeQueryParams {
    #[serde(default)]
    gt: Option<JsonLiteral>,
    #[serde(default)]
    gte: Option<JsonLiteral>,
    #[serde(default)]
    lt: Option<JsonLiteral>,
    #[serde(default)]
    lte: Option<JsonLiteral>,
    #[serde(default)]
    boost: Option<NotNaNf32>,
}

pub type RangeQuery = OneFieldMap<RangeQueryParams>;

impl ConvertableToQueryAst for RangeQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        let field = self.field;
        let RangeQueryParams {
            gt,
            gte,
            lt,
            lte,
            boost,
        } = self.value;
        let range_query_ast = crate::query_ast::RangeQuery {
            field,
            lower_bound: match (gt, gte) {
                (Some(_gt), Some(_gte)) => {
                    anyhow::bail!("Both gt and gte are set")
                }
                (Some(gt), None) => Bound::Excluded(gt),
                (None, Some(gte)) => Bound::Included(gte),
                (None, None) => Bound::Unbounded,
            },
            upper_bound: match (lt, lte) {
                (Some(_lt), Some(_lte)) => {
                    anyhow::bail!("Both lt and lte are set")
                }
                (Some(lt), None) => Bound::Excluded(lt),
                (None, Some(lte)) => Bound::Included(lte),
                (None, None) => Bound::Unbounded,
            },
        };
        let ast: QueryAst = range_query_ast.into();
        Ok(ast.boost(boost))
    }
}
