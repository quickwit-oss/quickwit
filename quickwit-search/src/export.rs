/*
 * Copyright (C) 2021 Quickwit Inc.
 *
 * Quickwit is offered under the AGPL v3.0 and as commercial software.
 * For commercial licensing, contact us at hello@quickwit.io.
 *
 * AGPL:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

use serde::Deserialize;

/// Output format for the search results.
#[derive(Deserialize, Debug, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    /// Format data by row in binary format. TODO: add link to format.
    RowBinary,
    /// Comma Separated Values format (https://datatracker.ietf.org/doc/html/rfc4180).
    /// By default, the delimiter is ,.
    CSV,
}

impl Default for OutputFormat {
    fn default() -> Self {
        OutputFormat::RowBinary
    }
}

impl ToString for OutputFormat {
    fn to_string(&self) -> String {
        match &self {
            Self::RowBinary => "row-binary".to_string(),
            Self::CSV => "csv".to_string(),
        }
    }
}
