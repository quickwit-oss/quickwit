// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

mod collector;
mod leaf;
mod root;

pub use collector::{FastFieldCollector, FastFieldCollectorBuilder};
pub use leaf::leaf_export;
pub use root::root_export;

use serde::Deserialize;
use std::{
    fmt::Display,
    io::{self, Write},
};
use tantivy::fastfield::FastValue;

/// Output format for export.
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
            Self::RowBinary => "rowbinary".to_string(),
            Self::CSV => "csv".to_string(),
        }
    }
}

impl From<String> for OutputFormat {
    fn from(spec: String) -> Self {
        match spec.as_str() {
            "rowbinary" => Self::RowBinary,
            _ => Self::CSV,
        }
    }
}

/// Serialize the values into the `buffer` as bytes.
///
/// Please note that the `buffer` is always cleared.
pub fn serialize<TFastValue: FastValue + Display>(
    values: &[TFastValue],
    buffer: &mut Vec<u8>,
    format: OutputFormat,
) -> io::Result<()> {
    match format {
        OutputFormat::CSV => serialize_csv(values, buffer),
        OutputFormat::RowBinary => serialize_row_binary(values, buffer),
    }
}

fn serialize_csv<TFastValue: FastValue + Display>(
    values: &[TFastValue],
    buffer: &mut Vec<u8>,
) -> io::Result<()> {
    buffer.clear();
    for value in values {
        writeln!(buffer, "{}", value)?;
    }
    Ok(())
}

fn serialize_row_binary<TFastValue: FastValue + Display>(
    values: &[TFastValue],
    buffer: &mut Vec<u8>,
) -> io::Result<()> {
    buffer.clear();
    buffer.reserve_exact(std::mem::size_of::<TFastValue>() * values.len());
    for value in values {
        buffer.extend(value.as_u64().to_le_bytes());
    }
    Ok(())
}
