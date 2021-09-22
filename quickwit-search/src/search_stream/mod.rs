// Copyright (C) 2021 Quickwit, Inc.
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

mod collector;
mod leaf;
mod root;

use std::fmt::Display;
use std::io;
use std::io::Write;

pub use collector::{FastFieldCollector, FastFieldCollectorBuilder};
pub use leaf::leaf_search_stream;
use quickwit_proto::OutputFormat;
pub use root::root_search_stream;
use tantivy::fastfield::FastValue;

/// Serialize the values into the `buffer` as bytes.
///
/// Please note that the `buffer` is always cleared.
pub fn serialize<TFastValue: FastValue + Display>(
    values: &[TFastValue],
    buffer: &mut Vec<u8>,
    format: OutputFormat,
) -> io::Result<()> {
    match format {
        OutputFormat::Csv => serialize_csv(values, buffer),
        OutputFormat::ClickHouseRowBinary => serialize_click_house_row_binary(values, buffer),
        OutputFormat::PartitionnedClickhouseRowBinary => panic!("Not covered yes")
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

fn serialize_click_house_row_binary<TFastValue: FastValue + Display>(
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

#[cfg(test)]
mod tests {
    use crate::search_stream::{serialize_click_house_row_binary, serialize_csv};

    #[test]
    fn test_serialize_row_binary() {
        let mut buffer = Vec::new();
        serialize_click_house_row_binary::<i64>(&[-10i64], &mut buffer).unwrap();
        assert_eq!(buffer, (-10i64).to_le_bytes());

        let mut buffer = Vec::new();
        serialize_click_house_row_binary::<f64>(&[-10f64], &mut buffer).unwrap();
        assert_eq!(buffer, (-10f64).to_le_bytes());
    }

    #[test]
    fn test_serialize_csv() {
        let mut buffer = Vec::new();
        serialize_csv::<i64>(&[-10i64], &mut buffer).unwrap();
        assert_eq!(buffer, "-10\n".as_bytes());
    }
}
