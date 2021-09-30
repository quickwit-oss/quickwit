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

pub use collector::FastFieldCollector;
pub use leaf::leaf_search_stream;
use quickwit_proto::OutputFormat;
pub use root::root_search_stream;
use tantivy::fastfield::FastValue;

use self::collector::PartitionValues;

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
    }
}

pub fn serialize_partitions<TFastValue: FastValue + Display, TPartitionFastValue: FastValue>(
    p_values: &[PartitionValues<TFastValue, TPartitionFastValue>],
    buffer: &mut Vec<u8>,
) -> io::Result<()> {
    let buf_size = helpers::partitions_size_in_bytes(p_values);
    buffer.clear();
    buffer.reserve_exact(buf_size);
    for partition in p_values {
        let values_byte_size =
            std::mem::size_of::<TFastValue>() * partition.fast_field_values.len();

        buffer.extend(partition.partition_value.as_u64().to_le_bytes());
        buffer.extend(values_byte_size.to_le_bytes());

        for value in &partition.fast_field_values {
            buffer.extend(value.as_u64().to_le_bytes());
        }
    }
    Ok(())
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

mod helpers {
    use std::fmt::Display;

    use tantivy::fastfield::FastValue;

    use super::collector::PartitionValues;

    #[inline(always)]
    pub fn partitions_size_in_bytes<
        TFastValue: FastValue + Display,
        TPartitionFastValue: FastValue,
    >(
        partitions: &[PartitionValues<TFastValue, TPartitionFastValue>],
    ) -> usize {
        let mut size = 0;
        for partition in partitions {
            size += partition_size_in_bytes(partition);
        }
        size
    }

    #[inline(always)]
    fn partition_size_in_bytes<TFastValue: FastValue + Display, TPartitionFastValue: FastValue>(
        partition: &PartitionValues<TFastValue, TPartitionFastValue>,
    ) -> usize {
        std::mem::size_of::<TFastValue>() * partition.fast_field_values.len()
            + std::mem::size_of::<u64>()
            + std::mem::size_of::<TPartitionFastValue>()
    }
}

#[cfg(test)]
mod tests {
    use crate::search_stream::collector::PartitionValues;
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

    #[test]
    fn test_serialize_partitions() {
        let mut buffer = Vec::new();
        let partition_1 = PartitionValues {
            partition_value: 1u64,
            fast_field_values: vec![3u64, 4u64],
        };
        let partition_2 = PartitionValues {
            partition_value: 2u64,
            fast_field_values: vec![5u64],
        };
        super::serialize_partitions::<u64, u64>(&[partition_1, partition_2], &mut buffer).unwrap();
        let expected_buffer: Vec<u8> = vec![
            1u64.to_le_bytes(),
            16usize.to_le_bytes(),
            3u64.to_le_bytes(),
            4u64.to_le_bytes(),
            2u64.to_le_bytes(),
            8usize.to_le_bytes(),
            5u64.to_le_bytes(),
        ]
        .into_iter()
        .flatten()
        .collect();
        assert_eq!(buffer, expected_buffer);
    }
}
