// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod collector;
mod leaf;
mod root;

use std::fmt::Display;
use std::io;
use std::io::Write;

pub use collector::FastFieldCollector;
pub use leaf::leaf_search_stream;
use quickwit_proto::search::OutputFormat;
pub use root::root_search_stream;
use tantivy::columnar::MonotonicallyMappableToU64;

use self::collector::PartitionValues;

pub trait ToLittleEndian {
    fn to_le_bytes(&self) -> [u8; 8];
}

impl ToLittleEndian for u64 {
    fn to_le_bytes(&self) -> [u8; 8] {
        u64::to_le_bytes(*self)
    }
}

impl ToLittleEndian for i64 {
    fn to_le_bytes(&self) -> [u8; 8] {
        i64::to_le_bytes(*self)
    }
}

impl ToLittleEndian for f64 {
    fn to_le_bytes(&self) -> [u8; 8] {
        f64::to_le_bytes(*self)
    }
}

/// Serialize the values into the `buffer` as bytes.
///
/// Please note that the `buffer` is always cleared.
pub fn serialize<T: ToLittleEndian + Display>(
    values: &[T],
    buffer: &mut Vec<u8>,
    format: OutputFormat,
) -> io::Result<()> {
    match format {
        OutputFormat::Csv => serialize_csv(values, buffer),
        OutputFormat::ClickHouseRowBinary => serialize_click_house_row_binary(values, buffer),
    }
}

pub fn serialize_partitions<
    TFastValue: MonotonicallyMappableToU64,
    TPartitionFastValue: MonotonicallyMappableToU64,
>(
    p_values: &[PartitionValues<TFastValue, TPartitionFastValue>],
    buffer: &mut Vec<u8>,
) -> io::Result<()> {
    let buf_size = helpers::partitions_size_in_bytes(p_values);
    buffer.clear();
    buffer.reserve_exact(buf_size);
    for partition in p_values {
        let values_byte_size = std::mem::size_of::<u64>() * partition.fast_field_values.len();

        buffer.extend(partition.partition_value.to_u64().to_le_bytes());
        buffer.extend(values_byte_size.to_le_bytes());

        for value in &partition.fast_field_values {
            buffer.extend(value.to_u64().to_le_bytes());
        }
    }
    Ok(())
}

fn serialize_csv<T: Display>(values: &[T], buffer: &mut Vec<u8>) -> io::Result<()> {
    buffer.clear();
    for value in values {
        writeln!(buffer, "{value}")?;
    }
    Ok(())
}

fn serialize_click_house_row_binary<T: ToLittleEndian>(
    values: &[T],
    buffer: &mut Vec<u8>,
) -> io::Result<()> {
    buffer.clear();
    buffer.reserve_exact(8 * values.len());
    for value in values {
        buffer.extend(value.to_le_bytes());
    }
    Ok(())
}

mod helpers {
    use super::collector::PartitionValues;

    #[inline(always)]
    pub fn partitions_size_in_bytes<TFastValue, TPartitionFastValue>(
        partitions: &[PartitionValues<TFastValue, TPartitionFastValue>],
    ) -> usize {
        let mut size = 0;
        for partition in partitions {
            size += partition_size_in_bytes(partition);
        }
        size
    }

    #[inline(always)]
    fn partition_size_in_bytes<TFastValue, TPartitionFastValue>(
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
