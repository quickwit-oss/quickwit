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

use std::io::{self, ErrorKind, Read};

use tantivy::schema::Type;
use tantivy::FieldMetadata;

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub struct FieldConfig {
    pub typ: Type,
    pub indexed: bool,
    pub stored: bool,
    pub fast: bool,
}

impl FieldConfig {
    fn serialize(&self) -> [u8; 2] {
        let typ = self.typ.to_code();
        let flags = (self.indexed as u8) << 2 | (self.stored as u8) << 1 | (self.fast as u8);
        [typ, flags]
    }
    fn deserialize_from(data: [u8; 2]) -> io::Result<FieldConfig> {
        let typ = Type::from_code(data[0]).ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("could not deserialize type {}", data[0]),
            )
        })?;

        let data = data[1];
        let indexed = (data & 0b100) != 0;
        let stored = (data & 0b010) != 0;
        let fast = (data & 0b001) != 0;

        Ok(FieldConfig {
            typ,
            indexed,
            stored,
            fast,
        })
    }
}

/// Serializes the Split fields.
///
/// `fields_metadata` has to be sorted.
pub fn serialize_split_fields(fields_metadata: &[FieldMetadata]) -> Vec<u8> {
    // ensure that fields_metadata is strictly sorted.
    debug_assert!(fields_metadata.windows(2).all(|w| w[0] < w[1]));
    let mut payload = Vec::new();
    // Write Num Fields
    let length = fields_metadata.len() as u32;
    payload.extend_from_slice(&length.to_le_bytes());

    for field_metadata in fields_metadata {
        write_field(field_metadata, &mut payload);
    }
    let compression_level = 3;
    let payload_compressed = zstd::stream::encode_all(&mut &payload[..], compression_level)
        .expect("zstd encoding failed");
    let mut out = Vec::new();
    // Write Header -- Format Version
    let format_version = 1u8;
    out.push(format_version);
    // Write Payload
    out.extend_from_slice(&payload_compressed);
    out
}

fn write_field(field_metadata: &FieldMetadata, out: &mut Vec<u8>) {
    let field_config = FieldConfig {
        typ: field_metadata.typ,
        indexed: field_metadata.indexed,
        stored: field_metadata.stored,
        fast: field_metadata.fast,
    };

    // Write Config 2 bytes
    out.extend_from_slice(&field_config.serialize());
    let str_length = field_metadata.field_name.len() as u16;
    // Write String length 2 bytes
    out.extend_from_slice(&str_length.to_le_bytes());
    out.extend_from_slice(field_metadata.field_name.as_bytes());
}

/// Reads a fixed number of bytes into an array and returns the array.
fn read_exact_array<R: Read, const N: usize>(reader: &mut R) -> io::Result<[u8; N]> {
    let mut buffer = [0u8; N];
    reader.read_exact(&mut buffer)?;
    Ok(buffer)
}

/// Reads the Split fields from a zstd compressed stream of bytes
pub fn read_split_fields<R: Read>(
    mut reader: R,
) -> io::Result<impl Iterator<Item = io::Result<FieldMetadata>>> {
    let format_version = read_exact_array::<_, 1>(&mut reader)?[0];
    assert_eq!(format_version, 1);
    let reader = zstd::Decoder::new(reader)?;
    read_split_fields_from_zstd(reader)
}

fn read_field<R: Read>(reader: &mut R) -> io::Result<FieldMetadata> {
    // Read FieldConfig (2 bytes)
    let config_bytes = read_exact_array::<_, 2>(reader)?;
    let field_config = FieldConfig::deserialize_from(config_bytes)?; // Assuming this returns a Result

    // Read field name length and the field name
    let name_len = u16::from_le_bytes(read_exact_array::<_, 2>(reader)?) as usize;

    let mut data = Vec::new();
    data.resize(name_len, 0);
    reader.read_exact(&mut data)?;

    let field_name = String::from_utf8(data).map_err(|err| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "Encountered invalid utf8 when deserializing field name: {}",
                err
            ),
        )
    })?;
    Ok(FieldMetadata {
        field_name,
        typ: field_config.typ,
        indexed: field_config.indexed,
        stored: field_config.stored,
        fast: field_config.fast,
    })
}

/// Reads the Split fields from a stream of bytes
fn read_split_fields_from_zstd<R: Read>(
    mut reader: R,
) -> io::Result<impl Iterator<Item = io::Result<FieldMetadata>>> {
    let mut num_fields = u32::from_le_bytes(read_exact_array::<_, 4>(&mut reader)?);

    Ok(std::iter::from_fn(move || {
        if num_fields == 0 {
            return None;
        }
        num_fields -= 1;

        Some(read_field(&mut reader))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_config_deser_test() {
        let field_config = FieldConfig {
            typ: Type::Str,
            indexed: true,
            stored: false,
            fast: true,
        };
        let serialized = field_config.serialize();
        let deserialized = FieldConfig::deserialize_from(serialized).unwrap();
        assert_eq!(field_config, deserialized);
    }
    #[test]
    fn write_read_field_test() {
        for typ in Type::iter_values() {
            let field_metadata = FieldMetadata {
                field_name: "test".to_string(),
                typ,
                indexed: true,
                stored: true,
                fast: true,
            };
            let mut out = Vec::new();
            write_field(&field_metadata, &mut out);
            let deserialized = read_field(&mut &out[..]).unwrap();
            assert_eq!(field_metadata, deserialized);
        }
        let field_metadata = FieldMetadata {
            field_name: "test".to_string(),
            typ: Type::Str,
            indexed: false,
            stored: true,
            fast: true,
        };
        let mut out = Vec::new();
        write_field(&field_metadata, &mut out);
        let deserialized = read_field(&mut &out[..]).unwrap();
        assert_eq!(field_metadata, deserialized);

        let field_metadata = FieldMetadata {
            field_name: "test".to_string(),
            typ: Type::Str,
            indexed: false,
            stored: false,
            fast: true,
        };
        let mut out = Vec::new();
        write_field(&field_metadata, &mut out);
        let deserialized = read_field(&mut &out[..]).unwrap();
        assert_eq!(field_metadata, deserialized);

        let field_metadata = FieldMetadata {
            field_name: "test".to_string(),
            typ: Type::Str,
            indexed: true,
            stored: false,
            fast: false,
        };
        let mut out = Vec::new();
        write_field(&field_metadata, &mut out);
        let deserialized = read_field(&mut &out[..]).unwrap();
        assert_eq!(field_metadata, deserialized);
    }
    #[test]
    fn write_split_fields_test() {
        let fields_metadata = vec![
            FieldMetadata {
                field_name: "test".to_string(),
                typ: Type::Str,
                indexed: true,
                stored: true,
                fast: true,
            },
            FieldMetadata {
                field_name: "test2".to_string(),
                typ: Type::Str,
                indexed: true,
                stored: false,
                fast: false,
            },
            FieldMetadata {
                field_name: "test3".to_string(),
                typ: Type::U64,
                indexed: true,
                stored: false,
                fast: true,
            },
        ];

        let out = serialize_split_fields(&fields_metadata);

        let deserialized: Vec<FieldMetadata> = read_split_fields(&mut &out[..])
            .unwrap()
            .map(|el| el.unwrap())
            .collect();

        assert_eq!(fields_metadata, deserialized);
    }
}
