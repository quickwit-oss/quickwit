/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use std::convert::TryFrom;

/// Base 2 bytes.
#[derive(Clone, Debug)]
pub enum ByteUnits {
    Bytes(u64),
    KiB(u64),
    MiB(u64),
    GiB(u64),
}

impl ByteUnits {
    pub fn bytes(&self) -> u64 {
        match *self {
            ByteUnits::Bytes(bytes) => bytes,
            ByteUnits::KiB(kib) => kib * (1 << 10),
            ByteUnits::MiB(mib) => mib * (1 << 20),
            ByteUnits::GiB(gib) => gib * (1 << 30),
        }
    }
}

/// Implementation of String to ByteUnits coversion
///
/// The following regex defines the specification:
/// ^[0-9]+_?(b|bytes|bytes|kb|kib|mb|mib|gb|gib)?$
/// Examples:
/// 2, 2b, 2byte, 2_bytes => ByteUnits::Bytes(2)
/// 2kb, 2kib => ByteUnits::KiB(2)
/// 2mb, 2mib => ByteUnits::MiB(2)
/// 2gb, 2gib => ByteUnits::GiB(2)
impl TryFrom<&str> for ByteUnits {
    type Error = std::num::ParseIntError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let number_str: String = value.chars().take_while(|c| c.is_numeric()).collect();
        let unit: String = value[number_str.len()..].trim_matches('_').to_lowercase();
        let number = number_str.parse::<u64>()?;
        Ok(match unit.as_str() {
            "b" | "byte" | "bytes" => Self::Bytes(number),
            "kb" | "kib" => Self::KiB(number),
            "mb" | "mib" => Self::MiB(number),
            "gb" | "gib" => Self::GiB(number),
            _ => Self::Bytes(number),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use super::ByteUnits;

    #[test]
    fn test_byte_units() {
        assert_eq!(ByteUnits::Bytes(1).bytes(), 1);
        assert_eq!(ByteUnits::KiB(1).bytes(), 1024);
        assert_eq!(ByteUnits::MiB(1).bytes(), 1024 * 1024);
        assert_eq!(ByteUnits::GiB(1).bytes(), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_from_string_conversion() {
        assert_eq!(ByteUnits::try_from("12").unwrap().bytes(), 12);
        assert_eq!(ByteUnits::try_from("12b").unwrap().bytes(), 12);
        assert_eq!(ByteUnits::try_from("30_byte").unwrap().bytes(), 30);
        assert_eq!(ByteUnits::try_from("20bytes").unwrap().bytes(), 20);

        assert_eq!(ByteUnits::try_from("7kb").unwrap().bytes(), 7 * 1024);
        assert_eq!(ByteUnits::try_from("7_kb").unwrap().bytes(), 7 * 1024);
        assert_eq!(ByteUnits::try_from("2kib").unwrap().bytes(), 2 * 1024);
        assert_eq!(ByteUnits::try_from("2_kib").unwrap().bytes(), 2 * 1024);

        assert_eq!(ByteUnits::try_from("7mb").unwrap().bytes(), 7 * 1024 * 1024);
        assert_eq!(
            ByteUnits::try_from("7_mb").unwrap().bytes(),
            7 * 1024 * 1024
        );
        assert_eq!(
            ByteUnits::try_from("2mib").unwrap().bytes(),
            2 * 1024 * 1024
        );
        assert_eq!(
            ByteUnits::try_from("2_mib").unwrap().bytes(),
            2 * 1024 * 1024
        );

        assert_eq!(
            ByteUnits::try_from("7gb").unwrap().bytes(),
            7 * 1024 * 1024 * 1024
        );
        assert_eq!(
            ByteUnits::try_from("7_gb").unwrap().bytes(),
            7 * 1024 * 1024 * 1024
        );
        assert_eq!(
            ByteUnits::try_from("2gib").unwrap().bytes(),
            2 * 1024 * 1024 * 1024
        );
        assert_eq!(
            ByteUnits::try_from("2_gib").unwrap().bytes(),
            2 * 1024 * 1024 * 1024
        );
    }
}
