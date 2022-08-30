// Copyright (C) 2022 Quickwit, Inc.
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

use crc32fast;

use super::BLOCK_LEN;

pub const HEADER_LEN: usize = 4 + 2 + 1;

fn crc32(data: &[u8]) -> u32 {
    let mut hash = crc32fast::Hasher::default();
    hash.update(data);
    hash.finalize()
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Header {
    checksum: u32,
    len: u16,
    frame_type: FrameType,
}

impl Header {
    pub fn for_payload(frame_type: FrameType, payload: &[u8]) -> Header {
        assert!(payload.len() < BLOCK_LEN);
        Header {
            checksum: crc32(payload),
            len: payload.len() as u16,
            frame_type,
        }
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn frame_type(&self) -> FrameType {
        self.frame_type
    }

    pub fn check(&self, payload: &[u8]) -> bool {
        crc32(payload) == self.checksum
    }

    pub fn serialize(&self, dest: &mut [u8]) {
        assert_eq!(dest.len(), HEADER_LEN);
        dest[..4].copy_from_slice(&self.checksum.to_le_bytes()[..]);
        dest[4..6].copy_from_slice(&self.len.to_le_bytes()[..]);
        dest[6] = self.frame_type.to_u8();
    }

    pub fn deserialize(data: &[u8]) -> Option<Header> {
        assert_eq!(data.len(), HEADER_LEN);
        let checksum = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let len = u16::from_le_bytes([data[4], data[5]]);
        let frame_type = FrameType::from_u8(data[6])?;
        Some(Header {
            checksum,
            len,
            frame_type,
        })
    }
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum FrameType {
    FULL = 1u8,
    FIRST = 2u8,
    MIDDLE = 3u8,
    LAST = 4u8,
}

impl FrameType {
    fn from_u8(b: u8) -> Option<FrameType> {
        match b {
            1u8 => Some(FrameType::FULL),
            2u8 => Some(FrameType::FIRST),
            3u8 => Some(FrameType::MIDDLE),
            4u8 => Some(FrameType::LAST),
            _ => None,
        }
    }

    fn to_u8(self) -> u8 {
        self as u8
    }

    pub fn is_first_frame_of_record(&self) -> bool {
        match self {
            FrameType::FULL | FrameType::FIRST => true,
            FrameType::LAST | FrameType::MIDDLE => false,
        }
    }

    pub fn is_last_frame_of_record(&self) -> bool {
        match self {
            FrameType::FULL | FrameType::LAST => true,
            FrameType::FIRST | FrameType::MIDDLE => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FrameType, Header, HEADER_LEN};

    #[test]
    fn test_frame_type_serialize_deserialize() {
        const ALL_FRAME_TYPES: [FrameType; 4] = [
            FrameType::FULL,
            FrameType::FIRST,
            FrameType::MIDDLE,
            FrameType::LAST,
        ];
        for frame_type in ALL_FRAME_TYPES {
            assert_eq!(FrameType::from_u8(frame_type.to_u8()), Some(frame_type));
        }
    }

    #[test]
    fn test_frame_deserialize_invalid() {
        assert_eq!(FrameType::from_u8(14u8), None);
    }

    #[test]
    fn test_header_serialize_deserialize() {
        let header = Header {
            checksum: 17u32,
            len: 42,
            frame_type: FrameType::FULL,
        };
        let mut buffer = [0u8; HEADER_LEN];
        header.serialize(&mut buffer);
        let serdeser_header = Header::deserialize(&buffer).unwrap();
        assert_eq!(header, serdeser_header);
    }

    #[test]
    fn test_header_deserialize_invalid() {
        let invalid_header_buffer = [14u8; HEADER_LEN];
        assert_eq!(Header::deserialize(&invalid_header_buffer), None);
    }
}
