//! This library defines a `log`.
//!
//! This log is strongly inspired by leveldb and rocksdb's implementation.
//!
//! The log is a sequence of blocks of `2^15 = 32_768 bytes`.
//! Even when resuming writing a log after a failure, the alignment of
//! blocks is guaranteed by the Writer.
//!
//! Record's payload can be of any size (including 0). They may span over
//! several blocks.
//!
//! The integrity of the log is protected by a checksum at the block
//! level. In case of corruption, some punctual record can be lost, while
//! later records are ok.
//!
//! # Usage
//!
mod frame;
mod mem;
mod multi_record_log;
mod record;
mod rolling;

#[cfg(test)]
mod tests;

pub use multi_record_log::MultiRecordLog;

use std::convert::TryInto;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum Record<'a> {
    AddRecord {
        position: u64,
        queue: &'a str,
        payload: &'a [u8],
    },
    Truncate {
        position: u64,
        queue: &'a str,
    },
}

impl<'a> Record<'a> {
    pub fn position(&self) -> u64 {
        match self {
            Record::AddRecord { position, .. } => *position,
            Record::Truncate { position, .. } => *position,
        }
    }
}

pub trait Serializable<'a>: Sized {
    /// Clears the buffer first.
    fn serialize(&self, buffer: &mut Vec<u8>);
    fn deserialize(buffer: &'a [u8]) -> Option<Self>;
}

impl<'a> Serializable<'a> for Record<'a> {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        buffer.clear();
        match *self {
            Record::AddRecord {
                position,
                queue,
                payload,
            } => {
                buffer.push(0u8);
                buffer.extend_from_slice(&position.to_le_bytes());
                buffer.extend_from_slice(&(queue.len() as u16).to_le_bytes());
                buffer.extend(queue.as_bytes());
                buffer.extend(payload);
            }
            Record::Truncate { queue, position } => {
                buffer.push(1u8);
                buffer.extend(&position.to_le_bytes());
                buffer.extend_from_slice(&(queue.len() as u16).to_le_bytes());
                buffer.extend(queue.as_bytes());
            }
        }
    }

    fn deserialize(buffer: &'a [u8]) -> Option<Record<'a>> {
        if buffer.len() < 8 {
            return None;
        }
        let enum_tag = buffer[0];
        let position = u64::from_le_bytes(buffer[1..9].try_into().unwrap());
        let queue_id_len = u16::from_le_bytes(buffer[9..11].try_into().unwrap()) as usize;
        let queue_id = std::str::from_utf8(&buffer[11..][..queue_id_len]).ok()?;
        match enum_tag {
            0u8 => {
                let payload = &buffer[11 + queue_id_len..];
                Some(Record::AddRecord {
                    position,
                    queue: queue_id,
                    payload,
                })
            }
            1u8 => Some(Record::Truncate {
                position,
                queue: queue_id,
            }),
            _ => None,
        }
    }
}

impl<'a> Serializable<'a> for &'a str {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        buffer.clear();
        buffer.extend_from_slice(self.as_bytes())
    }

    fn deserialize(buffer: &'a [u8]) -> Option<Self> {
        std::str::from_utf8(buffer).ok()
    }
}

impl<'a> Serializable<'a> for &'a [u8] {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        buffer.clear();
        buffer.extend_from_slice(self);
    }

    fn deserialize(buffer: &'a [u8]) -> Option<Self> {
        Some(buffer)
    }
}
