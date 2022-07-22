use std::array::TryFromSliceError;
use std::time::Duration;

pub type FileKey = u64;

#[derive(Debug)]
pub struct Corrupted;

impl From<TryFromSliceError> for Corrupted {
    fn from(_: TryFromSliceError) -> Self {
        Self
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) struct FileEntry {
    pub(crate) key: FileKey,
    pub(crate) file_checksum: u32,
    pub(crate) last_accessed: u64,
    pub(crate) file_size: u64,
}

impl FileEntry {
    pub(crate) const EMPTY: Self = Self {
        key: 0,
        file_checksum: 0,
        last_accessed: 0,
        file_size: 0,
    };
    pub(crate) const RAW_SIZE: usize = std::mem::size_of::<FileKey>()
        + std::mem::size_of::<u32>()
        + std::mem::size_of::<u64>()
        + std::mem::size_of::<u64>()
        + std::mem::size_of::<u32>(); // Row checksum

    pub(crate) fn new(
        key: FileKey,
        file_checksum: u32,
        last_accessed: Duration,
        file_size: u64,
    ) -> Self {
        Self {
            key,
            file_checksum,
            last_accessed: last_accessed.as_secs(),
            file_size,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self == &Self::EMPTY
    }

    pub(crate) fn to_bytes(self) -> [u8; Self::RAW_SIZE] {
        let mut buff = [0; Self::RAW_SIZE];

        buff[4..12].copy_from_slice(&self.key.to_be_bytes());
        buff[12..16].copy_from_slice(&self.file_checksum.to_be_bytes());
        buff[16..24].copy_from_slice(&self.last_accessed.to_be_bytes());
        buff[24..32].copy_from_slice(&self.file_size.to_be_bytes());

        let row_checksum = crc32fast::hash(&buff[4..]);
        buff[0..4].copy_from_slice(&row_checksum.to_be_bytes());

        buff
    }

    pub(crate) fn from_bytes(mut buff: &[u8]) -> Result<Self, Corrupted> {
        let row_checksum = read_be_u32(&mut buff)?;

        // Validate the row checksums are correct.
        if row_checksum != crc32fast::hash(buff) {
            return Err(Corrupted);
        }

        let key = read_be_u64(&mut buff)?;
        let checksum = read_be_u32(&mut buff)?;
        let last_accessed = read_be_u64(&mut buff)?;
        let file_size = read_be_u64(&mut buff)?;

        Ok(Self {
            key,
            file_checksum: checksum,
            last_accessed,
            file_size,
        })
    }
}

fn read_be_u64(input: &mut &[u8]) -> Result<u64, TryFromSliceError> {
    let (int_bytes, rest) = input.split_at(std::mem::size_of::<u64>());
    *input = rest;

    let converted = int_bytes.try_into()?;

    Ok(u64::from_be_bytes(converted))
}

fn read_be_u32(input: &mut &[u8]) -> Result<u32, TryFromSliceError> {
    let (int_bytes, rest) = input.split_at(std::mem::size_of::<u32>());
    *input = rest;

    let converted = int_bytes.try_into()?;

    Ok(u32::from_be_bytes(converted))
}
