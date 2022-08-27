use std::io;
use std::ops::Range;

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};

use super::{FrameType, Header, BLOCK_LEN, HEADER_LEN};

const NUM_BLOCKS_BUFFERED: usize = 10;
const BUFFER_LEN: usize = NUM_BLOCKS_BUFFERED * BLOCK_LEN;

pub struct FrameReader<R> {
    reader: R,

    /// At any point our buffer is split into
    /// | consumed_bytes | available_bytes | free bytes |
    buffer: Box<[u8; BUFFER_LEN]>,
    /// Range of the available bytes.
    available: Range<usize>,
    // The current block is corrupted.
    block_corrupted: bool,
}

#[derive(Error, Debug)]
pub(crate) enum ReadFrameError {
    #[error("Io error: {0}")]
    IoError(#[from] io::Error),
    #[error("Corruption in frame")]
    Corruption,
    #[error("Next frame not available")]
    NotAvailable,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    pub fn open(reader: R) -> Self {
        FrameReader {
            reader,
            buffer: Box::new([0u8; BUFFER_LEN]),
            available: 0..0,
            block_corrupted: false,
        }
    }

    /// Number of bytes available to read in our buffer.
    fn available_len(&self) -> usize {
        self.available.len()
    }

    /// Amount of bytes that are still available to fill
    /// in the buffer.
    fn free_len(&self) -> usize {
        BUFFER_LEN - self.available.end
    }

    /// Rotating the buffer move the available bytes
    /// to the beginning of the buffer:
    ///
    /// | ... consumed_bytes...  |   ... available_bytes ...  | ... free bytes ... |
    /// Becomes:
    /// |   ... available_bytes ...  | ... free bytes ...                          |
    fn rotate_buffer(&mut self) {
        assert!(self.free_len() < BLOCK_LEN);
        assert!(self.available_len() < BLOCK_LEN);
        let num_bytes_available = self.available_len();
        let (free_area, unread_data) = self.buffer.split_at_mut(self.available.start);
        // We don't just stet new_available_start to 0, because we want to keep block_alignment
        // within our buffer.
        let new_available_start = self.available.start % BLOCK_LEN;
        let new_available = new_available_start..new_available_start + num_bytes_available;
        free_area[new_available.clone()].copy_from_slice(&unread_data[..num_bytes_available]);
        self.available = new_available;
    }

    /// Attempts to read data.
    ///
    /// Attempts to fill the internal `buffer`,
    /// which is why it does not take a number of bytes to
    /// attempt to read.
    async fn read_slab(&mut self) -> io::Result<usize> {
        assert!(self.available_len() < BLOCK_LEN);
        if self.free_len() < BLOCK_LEN {
            // We are reaching the saturation of our buffer.
            // Let's "rotate" our buffer to make some room.
            self.rotate_buffer();
        }
        assert!(self.free_len() >= BLOCK_LEN);
        assert!(self.buffer[self.available.end..].len() >= BLOCK_LEN);
        let num_read_bytes: usize = self
            .reader
            .read(&mut self.buffer[self.available.end..])
            .await?;
        self.available.end += num_read_bytes;
        Ok(num_read_bytes)
    }

    // Returns the number of bytes remaining into
    // the current block.
    //
    // These bytes may or may not be available.
    fn num_bytes_to_end_of_block(&self) -> usize {
        BLOCK_LEN - (self.available.start % BLOCK_LEN)
    }

    async fn go_to_next_block_if_necessary(&mut self) -> Result<(), ReadFrameError> {
        let num_bytes_to_end_of_block = self.num_bytes_to_end_of_block();
        let need_to_skip_block = self.block_corrupted || num_bytes_to_end_of_block < HEADER_LEN;
        if !need_to_skip_block {
            return Ok(());
        }
        self.ensure_bytes_available(num_bytes_to_end_of_block)
            .await?;
        self.advance(num_bytes_to_end_of_block);
        self.block_corrupted = false;
        Ok(())
    }

    async fn ensure_bytes_available(&mut self, required_len: usize) -> Result<(), ReadFrameError> {
        if self.available_len() >= required_len {
            return Ok(());
        }
        while self.read_slab().await? > 0 {
            if self.available_len() >= required_len {
                return Ok(());
            }
        }
        Err(ReadFrameError::NotAvailable)
    }

    fn advance(&mut self, num_bytes: usize) {
        self.available.start += num_bytes;
    }

    // Attempt to read the header of the next frame
    // This method does not consume any bytes (which is why it is called get and not read).
    async fn get_frame_header(&mut self) -> Result<Header, ReadFrameError> {
        self.ensure_bytes_available(HEADER_LEN).await?;
        let header_bytes = &self.buffer[self.available.clone()][..HEADER_LEN];
        match Header::deserialize(&header_bytes) {
            Some(header) => Ok(header),
            None => {
                self.block_corrupted = true;
                Err(ReadFrameError::Corruption)
            }
        }
    }

    // Reads the next frame.
    pub(crate) async fn read_frame(&mut self) -> Result<(FrameType, &[u8]), ReadFrameError> {
        self.go_to_next_block_if_necessary().await?;
        let header = self.get_frame_header().await?;
        let frame_num_bytes = header.len() + HEADER_LEN;
        if self.num_bytes_to_end_of_block() < frame_num_bytes {
            // The number of bytes for this frame would span over
            // the next block.
            // This is a corruption for which we need to drop the entire block.
            self.block_corrupted = true;
            return Err(ReadFrameError::Corruption);
        }
        self.ensure_bytes_available(frame_num_bytes).await?;
        let frame_payload_range =
            (self.available.start + HEADER_LEN)..(self.available.start + frame_num_bytes);
        self.advance(frame_num_bytes);
        let frame_payload = &self.buffer[frame_payload_range];
        if !header.check(frame_payload) {
            // The CRC check is wrong.
            // We do not necessarily need to corrupt the block.
            //
            // With a little luck, a single frame payload byte was corrupted
            // but the frame length was correct.
            return Err(ReadFrameError::Corruption);
        }
        Ok((header.frame_type(), frame_payload))
    }
}
