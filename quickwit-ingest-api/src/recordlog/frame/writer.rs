use std::io;

use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};

use super::{FrameType, Header, BLOCK_LEN, HEADER_LEN};

pub(crate) struct FrameWriter<W> {
    wrt: BufWriter<W>,
    buffer: Box<[u8; BLOCK_LEN]>,
    current_block_len: usize,
    num_bytes_written: u64,
}

impl<W: AsyncWrite + Unpin> FrameWriter<W> {
    pub(crate) fn create_with_aligned_write(wrt: W) -> Self {
        FrameWriter {
            wrt: BufWriter::new(wrt),
            buffer: Box::new([0u8; BLOCK_LEN]),
            current_block_len: 0,
            num_bytes_written: 0u64,
        }
    }

    pub fn num_bytes_written(&self) -> u64 {
        self.num_bytes_written
    }

    pub async fn write_frame(&mut self, frame_type: FrameType, payload: &[u8]) -> io::Result<()> {
        if self.available_num_bytes_in_block() < HEADER_LEN {
            self.pad_block().await?;
        }
        assert!(payload.len() <= self.max_writable_frame_length());
        let record_len = HEADER_LEN + payload.len();
        assert!(record_len <= BLOCK_LEN);
        let (buffer_header, buffer_record) = self.buffer[..record_len].split_at_mut(HEADER_LEN);
        buffer_record.copy_from_slice(payload);
        Header::for_payload(frame_type, &payload).serialize(buffer_header);
        self.current_block_len = (self.current_block_len + record_len) % BLOCK_LEN;
        self.wrt.write_all(&self.buffer[..record_len]).await?;
        self.num_bytes_written += record_len as u64;
        Ok(())
    }

    /// Flush the buffered writer used in the FrameWriter.
    ///
    /// When writing to a file, this performs a syscall and
    /// the OS will be in charge of eventually writing the data
    /// to disk, but this is not sufficient to ensure durability.
    pub async fn flush(&mut self) -> io::Result<()> {
        self.wrt.flush().await
    }

    async fn pad_block(&mut self) -> io::Result<()> {
        let remaining_num_bytes_in_block = self.available_num_bytes_in_block();
        let b = vec![0u8; remaining_num_bytes_in_block];
        self.wrt.write_all(&b).await?;
        self.num_bytes_written += b.len() as u64;
        Ok(())
    }

    fn available_num_bytes_in_block(&self) -> usize {
        BLOCK_LEN - self.current_block_len
    }

    /// Returns he
    pub fn max_writable_frame_length(&self) -> usize {
        let available_num_bytes_in_block = self.available_num_bytes_in_block();
        if available_num_bytes_in_block >= HEADER_LEN {
            available_num_bytes_in_block - HEADER_LEN
        } else {
            // That block is finished. We will have to pad it.
            BLOCK_LEN - HEADER_LEN
        }
    }

    pub fn get_underlying_wrt(&mut self) -> &mut W {
        self.wrt.get_mut()
    }
}
