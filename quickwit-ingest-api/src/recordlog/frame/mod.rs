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

mod header;
mod reader;
mod writer;

use self::header::Header;
pub(crate) use self::header::{FrameType, HEADER_LEN};
pub(crate) use self::reader::{FrameReader, ReadFrameError};
pub(crate) use self::writer::FrameWriter;
pub(crate) const BLOCK_LEN: usize = 32_768;

#[cfg(test)]
mod tests {
    use std::io;

    use super::{FrameReader, FrameType, FrameWriter, ReadFrameError, BLOCK_LEN, HEADER_LEN};

    #[tokio::test]
    async fn test_frame_simple() -> io::Result<()> {
        let mut wrt: Vec<u8> = Vec::new();
        {
            let mut frame_writer = FrameWriter::create_with_aligned_write(&mut wrt);
            frame_writer
                .write_frame(FrameType::FIRST, &b"abc"[..])
                .await?;
            frame_writer
                .write_frame(FrameType::MIDDLE, &b"de"[..])
                .await?;
            frame_writer
                .write_frame(FrameType::LAST, &b"fgh"[..])
                .await?;
            frame_writer.flush().await?;
        }
        let mut frame_reader = FrameReader::open(&wrt[..]);
        assert!(matches!(
            frame_reader.read_frame().await,
            Ok((FrameType::FIRST, b"abc"))
        ));
        assert!(matches!(
            frame_reader.read_frame().await,
            Ok((FrameType::MIDDLE, b"de"))
        ));
        assert!(matches!(
            frame_reader.read_frame().await,
            Ok((FrameType::LAST, b"fgh"))
        ));
        assert!(matches!(
            frame_reader.read_frame().await,
            Err(ReadFrameError::NotAvailable)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_frame_partial() -> io::Result<()> {
        let mut wrt: Vec<u8> = Vec::new();
        {
            let mut frame_writer = FrameWriter::create_with_aligned_write(&mut wrt);
            frame_writer
                .write_frame(FrameType::FIRST, &b"abc"[..])
                .await?;
            frame_writer.flush().await?;
        }
        assert_eq!(wrt.len(), HEADER_LEN + 3);
        wrt.truncate(HEADER_LEN + 2);
        let mut frame_reader = FrameReader::open(&wrt[..]);
        assert!(matches!(
            frame_reader.read_frame().await,
            Err(ReadFrameError::NotAvailable)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_frame_corruption_in_payload() -> io::Result<()> {
        let mut wrt: Vec<u8> = Vec::new();
        {
            let mut frame_writer = FrameWriter::create_with_aligned_write(&mut wrt);
            frame_writer
                .write_frame(FrameType::FIRST, &b"abc"[..])
                .await?;
            frame_writer.flush().await?;
        }
        {
            let mut frame_writer = FrameWriter::create_with_aligned_write(&mut wrt);
            frame_writer
                .write_frame(FrameType::MIDDLE, &b"de"[..])
                .await?;
            frame_writer.flush().await?;
        }
        wrt[8] = 0u8;
        let mut frame_reader = FrameReader::open(&wrt[..]);
        assert!(matches!(
            frame_reader.read_frame().await,
            Err(ReadFrameError::Corruption)
        ));
        assert!(matches!(
            frame_reader.read_frame().await,
            Ok((FrameType::MIDDLE, b"de"))
        ));
        Ok(())
    }

    async fn repeat_empty_frame_util(repeat: usize) -> Vec<u8> {
        let mut wrt: Vec<u8> = Vec::new();
        {
            let mut frame_writer = FrameWriter::create_with_aligned_write(&mut wrt);
            for _ in 0..repeat {
                frame_writer
                    .write_frame(FrameType::FULL, &b""[..])
                    .await
                    .unwrap();
            }
            frame_writer.flush().await.unwrap();
        }
        wrt
    }

    #[tokio::test]
    async fn test_simple_multiple_blocks() -> io::Result<()> {
        let num_frames = 1 + BLOCK_LEN / HEADER_LEN;
        let buffer = repeat_empty_frame_util(num_frames).await;
        assert_eq!(buffer.len(), BLOCK_LEN + HEADER_LEN);
        let mut frame_reader = FrameReader::open(&buffer[..]);
        for _ in 0..num_frames {
            let read_frame_res = frame_reader.read_frame().await;
            assert!(matches!(read_frame_res, Ok((FrameType::FULL, &[]))));
        }
        assert!(matches!(
            frame_reader.read_frame().await,
            Err(ReadFrameError::NotAvailable)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_blocks_corruption_on_length() -> io::Result<()> {
        // We end up with 4681 frames on the first block.
        // 1 frame on the second block
        let num_frames = 1 + BLOCK_LEN / HEADER_LEN;
        let mut buffer = repeat_empty_frame_util(num_frames).await;
        buffer[2000 * HEADER_LEN + 5] = 255u8;
        assert_eq!(buffer.len(), BLOCK_LEN + HEADER_LEN);
        let mut frame_reader = FrameReader::open(&buffer[..]);
        for _ in 0..2000 {
            let read_frame_res = frame_reader.read_frame().await;
            assert!(matches!(read_frame_res, Ok((FrameType::FULL, &[]))));
        }
        assert!(matches!(
            frame_reader.read_frame().await,
            Err(ReadFrameError::Corruption)
        ));
        assert!(matches!(
            frame_reader.read_frame().await,
            Ok((FrameType::FULL, &[]))
        ));
        assert!(matches!(
            frame_reader.read_frame().await,
            Err(ReadFrameError::NotAvailable)
        ));
        Ok(())
    }
}
