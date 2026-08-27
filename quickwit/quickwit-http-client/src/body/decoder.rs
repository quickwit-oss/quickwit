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

use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::Decoder;

use crate::error::HttpError;

pub(super) const MAX_CHUNK_SIZE_LINE_SIZE: usize = 8 * 1024;
pub(super) const MAX_TRAILER_SECTION_SIZE: usize = 64 * 1024;

#[derive(Debug)]
enum ChunkedState {
    /// Reading the next chunk-size line.
    Size,
    /// Reading chunk data; `remaining` bytes left in the current chunk.
    Data { remaining: usize },
    /// Expecting the CRLF after a chunk's data.
    AfterCrlf,
    /// Reading the trailer section (after the `0`-sized chunk).
    Trailers,
}

#[derive(Debug)]
enum State {
    /// Actively decoding chunks.
    Chunked(ChunkedState),
    /// The terminal `0` chunk's trailers have been fully consumed.
    Done,
}

pub(super) enum DecodedItem {
    Data { bytes: Bytes, end_stream: bool },
    End,
}

#[derive(Debug)]
pub(super) struct HttpBodyDecoder {
    state: State,
    target: usize,
    chunk_buf: BytesMut,
    chunk_decoded: usize,
    trailer_bytes: usize,
}

impl HttpBodyDecoder {
    /// Creates a decoder for a `Transfer-Encoding: chunked` body. `target`
    /// is the frame coalescing size from [`super::BufferHint`].
    pub(super) fn new(target: usize) -> Self {
        Self {
            state: State::Chunked(ChunkedState::Size),
            target,
            chunk_buf: BytesMut::new(),
            chunk_decoded: 0,
            trailer_bytes: 0,
        }
    }

    pub(super) fn initial_capacity(&self) -> usize {
        match self.state {
            State::Chunked(_) => MAX_CHUNK_SIZE_LINE_SIZE,
            State::Done => 1,
        }
    }

    fn take_chunk_frame(&mut self, end_stream: bool) -> DecodedItem {
        let bytes = self.chunk_buf.split().freeze();
        self.chunk_decoded += bytes.len();
        DecodedItem::Data { bytes, end_stream }
    }

    fn decode_chunked(&mut self, src: &mut BytesMut) -> Result<Option<DecodedItem>, HttpError> {
        loop {
            match &mut self.state {
                State::Chunked(ChunkedState::Size) => match httparse::parse_chunk_size(src) {
                    Ok(httparse::Status::Complete((line_len, chunk_size))) => {
                        if line_len > MAX_CHUNK_SIZE_LINE_SIZE {
                            return Err(chunk_size_line_too_large());
                        }
                        let chunk_size = usize::try_from(chunk_size).map_err(|_| {
                            HttpError::InvalidLength("chunk size does not fit in usize".to_string())
                        })?;
                        src.advance(line_len);
                        self.state = if chunk_size == 0 {
                            State::Chunked(ChunkedState::Trailers)
                        } else {
                            State::Chunked(ChunkedState::Data {
                                remaining: chunk_size,
                            })
                        };
                    }
                    Ok(httparse::Status::Partial) => {
                        if src.len() > MAX_CHUNK_SIZE_LINE_SIZE {
                            return Err(chunk_size_line_too_large());
                        }
                        return Ok(None);
                    }
                    Err(error) => {
                        return Err(HttpError::InvalidLength(format!(
                            "invalid chunk size: {error}"
                        )));
                    }
                },
                State::Chunked(ChunkedState::Data { remaining }) => {
                    let output_space = self.target - self.chunk_buf.len();
                    let take = src.len().min(*remaining).min(output_space);
                    // TODO we could try to avoid this copy, it's not on the main path though
                    self.chunk_buf.extend_from_slice(&src[..take]);
                    src.advance(take);
                    *remaining -= take;

                    if self.chunk_buf.len() == self.target {
                        return Ok(Some(self.take_chunk_frame(false)));
                    }
                    if *remaining == 0 {
                        self.state = State::Chunked(ChunkedState::AfterCrlf);
                        continue;
                    }
                    return Ok(None);
                }
                State::Chunked(ChunkedState::AfterCrlf) => {
                    if src.len() < 2 {
                        return Ok(None);
                    }
                    if &src[..2] != b"\r\n" {
                        return Err(HttpError::InvalidLength(format!(
                            "expected CRLF after chunk, got {:?}",
                            &src[..2]
                        )));
                    }
                    src.advance(2);
                    self.state = State::Chunked(ChunkedState::Size);
                }
                State::Chunked(ChunkedState::Trailers) => {
                    let Some(line_len) = find_crlf(src) else {
                        if src.len() > MAX_TRAILER_SECTION_SIZE - self.trailer_bytes {
                            return Err(trailer_section_too_large());
                        }
                        return Ok(None);
                    };
                    let encoded_line_len = line_len + 2;
                    if encoded_line_len > MAX_TRAILER_SECTION_SIZE - self.trailer_bytes {
                        return Err(trailer_section_too_large());
                    }
                    self.trailer_bytes += encoded_line_len;
                    src.advance(encoded_line_len);
                    if line_len != 0 {
                        continue;
                    }
                    self.state = State::Done;
                    return if self.chunk_buf.is_empty() {
                        Ok(Some(DecodedItem::End))
                    } else {
                        Ok(Some(self.take_chunk_frame(true)))
                    };
                }
                State::Done => return Ok(Some(DecodedItem::End)),
            }
        }
    }

    fn unexpected_chunked_eof(&self) -> HttpError {
        let buffered = self.chunk_decoded + self.chunk_buf.len();
        let expected = match &self.state {
            State::Chunked(ChunkedState::Data { remaining }) => buffered + remaining,
            _ => 0,
        };
        HttpError::UnexpectedEof {
            read: buffered,
            expected,
        }
    }
}

impl Decoder for HttpBodyDecoder {
    type Item = DecodedItem;
    type Error = HttpError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.decode_chunked(src)
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(item) = self.decode(src)? {
            return Ok(Some(item));
        }
        match self.state {
            State::Chunked(_) => Err(self.unexpected_chunked_eof()),
            State::Done => Ok(Some(DecodedItem::End)),
        }
    }
}

fn find_crlf(src: &[u8]) -> Option<usize> {
    src.windows(2).position(|window| window == b"\r\n")
}

fn chunk_size_line_too_large() -> HttpError {
    HttpError::InvalidLength(format!(
        "chunk-size line exceeded {MAX_CHUNK_SIZE_LINE_SIZE} bytes"
    ))
}

fn trailer_section_too_large() -> HttpError {
    HttpError::InvalidLength(format!(
        "trailer section exceeded {MAX_TRAILER_SECTION_SIZE} bytes"
    ))
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use tokio_util::codec::Decoder;

    use super::*;

    #[test]
    fn chunked_decodes_single_chunk() {
        let mut decoder = HttpBodyDecoder::new(4);
        let mut src = BytesMut::new();
        // One chunk of 4 bytes, then a terminating 0 chunk.
        src.extend_from_slice(b"4\r\nwiki\r\n0\r\n\r\n");
        let item = decoder.decode(&mut src).unwrap().unwrap();
        let DecodedItem::Data { bytes, end_stream } = item else {
            panic!("expected data");
        };
        assert_eq!(&*bytes, b"wiki");
        assert!(!end_stream);
        let item = decoder.decode(&mut src).unwrap().unwrap();
        assert!(matches!(item, DecodedItem::End));
    }

    #[test]
    fn chunked_coalesces_across_chunks() {
        let mut decoder = HttpBodyDecoder::new(8 * 1024);
        let mut src = BytesMut::new();
        // 16 bytes in 4-byte chunks, should coalesce into one 8K frame.
        for _ in 0..4 {
            src.extend_from_slice(b"4\r\nwiki\r\n");
        }
        src.extend_from_slice(b"0\r\n\r\n");
        let item = decoder.decode(&mut src).unwrap().unwrap();
        let DecodedItem::Data { bytes, end_stream } = item else {
            panic!("expected data");
        };
        assert_eq!(bytes.len(), 16);
        assert!(end_stream);
        let item = decoder.decode(&mut src).unwrap().unwrap();
        assert!(matches!(item, DecodedItem::End));
    }
}
