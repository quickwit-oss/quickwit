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
use crate::response::BodyStrategy;

pub(super) const MAX_CHUNK_SIZE_LINE_SIZE: usize = 8 * 1024;
pub(super) const MAX_TRAILER_SECTION_SIZE: usize = 64 * 1024;

#[derive(Debug)]
pub(super) enum DecodeState {
    Known { expected: usize, remaining: usize },
    Chunked(ChunkedState),
    UntilClose,
    Done,
    Invalid(Option<HttpError>),
}

#[derive(Debug)]
pub(super) enum ChunkedState {
    Size,
    Data { remaining: usize },
    AfterCrlf,
    Trailers,
}

pub(super) enum DecodedItem {
    Data { bytes: Bytes, end_stream: bool },
    End,
}

#[derive(Debug)]
pub(super) struct HttpBodyDecoder {
    state: DecodeState,
    target: usize,
    chunk_buf: BytesMut,
    chunk_decoded: usize,
    trailer_bytes: usize,
}

impl HttpBodyDecoder {
    pub(super) fn new(strategy: BodyStrategy, target: usize, leftover_len: usize) -> Self {
        let state = match strategy {
            BodyStrategy::Known(expected) if leftover_len > expected => DecodeState::Invalid(Some(
                HttpError::InvalidLength("body longer than Content-Length".to_string()),
            )),
            BodyStrategy::Known(expected) => DecodeState::Known {
                expected,
                remaining: expected,
            },
            BodyStrategy::Chunked => DecodeState::Chunked(ChunkedState::Size),
            BodyStrategy::UntilClose => DecodeState::UntilClose,
            BodyStrategy::Empty => DecodeState::Done,
        };
        Self {
            state,
            target,
            chunk_buf: BytesMut::new(),
            chunk_decoded: 0,
            trailer_bytes: 0,
        }
    }

    pub(super) fn initial_capacity(&self) -> usize {
        match self.state {
            DecodeState::Known { remaining, .. } => remaining.min(self.target),
            DecodeState::UntilClose => self.target,
            DecodeState::Chunked(_) => MAX_CHUNK_SIZE_LINE_SIZE,
            DecodeState::Done | DecodeState::Invalid(_) => 1,
        }
    }

    pub(super) fn known_remaining(&self) -> Option<usize> {
        match self.state {
            DecodeState::Known { remaining, .. } => Some(remaining),
            DecodeState::Done => Some(0),
            _ => None,
        }
    }

    fn decode_known(&mut self, src: &mut BytesMut) -> Result<Option<DecodedItem>, HttpError> {
        let DecodeState::Known { remaining, .. } = &mut self.state else {
            unreachable!();
        };
        if src.len() > *remaining {
            return Err(HttpError::InvalidLength(
                "body longer than Content-Length".to_string(),
            ));
        }
        let minimum_frame_len = (*remaining).min(self.target);
        if src.len() < minimum_frame_len {
            src.reserve(minimum_frame_len - src.len());
            return Ok(None);
        }

        // Transfer read-ahead too, leaving no partial frame to copy on reserve.
        let bytes = std::mem::take(src).freeze();
        *remaining -= bytes.len();
        let end_stream = *remaining == 0;
        if end_stream {
            self.state = DecodeState::Done;
        }
        Ok(Some(DecodedItem::Data { bytes, end_stream }))
    }

    fn decode_until_close(&mut self, src: &mut BytesMut) -> Option<DecodedItem> {
        if src.len() < self.target {
            src.reserve(self.target - src.len());
            return None;
        }
        Some(DecodedItem::Data {
            bytes: src.split_to(self.target).freeze(),
            end_stream: false,
        })
    }

    fn take_chunk_frame(&mut self, end_stream: bool) -> DecodedItem {
        let bytes = self.chunk_buf.split().freeze();
        self.chunk_decoded += bytes.len();
        DecodedItem::Data { bytes, end_stream }
    }

    fn decode_chunked(&mut self, src: &mut BytesMut) -> Result<Option<DecodedItem>, HttpError> {
        loop {
            match &mut self.state {
                DecodeState::Chunked(ChunkedState::Size) => match httparse::parse_chunk_size(src) {
                    Ok(httparse::Status::Complete((line_len, chunk_size))) => {
                        if line_len > MAX_CHUNK_SIZE_LINE_SIZE {
                            return Err(chunk_size_line_too_large());
                        }
                        let chunk_size = usize::try_from(chunk_size).map_err(|_| {
                            HttpError::InvalidLength("chunk size does not fit in usize".to_string())
                        })?;
                        src.advance(line_len);
                        self.state = if chunk_size == 0 {
                            DecodeState::Chunked(ChunkedState::Trailers)
                        } else {
                            DecodeState::Chunked(ChunkedState::Data {
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
                DecodeState::Chunked(ChunkedState::Data { remaining }) => {
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
                        self.state = DecodeState::Chunked(ChunkedState::AfterCrlf);
                        continue;
                    }
                    return Ok(None);
                }
                DecodeState::Chunked(ChunkedState::AfterCrlf) => {
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
                    self.state = DecodeState::Chunked(ChunkedState::Size);
                }
                DecodeState::Chunked(ChunkedState::Trailers) => {
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
                    self.state = DecodeState::Done;
                    return if self.chunk_buf.is_empty() {
                        Ok(Some(DecodedItem::End))
                    } else {
                        Ok(Some(self.take_chunk_frame(true)))
                    };
                }
                _ => unreachable!(),
            }
        }
    }

    fn unexpected_chunked_eof(&self) -> HttpError {
        let buffered = self.chunk_decoded + self.chunk_buf.len();
        let expected = match self.state {
            DecodeState::Chunked(ChunkedState::Data { remaining }) => buffered + remaining,
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
        match self.state {
            DecodeState::Known { .. } => self.decode_known(src),
            DecodeState::Chunked(_) => self.decode_chunked(src),
            DecodeState::UntilClose => Ok(self.decode_until_close(src)),
            DecodeState::Done => Ok(Some(DecodedItem::End)),
            DecodeState::Invalid(ref mut error) => Err(error
                .take()
                .unwrap_or_else(|| HttpError::InvalidLength("invalid body state".to_string()))),
        }
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(item) = self.decode(src)? {
            return Ok(Some(item));
        }
        match self.state {
            DecodeState::Known {
                expected,
                remaining,
            } => Err(HttpError::UnexpectedEof {
                read: expected - remaining + src.len(),
                expected,
            }),
            DecodeState::Chunked(_) => Err(self.unexpected_chunked_eof()),
            DecodeState::UntilClose => {
                self.state = DecodeState::Done;
                if src.is_empty() {
                    Ok(Some(DecodedItem::End))
                } else {
                    Ok(Some(DecodedItem::Data {
                        bytes: src.split().freeze(),
                        end_stream: true,
                    }))
                }
            }
            DecodeState::Done => Ok(Some(DecodedItem::End)),
            DecodeState::Invalid(_) => unreachable!("decode returned the invalid-state error"),
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
    use crate::response::BodyStrategy;

    #[test]
    fn known_decoder_includes_read_ahead_in_frame() {
        let mut decoder = HttpBodyDecoder::new(BodyStrategy::Known(2_000), 1_000, 0);
        let mut src = BytesMut::from(&[b'x'; 1_200][..]);

        let Some(DecodedItem::Data { bytes, end_stream }) = decoder.decode(&mut src).unwrap()
        else {
            panic!("expected a data frame");
        };
        assert_eq!(bytes.len(), 1_200);
        assert!(!end_stream);
        assert!(src.is_empty());

        src.extend_from_slice(&[b'x'; 800]);
        let Some(DecodedItem::Data { bytes, end_stream }) = decoder.decode(&mut src).unwrap()
        else {
            panic!("expected the final data frame");
        };
        assert_eq!(bytes.len(), 800);
        assert!(end_stream);
    }
}
