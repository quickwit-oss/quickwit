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

use bytes::Bytes;
use quickwit_proto::ingest::DocBatchV2;
use quickwit_proto::types::DocUidGenerator;

/// Iterates over the non-blank lines of an NDJSON body.
pub fn split_ndjson_lines(ndjson_body: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut line_start = 0;
    memchr::memchr_iter(b'\n', ndjson_body)
        .chain(std::iter::once(ndjson_body.len()))
        .map(move |line_end| {
            let line = &ndjson_body[line_start..line_end];
            line_start = line_end + 1;
            line
        })
        .filter(|line| !is_empty_or_blank_line(line))
}

/// Builds a [`DocBatchV2`] whose doc buffer is the NDJSON body itself, without copying. The docs
/// must cover the whole buffer, so newlines and blank lines stay attached to neighboring docs.
pub fn doc_batch_v2_from_ndjson(ndjson_body: Bytes) -> Option<DocBatchV2> {
    let mut doc_uids = Vec::new();
    let mut doc_lengths = Vec::new();
    let mut doc_uid_generator = DocUidGenerator::default();
    let mut segment_start = 0usize;
    let mut line_start = 0usize;

    for position in memchr::memchr_iter(b'\n', &ndjson_body) {
        let line = &ndjson_body[line_start..position];
        if !is_empty_or_blank_line(line) {
            doc_uids.push(doc_uid_generator.next_doc_uid());
            doc_lengths.push((position + 1 - segment_start) as u32);
            segment_start = position + 1;
        }
        line_start = position + 1;
    }

    let line = &ndjson_body[line_start..];
    if !is_empty_or_blank_line(line) {
        doc_uids.push(doc_uid_generator.next_doc_uid());
        doc_lengths.push((ndjson_body.len() - segment_start) as u32);
        segment_start = ndjson_body.len();
    }

    if doc_uids.is_empty() {
        return None;
    }
    if segment_start < ndjson_body.len() {
        let trailing_whitespace_len = ndjson_body.len() - segment_start;
        let last_doc_len = doc_lengths
            .last_mut()
            .expect("doc lengths should not be empty");
        *last_doc_len += trailing_whitespace_len as u32;
    }

    Some(DocBatchV2 {
        doc_uids,
        doc_buffer: ndjson_body,
        doc_lengths,
    })
}

#[inline]
fn is_empty_or_blank_line(line: &[u8]) -> bool {
    line.is_empty() || line.iter().all(|ch| ch.is_ascii_whitespace())
}

#[cfg(test)]
mod tests {
    use std::str;

    use super::*;

    #[test]
    fn test_split_ndjson_lines() {
        let test_cases = [
            // an empty line is inserted before the metadata action and the doc
            (&b"\n{ \"create\" : { \"_index\" : \"my-index-1\", \"_id\" : \"1\"} }\n{\"id\": 1, \"message\": \"push\"}"[..], 2),
            // a blank line is inserted before the metadata action and the doc
            (&b"       \n{ \"create\" : { \"_index\" : \"my-index-1\", \"_id\" : \"1\"} }\n{\"id\": 1, \"message\": \"push\"}"[..], 2),
            // an empty line is inserted after the metadata action and before the doc
            (&b"{ \"create\" : { \"_index\" : \"my-index-1\", \"_id\" : \"1\"} }\n\n{\"id\": 1, \"message\": \"push\"}"[..], 2),
            // a blank line is inserted after the metadata action and before the doc
            (&b"{ \"create\" : { \"_index\" : \"my-index-1\", \"_id\" : \"1\"} }\n     \n{\"id\": 1, \"message\": \"push\"}"[..], 2),
        ];
        for &(input, expected_count) in &test_cases {
            assert_eq!(split_ndjson_lines(input).count(), expected_count);
        }
        let lines: Vec<&[u8]> = split_ndjson_lines(b"a\n\nbc\n  \nd").collect();
        assert_eq!(lines, [&b"a"[..], b"bc", b"d"]);

        let lines: Vec<&[u8]> = split_ndjson_lines(b"a\n").collect();
        assert_eq!(lines, [&b"a"[..]]);

        assert_eq!(split_ndjson_lines(b"").count(), 0);
        assert_eq!(split_ndjson_lines(b"\n \n\t").count(), 0);
    }

    #[test]
    fn test_doc_batch_v2_from_ndjson_zero_copy() {
        let body = Bytes::from_static(b"\n  {\"id\":1}\n\n{\"id\":2}\n   \n");
        let doc_batch = doc_batch_v2_from_ndjson(body.clone()).unwrap();
        assert_eq!(doc_batch.num_docs(), 2);
        assert_eq!(doc_batch.doc_buffer, body);

        let docs: Vec<Bytes> = doc_batch.docs().map(|(_doc_uid, doc)| doc).collect();
        assert_eq!(str::from_utf8(&docs[0]).unwrap(), "\n  {\"id\":1}\n");
        assert_eq!(str::from_utf8(&docs[1]).unwrap(), "\n{\"id\":2}\n   \n");
    }

    #[test]
    fn test_doc_batch_v2_from_ndjson_no_trailing_newline() {
        let body = Bytes::from_static(b"{\"id\":1}\n{\"id\":2}");
        let doc_batch = doc_batch_v2_from_ndjson(body).unwrap();
        let docs: Vec<Bytes> = doc_batch.docs().map(|(_doc_uid, doc)| doc).collect();
        assert_eq!(docs, [&b"{\"id\":1}\n"[..], b"{\"id\":2}"]);
    }

    #[test]
    fn test_doc_batch_v2_from_blank_ndjson() {
        assert!(doc_batch_v2_from_ndjson(Bytes::new()).is_none());
        assert!(doc_batch_v2_from_ndjson(Bytes::from_static(b"\n \n\t")).is_none());
    }

    #[test]
    fn test_doc_batch_v2_from_ndjson_matches_split_ndjson_lines() {
        let body = Bytes::from_static(b"  \n{\"a\":1}\n\n \t\n{\"b\":2}\n{\"c\":3}\n\n");
        let doc_batch = doc_batch_v2_from_ndjson(body.clone()).unwrap();
        let docs: Vec<Bytes> = doc_batch.docs().map(|(_doc_uid, doc)| doc).collect();
        let lines: Vec<&[u8]> = split_ndjson_lines(&body).collect();
        assert_eq!(docs.len(), lines.len());
        for (doc, line) in docs.iter().zip(&lines) {
            assert_eq!(doc.trim_ascii(), *line);
        }
        let doc_uids: Vec<_> = doc_batch.docs().map(|(doc_uid, _doc)| doc_uid).collect();
        assert!(doc_uids.windows(2).all(|pair| pair[0] < pair[1]));
    }
}
