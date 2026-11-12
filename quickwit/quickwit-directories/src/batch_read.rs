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

use std::future::Future;
use std::io;
use std::ops::Range;

use tantivy::directory::OwnedBytes;

/// Filters cache hits and reads all misses as one batch, preserving result order.
/// Like `FileHandle::read_bytes_batch_async`, `read_misses` must return one buffer
/// per range, in the same order.
pub(crate) async fn read_batch_with_cache<Get, Read, ReadFuture>(
    byte_ranges: Vec<Range<usize>>,
    mut get_cached: Get,
    read_misses: Read,
) -> io::Result<Vec<OwnedBytes>>
where
    Get: FnMut(Range<usize>) -> Option<OwnedBytes>,
    Read: FnOnce(Vec<Range<usize>>) -> ReadFuture,
    ReadFuture: Future<Output = io::Result<Vec<OwnedBytes>>>,
{
    let mut results = vec![OwnedBytes::empty(); byte_ranges.len()];
    let mut misses = Vec::new();
    for (index, byte_range) in byte_ranges.iter().enumerate() {
        if let Some(bytes) = get_cached(byte_range.clone()) {
            results[index] = bytes;
        } else {
            misses.push(index);
        }
    }
    if misses.is_empty() {
        return Ok(results);
    }
    if misses.len() == byte_ranges.len() {
        return read_misses(byte_ranges).await;
    }
    let ranges = misses
        .iter()
        .map(|&index| byte_ranges[index].clone())
        .collect();
    let payloads = read_misses(ranges).await?;
    for (index, bytes) in misses.into_iter().zip(payloads) {
        results[index] = bytes;
    }
    Ok(results)
}
