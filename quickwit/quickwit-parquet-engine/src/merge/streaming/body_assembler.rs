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

//! Page-bounded body column assembly — the streaming core.
//!
//! For each `(output_idx, body_col)` pair the engine builds a
//! [`BodyColOutputPageAssembler`] that yields one assembled output
//! page per `Iterator::next` call. Each call:
//!
//! 1. Walks the destinations table to pick `(input_idx, input_row)` pairs that map to the next
//!    `OUTPUT_PAGE_ROWS` positions of this output.
//! 2. Advances each contributing input's decoder forward until its cached pages cover the needed
//!    rows. The decoder + page cache live on [`InputDecoderState`] (not the assembler) so a page
//!    whose row range straddles two outputs survives into the next output's assembler — the stream
//!    cannot be rewound.
//! 3. Concatenates each input's cached pages and calls `arrow::compute::interleave` to assemble the
//!    output page.
//! 4. Evicts pages whose last row falls below the cursor.
//!
//! Memory bound per `next()`: one in-progress output page plus a few
//! in-flight input pages per input — never a whole column chunk. See
//! the MS-7 test in the parent test module for the runtime assertion.
//!
//! [`StreamingBodyColIter`] wraps the `Result<ArrayRef>` page stream
//! into the `Iterator<Item = ArrayRef>` shape
//! `write_next_column_arrays` expects, capturing the first assembly
//! error in a side cell.

use std::collections::HashSet;

use anyhow::{Context, Result, anyhow, bail};
use arrow::array::{Array, ArrayRef, new_null_array};
use arrow::compute::interleave;
use arrow::datatypes::{DataType, Field};
use tokio::runtime::Handle;

use super::{
    InputDecoderState, InputRowDestinations, OUTPUT_PAGE_ROWS, record_body_col_page_cache_len,
};

/// Adapts a `Result<ArrayRef>` page assembler into the
/// `Iterator<Item = ArrayRef>` shape `write_next_column_arrays` expects.
/// The first assembly error is captured in `error_slot` and iteration
/// ends; the caller MUST check the slot after the writer returns. If
/// `service_collector` is `Some`, every yielded page is scanned for
/// service names and added to the set; collection failures also stop
/// the iterator and populate `error_slot`.
pub(crate) struct StreamingBodyColIter<'a, I> {
    pub(crate) inner: I,
    pub(crate) error_slot: &'a mut Option<anyhow::Error>,
    pub(crate) service_collector: Option<&'a mut HashSet<String>>,
}

impl<I> Iterator for StreamingBodyColIter<'_, I>
where I: Iterator<Item = Result<ArrayRef>>
{
    type Item = ArrayRef;

    fn next(&mut self) -> Option<ArrayRef> {
        if self.error_slot.is_some() {
            return None;
        }
        match self.inner.next() {
            Some(Ok(arr)) => {
                if let Some(out) = self.service_collector.as_deref_mut()
                    && let Err(e) = collect_service_names_from_page(arr.as_ref(), out)
                {
                    *self.error_slot = Some(e);
                    return None;
                }
                Some(arr)
            }
            Some(Err(e)) => {
                *self.error_slot = Some(e);
                None
            }
            None => None,
        }
    }
}

/// Per-page service name collector. Used during the streaming write
/// of the "service" body col to populate per-output service_names.
pub(crate) fn collect_service_names_from_page(
    arr: &dyn Array,
    out: &mut HashSet<String>,
) -> Result<()> {
    use arrow::array::AsArray;
    use arrow::datatypes::{Int8Type, Int16Type, Int32Type, Int64Type};

    fn extend_from_strings(strings: &arrow::array::StringArray, out: &mut HashSet<String>) {
        for i in 0..strings.len() {
            if strings.is_valid(i) {
                out.insert(strings.value(i).to_string());
            }
        }
    }

    match arr.data_type() {
        DataType::Utf8 => {
            let strings = arr
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| anyhow!("expected StringArray for service col page"))?;
            extend_from_strings(strings, out);
        }
        DataType::LargeUtf8 => {
            let strings = arr
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
                .ok_or_else(|| anyhow!("expected LargeStringArray for service col page"))?;
            for i in 0..strings.len() {
                if strings.is_valid(i) {
                    out.insert(strings.value(i).to_string());
                }
            }
        }
        DataType::Dictionary(key_type, value_type)
            if matches!(value_type.as_ref(), DataType::Utf8) =>
        {
            // Extract the dictionary's values that are referenced by
            // valid (non-null) keys.
            match key_type.as_ref() {
                DataType::Int8 => {
                    let dict = arr.as_dictionary::<Int8Type>();
                    if let Some(strings) = dict
                        .values()
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                    {
                        for i in 0..dict.len() {
                            if dict.is_valid(i) {
                                let key = dict.keys().value(i) as usize;
                                if key < strings.len() && strings.is_valid(key) {
                                    out.insert(strings.value(key).to_string());
                                }
                            }
                        }
                    }
                }
                DataType::Int16 => {
                    let dict = arr.as_dictionary::<Int16Type>();
                    if let Some(strings) = dict
                        .values()
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                    {
                        for i in 0..dict.len() {
                            if dict.is_valid(i) {
                                let key = dict.keys().value(i) as usize;
                                if key < strings.len() && strings.is_valid(key) {
                                    out.insert(strings.value(key).to_string());
                                }
                            }
                        }
                    }
                }
                DataType::Int32 => {
                    let dict = arr.as_dictionary::<Int32Type>();
                    if let Some(strings) = dict
                        .values()
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                    {
                        for i in 0..dict.len() {
                            if dict.is_valid(i) {
                                let key = dict.keys().value(i) as usize;
                                if key < strings.len() && strings.is_valid(key) {
                                    out.insert(strings.value(key).to_string());
                                }
                            }
                        }
                    }
                }
                DataType::Int64 => {
                    let dict = arr.as_dictionary::<Int64Type>();
                    if let Some(strings) = dict
                        .values()
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                    {
                        for i in 0..dict.len() {
                            if dict.is_valid(i) {
                                let key = dict.keys().value(i) as usize;
                                if key < strings.len() && strings.is_valid(key) {
                                    out.insert(strings.value(key).to_string());
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        _ => {
            // Skip non-string types — service col is expected to be
            // string-like; if it isn't, just don't collect names.
        }
    }
    Ok(())
}

/// Assembles output pages for one (output_idx, body_col). See the
/// module docs for the full contract.
pub(crate) struct BodyColOutputPageAssembler<'a> {
    handle: &'a Handle,
    decoders_state: &'a mut [InputDecoderState],
    input_col_indices: &'a [Option<usize>],
    destinations: &'a InputRowDestinations,
    out_idx: usize,
    col_name: &'a str,
    out_field: &'a Field,
    /// Total rows written so far for this output's col.
    rows_emitted: usize,
    /// Total rows expected = destinations.rows_per_output[out_idx].
    expected_rows: usize,
    /// EOF flag (returns None on subsequent calls once true).
    done: bool,
}

impl<'a> BodyColOutputPageAssembler<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        handle: &'a Handle,
        decoders_state: &'a mut [InputDecoderState],
        input_col_indices: &'a [Option<usize>],
        destinations: &'a InputRowDestinations,
        out_idx: usize,
        col_name: &'a str,
        out_field: &'a Field,
    ) -> Self {
        Self {
            handle,
            decoders_state,
            input_col_indices,
            destinations,
            out_idx,
            col_name,
            out_field,
            rows_emitted: 0,
            expected_rows: destinations.rows_per_output[out_idx],
            done: false,
        }
    }

    pub(crate) fn into_iter(self) -> BodyColOutputPageIter<'a> {
        BodyColOutputPageIter { inner: self }
    }
}

pub(crate) struct BodyColOutputPageIter<'a> {
    inner: BodyColOutputPageAssembler<'a>,
}

impl Iterator for BodyColOutputPageIter<'_> {
    type Item = Result<ArrayRef>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.inner.done || self.inner.rows_emitted >= self.inner.expected_rows {
            self.inner.done = true;
            return None;
        }
        match assemble_one_output_page(&mut self.inner) {
            Ok(Some(arr)) => Some(Ok(arr)),
            Ok(None) => {
                self.inner.done = true;
                None
            }
            Err(e) => {
                self.inner.done = true;
                Some(Err(e))
            }
        }
    }
}

fn assemble_one_output_page(s: &mut BodyColOutputPageAssembler) -> Result<Option<ArrayRef>> {
    let remaining = s.expected_rows - s.rows_emitted;
    if remaining == 0 {
        return Ok(None);
    }
    let page_size = remaining.min(OUTPUT_PAGE_ROWS);

    // Collect (input_idx, input_row) indices for this output page by
    // walking destinations.per_input from each input's cursor. For
    // each target output position, find the (input, row) that maps to
    // it. See module docs for why this is correct given the merge
    // plan invariants.
    let mut indices_per_input: Vec<Vec<usize>> = vec![Vec::new(); s.decoders_state.len()];
    let mut interleave_indices: Vec<(usize, usize)> = Vec::with_capacity(page_size);
    let mut total_picked = 0usize;

    while total_picked < page_size {
        let target_pos = s.rows_emitted + total_picked;
        let mut found = false;
        for (input_idx, dests) in s.destinations.per_input.iter().enumerate() {
            let cursor = match s.input_col_indices[input_idx] {
                Some(col_parquet_idx) => {
                    s.decoders_state[input_idx].body_col_cursor(col_parquet_idx)
                }
                None => 0,
            };
            for (input_row, dest) in dests.iter().enumerate().skip(cursor) {
                match dest {
                    Some((o, p)) if *o == s.out_idx => {
                        if *p == target_pos {
                            interleave_indices.push((input_idx, input_row));
                            indices_per_input[input_idx].push(input_row);
                            found = true;
                            break;
                        }
                    }
                    _ => {}
                }
                if found {
                    break;
                }
            }
            if found {
                break;
            }
        }
        if !found {
            // Shouldn't happen — every output position should be reachable.
            bail!(
                "merge plan inconsistency: output {} position {target_pos} not found in any input",
                s.out_idx,
            );
        }
        total_picked += 1;
    }

    // Advance each contributing input's decoder until its cached
    // pages cover the maximum input row we'll reference.
    for (input_idx, input_rows) in indices_per_input.iter().enumerate() {
        if input_rows.is_empty() {
            continue;
        }
        let col_parquet_idx = match s.input_col_indices[input_idx] {
            Some(c) => c,
            None => continue,
        };
        let max_needed_row = *input_rows.iter().max().expect("non-empty");
        advance_decoder_to_row(
            s.handle,
            &mut s.decoders_state[input_idx],
            col_parquet_idx,
            max_needed_row,
        )?;
    }

    // Build the per-input value array. For inputs lacking this col,
    // a single null-row placeholder routes interleave indices to position 0.
    let mut input_array_refs: Vec<ArrayRef> = Vec::with_capacity(s.decoders_state.len());
    let mut input_cache_starts: Vec<usize> = Vec::with_capacity(s.decoders_state.len());

    for input_idx in 0..s.decoders_state.len() {
        match s.input_col_indices[input_idx] {
            Some(col_parquet_idx) => {
                let pages = s.decoders_state[input_idx].body_col_cache(col_parquet_idx);
                if pages.is_empty() {
                    input_array_refs.push(new_null_array(s.out_field.data_type(), 0));
                    input_cache_starts.push(0);
                } else {
                    let cache_start = pages[0].row_start;
                    let arrays: Vec<&dyn Array> = pages.iter().map(|p| p.array.as_ref()).collect();
                    let concatenated = arrow::compute::concat(&arrays).with_context(|| {
                        format!(
                            "concatenating cached pages for input {input_idx} col '{}'",
                            s.col_name,
                        )
                    })?;
                    input_array_refs.push(concatenated);
                    input_cache_starts.push(cache_start);
                }
            }
            None => {
                let null_arr = new_null_array(s.out_field.data_type(), 1);
                input_array_refs.push(null_arr);
                input_cache_starts.push(0);
            }
        }
    }

    let interleave_local: Vec<(usize, usize)> = interleave_indices
        .iter()
        .map(|&(i_idx, i_row)| match s.input_col_indices[i_idx] {
            Some(_) => (i_idx, i_row - input_cache_starts[i_idx]),
            None => (i_idx, 0),
        })
        .collect();

    let array_refs_ref: Vec<&dyn Array> = input_array_refs.iter().map(|a| a.as_ref()).collect();
    let assembled = interleave(&array_refs_ref, &interleave_local).with_context(|| {
        format!(
            "interleaving body col '{}' for output {}",
            s.col_name, s.out_idx,
        )
    })?;

    // Bump cursors past consumed rows and evict pages whose last row
    // falls below the cursor. Both live on InputDecoderState (per
    // col) so they persist across regions/outputs that share this
    // column.
    for (input_idx, input_rows) in indices_per_input.iter().enumerate() {
        if input_rows.is_empty() {
            continue;
        }
        let max_row = *input_rows.iter().max().expect("non-empty");
        let state = &mut s.decoders_state[input_idx];
        let Some(col_parquet_idx) = s.input_col_indices[input_idx] else {
            continue;
        };
        state.set_body_col_cursor(col_parquet_idx, max_row + 1);
    }

    s.rows_emitted += page_size;
    Ok(Some(assembled))
}

/// Drive the input's persistent decoder forward via `block_on` until
/// the cached pages for `col_parquet_idx` cover up through `target_row`
/// (inclusive). Stops as soon as the latest cached page ends past
/// `target_row`.
///
/// The decoder MUST be the long-lived [`InputDecoderState::decoder`]:
/// it preserves the per-(rg, col) `rows_decoded` counter so successive
/// `DecodedPage::row_start` values are absolute input row indices,
/// not page-local zeros. Likewise, the cache lives on the state so
/// pages whose row range spans an output boundary survive into the
/// next output's assembler.
fn advance_decoder_to_row(
    handle: &Handle,
    state: &mut InputDecoderState,
    col_parquet_idx: usize,
    target_row: usize,
) -> Result<()> {
    // Already covered by what's cached for this col?
    if let Some(last) = state.body_col_cache(col_parquet_idx).last() {
        let last_end = last.row_start + last.array.len();
        if target_row < last_end {
            return Ok(());
        }
    }

    // Drain pages from the stream until either the target is covered
    // for `col_parquet_idx` or the stream runs out. Pages emitted for
    // a different col_idx still get cached under their own col so a
    // later request for that col can find them without re-fetching —
    // critical for the synthesized-prefix path, which re-reads earlier
    // cols across multiple regions after the stream has moved on.
    loop {
        let decoded = handle
            .block_on(state.decoder.decode_next_page())
            .context("decoding body col page")?;
        let page = match decoded {
            Some(p) => p,
            None => bail!(
                "stream EOF while advancing to row {target_row} for parquet col {col_parquet_idx}",
            ),
        };
        let page_col = page.col_idx;
        let end = page.row_start + page.array.len();
        state.body_col_cache_mut(page_col).push(page);
        record_body_col_page_cache_len(state.body_col_caches_total_len());
        if page_col == col_parquet_idx && target_row < end {
            return Ok(());
        }
    }
}
