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

//! Developer CLI for inspecting Parquet files written by Quickwit's
//! metrics pipeline.
//!
//! Reads a `.parquet` file's footer (including the page-level Column
//! Index and Offset Index produced by `EnabledStatistics::Page`) and
//! prints a per-row-group, per-column report. The output is intended
//! for debugging the on-disk format and verifying claims like
//! `qh.rg_partition_prefix_len`.
//!
//! ## Usage
//!
//! ```text
//! cargo run -p quickwit-parquet-engine --bin inspect_parquet -- <PATH> [FLAGS]
//!
//!   --json             Emit JSON instead of human-readable output
//!   --all-pages        Show every page (default: first 10 per column)
//!   --verify-prefix    Check that the file's claimed
//!                      qh.rg_partition_prefix_len actually holds; exits
//!                      with status 2 if it doesn't
//! ```

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result, bail};
use quickwit_parquet_engine::storage::{
    ColumnReport, ParquetPageStatsReport, inspect_parquet_page_stats, verify_partition_prefix,
};

const DEFAULT_PAGE_PREVIEW_LIMIT: usize = 10;

fn main() -> ExitCode {
    let opts = match parse_args() {
        Ok(opts) => opts,
        Err(err) => {
            eprintln!("error: {err:#}");
            eprintln!();
            print_usage();
            return ExitCode::from(2);
        }
    };

    match run(opts) {
        Ok(()) => ExitCode::SUCCESS,
        Err(VerifyOrIoError::Verification(err)) => {
            eprintln!("verification failed: {err:#}");
            ExitCode::from(2)
        }
        Err(VerifyOrIoError::Io(err)) => {
            eprintln!("error: {err:#}");
            ExitCode::FAILURE
        }
    }
}

#[derive(Debug)]
struct Options {
    path: PathBuf,
    json: bool,
    all_pages: bool,
    verify_prefix: bool,
}

fn parse_args() -> Result<Options> {
    let mut path: Option<PathBuf> = None;
    let mut json = false;
    let mut all_pages = false;
    let mut verify_prefix = false;

    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--json" => json = true,
            "--all-pages" => all_pages = true,
            "--verify-prefix" => verify_prefix = true,
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            other if other.starts_with("--") => bail!("unknown flag: {other}"),
            other => {
                if path.is_some() {
                    bail!("expected exactly one path argument; got an extra: {other}");
                }
                path = Some(PathBuf::from(other));
            }
        }
    }

    Ok(Options {
        path: path.context("path to a .parquet file is required")?,
        json,
        all_pages,
        verify_prefix,
    })
}

fn print_usage() {
    eprintln!(
        "usage: inspect_parquet <PATH> [--json] [--all-pages] [--verify-prefix]\n\nInspects a \
         Parquet file's footer and page-level statistics.\nExit codes: 0 = success, 1 = I/O / \
         parse error, 2 = verification failed."
    );
}

enum VerifyOrIoError {
    Verification(anyhow::Error),
    Io(anyhow::Error),
}

impl<E: Into<anyhow::Error>> From<E> for VerifyOrIoError {
    fn from(err: E) -> Self {
        Self::Io(err.into())
    }
}

fn run(opts: Options) -> Result<(), VerifyOrIoError> {
    let max_pages = if opts.all_pages {
        usize::MAX
    } else {
        DEFAULT_PAGE_PREVIEW_LIMIT
    };
    let report = inspect_parquet_page_stats(&opts.path, max_pages)?;

    if opts.json {
        let json = serde_json::to_string_pretty(&report).context("serializing report as JSON")?;
        println!("{json}");
    } else {
        print_human_report(&opts.path, &report, opts.all_pages);
    }

    if opts.verify_prefix {
        verify_partition_prefix(&report).map_err(VerifyOrIoError::Verification)?;
        if !opts.json {
            println!();
            println!(
                "verify-prefix: OK (rg_partition_prefix_len = {})",
                report.rg_partition_prefix_len
            );
        }
    }

    Ok(())
}

fn print_human_report(path: &std::path::Path, report: &ParquetPageStatsReport, all_pages: bool) {
    println!("== File ==");
    println!("  path:                {}", path.display());
    println!("  size:                {} bytes", report.file_size);
    println!("  num_row_groups:      {}", report.num_row_groups);
    println!(
        "  rg_partition_prefix_len: {}{}",
        report.rg_partition_prefix_len,
        if report.rg_partition_prefix_len == 0 {
            " (absent — no alignment claimed)"
        } else {
            ""
        }
    );
    if let Some(sf) = &report.sort_fields {
        println!("  sort_fields:         {sf}");
    }
    println!(
        "  page_index_coverage: {}",
        if report.has_full_page_index_coverage() {
            "full"
        } else {
            "partial — some columns missing column index"
        }
    );

    if !report.kv_metadata.is_empty() {
        println!();
        println!("== KV Metadata ==");
        for (k, v) in &report.kv_metadata {
            let display_value = if v.len() > 80 {
                format!("{}… ({} bytes)", &v[..77], v.len())
            } else {
                v.clone()
            };
            println!("  {k} = {display_value}");
        }
    }

    for (rg_idx, rg) in report.row_groups.iter().enumerate() {
        println!();
        println!("== Row Group {rg_idx} ==");
        println!("  num_rows:        {}", rg.num_rows);
        println!("  total_byte_size: {}", rg.total_byte_size);
        for col in &rg.columns {
            print_column(col, all_pages);
        }
    }
}

fn print_column(col: &ColumnReport, all_pages: bool) {
    println!();
    println!("  -- Column: {} --", col.column_path);
    println!(
        "     column_index: {}, offset_index: {}, num_pages: {}",
        col.has_column_index, col.has_offset_index, col.num_pages
    );
    let (chunk_min, chunk_max) = (
        col.chunk_min.as_deref().unwrap_or("—"),
        col.chunk_max.as_deref().unwrap_or("—"),
    );
    println!("     chunk min/max: [{chunk_min}, {chunk_max}]");

    if col.pages.is_empty() {
        return;
    }

    let displayed = if all_pages {
        col.pages.len()
    } else {
        col.pages.len().min(DEFAULT_PAGE_PREVIEW_LIMIT)
    };
    let truncated = col.num_pages.saturating_sub(displayed);
    if displayed == 0 {
        return;
    }

    println!("     pages [showing {displayed} of {}]:", col.num_pages);
    for (i, page) in col.pages.iter().take(displayed).enumerate() {
        let min = page.min.as_deref().unwrap_or("—");
        let max = page.max.as_deref().unwrap_or("—");
        let nulls = page
            .null_count
            .map(|n| n.to_string())
            .unwrap_or_else(|| "—".to_string());
        let rows = page
            .num_rows
            .map(|n| n.to_string())
            .unwrap_or_else(|| "—".to_string());
        let off = page
            .offset
            .map(|n| format!("{n}"))
            .unwrap_or_else(|| "—".to_string());
        let csz = page
            .compressed_page_size
            .map(|n| format!("{n}"))
            .unwrap_or_else(|| "—".to_string());
        println!(
            "       [{i:>3}] rows={rows:>6}  off={off:>10}  compressed_size={csz:>8}  \
             nulls={nulls:>6}  min/max=[{min}, {max}]"
        );
    }
    if truncated > 0 {
        println!("     … {truncated} more pages omitted (use --all-pages to see them)");
    }
}
