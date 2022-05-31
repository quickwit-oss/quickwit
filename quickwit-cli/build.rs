// Copyright (C) 2021 Quickwit, Inc.
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

use std::process::Command;

const NONE: &str = "none";

const UNKNOWN: &str = "unknown";

fn main() {
    commit_info();
}

fn commit_info() {
    // Extract commit hash and date
    let output_bytes = match Command::new("git")
        .arg("log")
        .arg("-1")
        .arg("--date=short")
        .arg("--format=%H %h %cd")
        .arg("--abbrev=9")
        .output()
    {
        Ok(output) if output.status.success() => output.stdout,
        _ => Vec::new(),
    };
    let output = String::from_utf8(output_bytes).unwrap();
    let mut parts = output.split_whitespace();
    let mut next = || parts.next().unwrap_or(UNKNOWN);
    println!("cargo:rustc-env=QW_COMMIT_HASH={}", next());
    println!("cargo:rustc-env=QW_COMMIT_SHORT_HASH={}", next());
    println!("cargo:rustc-env=QW_COMMIT_DATE={}", next());

    // Extract commit version tag
    let output_bytes = match Command::new("git")
        .arg("tag")
        .arg("--points-at")
        .arg("HEAD")
        .output()
    {
        Ok(output) if output.status.success() => output.stdout,
        _ => Vec::new(),
    };
    let output = String::from_utf8(output_bytes).unwrap();
    let version_tag = output
        .split_whitespace()
        .find(|tag| tag.ends_with(env!("CARGO_PKG_VERSION")))
        .unwrap_or(NONE);
    println!("cargo:rustc-env=QW_COMMIT_VERSION_TAG={}", version_tag);
}
