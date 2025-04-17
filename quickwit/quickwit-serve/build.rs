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

use std::env;
use std::process::Command;

use time::OffsetDateTime;
use time::macros::format_description;

fn main() {
    println!(
        "cargo:rustc-env=BUILD_DATE={}",
        OffsetDateTime::now_utc()
            .format(format_description!(
                "[year]-[month]-[day]T[hour]:[minute]:[second]Z"
            ))
            .unwrap()
    );
    println!(
        "cargo:rustc-env=BUILD_PROFILE={}",
        env::var("PROFILE").unwrap()
    );
    println!(
        "cargo:rustc-env=BUILD_TARGET={}",
        env::var("TARGET").unwrap()
    );
    commit_info();
}

/// Extracts commit date, hash, and tags
fn commit_info() {
    // Extract commit date and hash.
    let output_bytes = match Command::new("git")
        .arg("log")
        .arg("-1")
        .arg("--format=%cd %H")
        .arg("--date=format-local:%Y-%m-%dT%H:%M:%SZ")
        .env("TZ", "UTC0")
        .output()
    {
        Ok(output) if output.status.success() => output.stdout,
        _ => Vec::new(),
    };
    let output = String::from_utf8(output_bytes).unwrap();
    let mut parts = output.split_whitespace();

    if let Some(commit_date) = parts.next() {
        println!("cargo:rustc-env=QW_COMMIT_DATE={commit_date}");
    }
    if let Some(commit_hash) = parts.next() {
        println!("cargo:rustc-env=QW_COMMIT_HASH={commit_hash}");
    }

    // Extract commit tags.
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
    let tags = output.lines().collect::<Vec<_>>();
    if !tags.is_empty() {
        println!("cargo:rustc-env=QW_COMMIT_TAGS={}", tags.join(","));
    }
}
