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

//! Build script for quickwit-lambda-client.
//!
//! This script downloads the pre-built Lambda zip from a GitHub release
//! and places it in OUT_DIR for embedding via include_bytes!
//!
//! The Lambda binary is built separately in CI and published as a GitHub release.

use std::env;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// URL to download the pre-built Lambda zip from GitHub releases.
/// This should be updated when a new Lambda binary is released.
const LAMBDA_ZIP_URL: &str = "https://github.com/quickwit-oss/quickwit/releases/download/lambda-ff6fdfa5/quickwit-aws-lambda--aarch64.zip";

/// Expected SHA256 hash of the Lambda zip artifact.
/// Must be updated alongside LAMBDA_ZIP_URL when a new Lambda binary is released.
const LAMBDA_ZIP_SHA256: &str = "fa940f44178e28460c21e44bb2610b776542b9b97db66a53bc65b10cad653b90";

/// AWS Lambda direct upload limit is 50MB.
/// Larger artifacts must be uploaded via S3.
const MAX_LAMBDA_ZIP_SIZE: usize = 50 * 1024 * 1024;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    let zip_path = out_dir.join("lambda_bootstrap.zip");

    fetch_lambda_zip(&zip_path);

    // Export first 8 hex chars of the SHA256 as environment variable.
    // This is used to create a unique qualifier for Lambda versioning.
    let hash_short = &LAMBDA_ZIP_SHA256[..8];
    println!("cargo:rustc-env=LAMBDA_BINARY_HASH={}", hash_short);
    println!("lambda binary hash (short): {}", hash_short);
}

/// Fetch the Lambda zip and save it to `local_cache_path`.
///
/// If a cached file already exists with the correct SHA256, this is a no-op.
/// If the hash doesn't match (stale artifact), the file is deleted and re-downloaded.
/// If no cached file exists, the artifact is downloaded fresh.
///
/// This function panics if a problem happens.
fn fetch_lambda_zip(local_cache_path: &Path) {
    // Try the cache first.
    if let Ok(data) = std::fs::read(local_cache_path) {
        let actual_hash = sha256_hex(&data);
        if actual_hash == LAMBDA_ZIP_SHA256 {
            println!("using cached Lambda zip from {:?}", local_cache_path);
            return;
        }
        println!("cargo:warning=cached Lambda zip has wrong SHA256, re-downloading");
        std::fs::remove_file(local_cache_path).expect("failed to delete stale cached zip");
    }

    // Download from the remote URL.
    println!(
        "cargo:warning=downloading Lambda zip from: {}",
        LAMBDA_ZIP_URL
    );
    let data = download_lambda_zip(LAMBDA_ZIP_URL).expect("failed to download Lambda zip");

    // Verify SHA256 BEFORE writing to disk.
    let actual_hash = sha256_hex(&data);
    if actual_hash != LAMBDA_ZIP_SHA256 {
        panic!(
            "SHA256 mismatch for Lambda zip!\n  expected: {LAMBDA_ZIP_SHA256}\n  actual:   \
             {actual_hash}\nThe artifact at {LAMBDA_ZIP_URL} may have been tampered with."
        );
    }

    std::fs::write(local_cache_path, &data).expect("failed to write zip file");
    println!(
        "cargo:warning=downloaded Lambda zip to {:?} ({} bytes)",
        local_cache_path,
        data.len()
    );
}

fn sha256_hex(data: &[u8]) -> String {
    format!("{:x}", Sha256::digest(data))
}

fn download_lambda_zip(url: &str) -> Result<Vec<u8>, String> {
    let response = ureq::get(url)
        .call()
        .map_err(|err| format!("HTTP request failed: {err}"))?;
    // Set limit higher than MAX_LAMBDA_ZIP_SIZE so we can detect oversized artifacts.
    let data = response
        .into_body()
        .with_config()
        .limit(MAX_LAMBDA_ZIP_SIZE as u64 + 1)
        .read_to_vec()
        .map_err(|err| format!("failed to read response body: {err}"))?;
    if data.len() > MAX_LAMBDA_ZIP_SIZE {
        return Err(format!(
            "Lambda zip is too large ({} bytes, max {} bytes).\nAWS Lambda does not support \
             direct upload of binaries larger than 50MB.\nWorkaround: upload the Lambda zip to S3 \
             and deploy from there instead.",
            data.len(),
            MAX_LAMBDA_ZIP_SIZE
        ));
    }
    Ok(data)
}
