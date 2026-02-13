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
use std::path::PathBuf;

/// URL to download the pre-built Lambda zip from GitHub releases.
/// This should be updated when a new Lambda binary is released.
const LAMBDA_ZIP_URL: &str = "https://github.com/quickwit-oss/quickwit/releases/download/lambda-ff6fdfa5/quickwit-aws-lambda--aarch64.zip";

/// AWS Lambda direct upload limit is 50MB.
/// Larger artifacts must be uploaded via S3.
const MAX_LAMBDA_ZIP_SIZE: usize = 50 * 1024 * 1024;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    let zip_path = out_dir.join("lambda_bootstrap.zip");
    let url_marker_path = out_dir.join("lambda_bootstrap.url");

    // Check if we already have the zip from the same URL
    let needs_download = if zip_path.try_exists().unwrap() && url_marker_path.try_exists().unwrap()
    {
        let cached_url = std::fs::read_to_string(&url_marker_path).unwrap_or_default();
        cached_url.trim() != LAMBDA_ZIP_URL
    } else {
        true
    };

    let lambda_zip_payload: Vec<u8> = if needs_download {
        println!(
            "cargo:warning=Downloading Lambda zip from: {}",
            LAMBDA_ZIP_URL
        );
        let data: Vec<u8> =
            download_lambda_zip(LAMBDA_ZIP_URL).expect("failed to download lambda zip");
        std::fs::write(&zip_path, &data).expect("Failed to write zip file");
        std::fs::write(&url_marker_path, LAMBDA_ZIP_URL).expect("Failed to write URL marker");
        println!(
            "cargo:warning=Downloaded Lambda zip to {:?} ({} bytes)",
            zip_path,
            data.len()
        );
        data
    } else {
        println!("Using cached Lambda zip from {:?}", zip_path);
        std::fs::read(&zip_path).expect("Failed to read cached zip file")
    };

    // Compute MD5 hash of the zip and export as environment variable.
    // This is used to create a unique qualifier for Lambda versioning.
    let digest = md5::compute(&lambda_zip_payload);
    let hash_short = &format!("{:x}", digest)[..8]; // First 8 hex chars
    println!("cargo:rustc-env=LAMBDA_BINARY_HASH={}", hash_short);
    println!("Lambda binary hash (short): {}", hash_short);
}

fn download_lambda_zip(url: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let response = ureq::get(url).call();
    // Set limit higher than MAX_LAMBDA_ZIP_SIZE so we can provide a better error message
    let data = response?
        .into_body()
        .with_config()
        .limit(MAX_LAMBDA_ZIP_SIZE as u64 + 1) // We download one more byte to trigger the panic below.
        .read_to_vec()?;
    if data.len() > MAX_LAMBDA_ZIP_SIZE {
        panic!(
            "Lambda zip is too large ({} bytes, max {} bytes).\nAWS Lambda does not support \
             direct upload of binaries larger than 50MB.\nWorkaround: upload the Lambda zip to S3 \
             and deploy from there instead.",
            data.len(),
            MAX_LAMBDA_ZIP_SIZE
        );
    }
    validate_zip(&data)?;
    Ok(data)
}

fn validate_zip(data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let cursor = std::io::Cursor::new(data);
    let mut archive = zip::ZipArchive::new(cursor)?;
    // Verify we can read all entries (checks CRC for each file)
    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        std::io::copy(&mut file, &mut std::io::sink())?;
    }
    Ok(())
}
