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

//! Build script for quickwit-lambda.
//!
//! When the `auto-deploy` feature is enabled, this script:
//! 1. Downloads the pre-built Lambda zip from a GitHub release
//! 2. Places the zip in OUT_DIR for embedding via include_bytes!
//!
//! The Lambda binary is built separately in CI and published as a GitHub release.

fn main() {
    #[cfg(feature = "auto-deploy")]
    auto_deploy_build();

    #[cfg(not(feature = "auto-deploy"))]
    println!("cargo:rerun-if-changed=build.rs");
}

/// URL to download the pre-built Lambda zip from GitHub releases.
/// This should be updated when a new Lambda binary is released.
#[cfg(feature = "auto-deploy")]
const LAMBDA_ZIP_URL: &str =
    "https://github.com/quickwit-oss/quickwit/releases/download/lambda-c07a00b0/quickwit-aws-lambda--aarch64.zip";

/// AWS Lambda direct upload limit is 50MB.
/// Larger artifacts must be uploaded via S3.
#[cfg(feature = "auto-deploy")]
const MAX_LAMBDA_ZIP_SIZE: usize = 50 * 1024 * 1024;

#[cfg(feature = "auto-deploy")]
fn auto_deploy_build() {
    use std::env;
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=QUICKWIT_LAMBDA_ZIP_URL");

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    let zip_path = out_dir.join("lambda_bootstrap.zip");

    // Allow overriding the URL via environment variable
    let url = env::var("QUICKWIT_LAMBDA_ZIP_URL").unwrap_or_else(|_| LAMBDA_ZIP_URL.to_string());

    println!("cargo:warning=Downloading Lambda zip from: {}", url);

    match download_lambda_zip(&url) {
        Ok(data) => {
            let mut file = File::create(&zip_path).expect("Failed to create zip file");
            file.write_all(&data).expect("Failed to write zip file");
            println!(
                "cargo:warning=Downloaded Lambda zip to {:?} ({} bytes)",
                zip_path,
                data.len()
            );
        }
        Err(e) => {
            panic!("Failed to download Lambda zip: {}", e);
        }
    }
}

#[cfg(feature = "auto-deploy")]
fn download_lambda_zip(url: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let response = ureq::get(url).call()?;
    // Set limit higher than MAX_LAMBDA_ZIP_SIZE so we can provide a better error message
    let data = response
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
    Ok(data)
}
