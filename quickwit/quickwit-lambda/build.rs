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
//! 1. Looks for a pre-built Lambda binary at a known location
//! 2. Creates a zip file containing the binary named "bootstrap"
//! 3. Places the zip in OUT_DIR for embedding via include_bytes!
//!
//! The Lambda binary should be pre-built in CI for the aarch64-unknown-linux-musl
//! target and placed in the expected location.

fn main() {
    #[cfg(feature = "auto-deploy")]
    auto_deploy_build();

    #[cfg(not(feature = "auto-deploy"))]
    println!("cargo:rerun-if-changed=build.rs");
}

#[cfg(feature = "auto-deploy")]
fn auto_deploy_build() {
    use std::env;
    use std::path::PathBuf;

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=QUICKWIT_LAMBDA_BINARY_PATH");

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    let zip_path = out_dir.join("lambda_bootstrap.zip");

    // Look for the pre-built Lambda binary in order of preference:
    // 1. Environment variable QUICKWIT_LAMBDA_BINARY_PATH
    // 2. Target directory (for local builds with cross-compilation)
    // 3. Fallback to creating a placeholder for development

    let binary_path = if let Ok(path) = env::var("QUICKWIT_LAMBDA_BINARY_PATH") {
        println!(
            "cargo:warning=Using Lambda binary from QUICKWIT_LAMBDA_BINARY_PATH: {}",
            path
        );
        Some(PathBuf::from(path))
    } else {
        // Try to find in target directory
        let workspace_root = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();

        let potential_paths = [
            workspace_root
                .join("target/aarch64-unknown-linux-musl/release/quickwit-lambda-leaf-search"),
            workspace_root.join("target/lambda/quickwit-lambda-leaf-search/bootstrap"),
            workspace_root.join("lambda_bootstrap"),
        ];

        potential_paths.into_iter().find(|p| p.exists())
    };

    match binary_path {
        Some(path) => {
            println!("cargo:warning=Packaging Lambda binary from: {:?}", path);
            create_lambda_zip(&path, &zip_path);
        }
        None => {
            // Create a placeholder zip for development builds
            // This allows compilation to succeed, but deploy() will fail at runtime
            // if someone tries to use auto-deploy without a proper binary
            println!("cargo:warning=No Lambda binary found, creating placeholder zip");
            println!("cargo:warning=Set QUICKWIT_LAMBDA_BINARY_PATH or build the binary first");
            create_placeholder_zip(&zip_path);
        }
    }
}

#[cfg(feature = "auto-deploy")]
fn create_lambda_zip(binary_path: &std::path::Path, zip_path: &std::path::Path) {
    use std::fs::File;
    use std::io::Read;

    use zip::ZipWriter;
    use zip::write::FileOptions;

    let mut binary_data = Vec::new();
    File::open(binary_path)
        .expect("Failed to open Lambda binary")
        .read_to_end(&mut binary_data)
        .expect("Failed to read Lambda binary");

    let zip_file = File::create(zip_path).expect("Failed to create zip file");
    let mut zip = ZipWriter::new(zip_file);

    // Lambda requires the binary to be named "bootstrap" with executable permissions
    let options = FileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated)
        .unix_permissions(0o755);

    zip.start_file("bootstrap", options)
        .expect("Failed to start zip file entry");

    std::io::Write::write_all(&mut zip, &binary_data).expect("Failed to write binary to zip");

    zip.finish().expect("Failed to finalize zip file");

    println!(
        "cargo:warning=Created Lambda zip at {:?} ({} bytes)",
        zip_path,
        std::fs::metadata(zip_path).unwrap().len()
    );
}

#[cfg(feature = "auto-deploy")]
fn create_placeholder_zip(zip_path: &std::path::Path) {
    use std::fs::File;

    use zip::ZipWriter;
    use zip::write::FileOptions;

    let zip_file = File::create(zip_path).expect("Failed to create placeholder zip file");
    let mut zip = ZipWriter::new(zip_file);

    // Create a placeholder script that returns an error
    let placeholder_script = r#"#!/bin/sh
echo "ERROR: This is a placeholder Lambda binary."
echo "The auto-deploy feature requires a properly built Lambda binary."
echo "Please build the quickwit-lambda-leaf-search binary for aarch64-unknown-linux-musl"
echo "and set QUICKWIT_LAMBDA_BINARY_PATH environment variable."
exit 1
"#;

    let options = FileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated)
        .unix_permissions(0o755);

    zip.start_file("bootstrap", options)
        .expect("Failed to start zip file entry");

    std::io::Write::write_all(&mut zip, placeholder_script.as_bytes())
        .expect("Failed to write placeholder to zip");

    zip.finish().expect("Failed to finalize zip file");
}
