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
//
//! Lightweight test harness that replaces the full `ClusterSandbox` for
//! DataFusion integration tests.
//!
//! - [`TestSandbox::start`] spins up an in-process file-backed metastore and an unconfigured
//!   `StorageResolver`. No gRPC, no cluster, no CLI startup.
//!
//! Lives here rather than in `quickwit-integration-tests` so this crate's tests
//! do not depend on `quickwit-cli` / `quickwit-serve`.

use std::str::FromStr;

use quickwit_common::uri::Uri;
use quickwit_metastore::MetastoreResolver;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_storage::StorageResolver;
use tempfile::TempDir;

/// In-process metastore + storage harness.
///
/// Holds the tempdir backing the file-backed metastore so it is not dropped
/// before the test finishes. `data_dir` is a separate directory for parquet
/// split files the test writes via `publish_split`.
pub struct TestSandbox {
    pub metastore: MetastoreServiceClient,
    pub storage_resolver: StorageResolver,
    pub data_dir: TempDir,
    _metastore_dir: TempDir,
}

impl TestSandbox {
    pub async fn start() -> Self {
        // SAFETY: tests are single-threaded by default; setting env before the
        // first metastore resolve is fine.
        unsafe { std::env::set_var("QW_DISABLE_TELEMETRY", "1") };

        let metastore_dir = tempfile::tempdir().expect("metastore tempdir");
        let data_dir = tempfile::tempdir().expect("data tempdir");

        let metastore_uri_str = format!("file://{}/metastore", metastore_dir.path().display());
        let metastore_uri = Uri::from_str(&metastore_uri_str).expect("valid metastore uri");

        let metastore = MetastoreResolver::unconfigured()
            .resolve(&metastore_uri)
            .await
            .expect("resolve file-backed metastore");

        Self {
            metastore,
            storage_resolver: StorageResolver::unconfigured(),
            data_dir,
            _metastore_dir: metastore_dir,
        }
    }
}
