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

// This file is an integration test that assumes an IPFS node (Kubo) is
// reachable on its RPC API. Start one with:
// `docker compose up -d kubo` (from the repository root), or point
// `QW_IPFS_API_ENDPOINT` at an existing node.

#[cfg(all(feature = "integration-testsuite", feature = "ipfs"))]
#[cfg_attr(not(feature = "ci-test"), ignore)]
mod ipfs_storage_test_suite {
    use std::str::FromStr;

    use anyhow::Context;
    use quickwit_common::rand::append_random_suffix;
    use quickwit_common::setup_logging_for_tests;
    use quickwit_common::uri::Uri;
    use quickwit_config::IpfsStorageConfig;
    use quickwit_storage::{IpfsStorage, Storage};

    #[tokio::test]
    async fn ipfs_storage_test_suite() -> anyhow::Result<()> {
        setup_logging_for_tests();

        let root_dir = append_random_suffix("quickwit-integration-tests").to_lowercase();
        let storage_config = IpfsStorageConfig::default();
        let storage_uri = Uri::from_str(&format!("ipfs://{root_dir}"))?;
        let mut ipfs_storage = IpfsStorage::from_uri(&storage_config, &storage_uri)?;
        ipfs_storage
            .check_connectivity()
            .await
            .context("IPFS node unreachable. start one with `docker compose up -d kubo`")?;

        quickwit_storage::storage_test_suite(&mut ipfs_storage).await?;

        let mut ipfs_storage = IpfsStorage::from_uri(
            &storage_config,
            &Uri::from_str(&format!("ipfs://{root_dir}/sub/prefix"))?,
        )?;
        ipfs_storage.check_connectivity().await?;

        quickwit_storage::storage_test_single_part_upload(&mut ipfs_storage)
            .await
            .context("test single-part upload failed")?;

        quickwit_storage::storage_test_multi_part_upload(&mut ipfs_storage)
            .await
            .context("test multi-part (large file) upload failed")?;

        // Every stored file must expose a CID: this is the content address
        // that makes splits pinnable and shareable across the IPFS network.
        let test_path = std::path::Path::new("cid-check.txt");
        ipfs_storage
            .put(test_path, Box::new(b"content-addressed".to_vec()))
            .await?;
        let cid = ipfs_storage.file_cid(test_path).await?;
        assert!(!cid.is_empty());
        ipfs_storage.delete(test_path).await?;
        Ok(())
    }
}
