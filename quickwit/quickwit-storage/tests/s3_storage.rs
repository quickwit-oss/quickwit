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

// This file is an integration test that assumes that the environment
// makes it po

#[cfg(feature = "integration-testsuite")]
pub mod s3_storage_test_suite {

    use std::path::PathBuf;
    use std::str::FromStr;

    use anyhow::Context;
    use once_cell::sync::OnceCell;
    use quickwit_common::setup_logging_for_tests;
    use quickwit_common::uri::Uri;
    use quickwit_config::S3StorageConfig;
    use quickwit_storage::{MultiPartPolicy, S3CompatibleObjectStorage};
    use tokio::runtime::Runtime;

    // Introducing a common runtime for the unit tests in this file.
    //
    // By default, tokio creates a new runtime, for each unit test.
    // Here, we want to use the singleton `AwsSdkConfig` object.
    // This object packs a smithy connector which itself includes a
    // hyper client pool. A hyper client cannot be used from multiple runtimes.
    fn test_runtime_singleton() -> &'static Runtime {
        static RUNTIME_CACHE: OnceCell<tokio::runtime::Runtime> = OnceCell::new();
        RUNTIME_CACHE.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap()
        })
    }

    async fn run_s3_storage_test_suite(s3_storage_config: S3StorageConfig, bucket_uri: &str) {
        setup_logging_for_tests();

        let storage_uri = Uri::from_str(bucket_uri).unwrap();
        let mut object_storage =
            S3CompatibleObjectStorage::from_uri(&s3_storage_config, &storage_uri)
                .await
                .unwrap();

        quickwit_storage::storage_test_suite(&mut object_storage)
            .await
            .context("S3 storage test suite failed")
            .unwrap();

        let mut object_storage =
            S3CompatibleObjectStorage::from_uri(&s3_storage_config, &storage_uri)
                .await
                .unwrap()
                .with_prefix("test-s3-compatible-storage".to_string());

        quickwit_storage::storage_test_single_part_upload(&mut object_storage)
            .await
            .context("test single-part upload failed")
            .unwrap();

        object_storage.set_policy(MultiPartPolicy {
            target_part_num_bytes: 5 * 1_024 * 1_024, //< the minimum on S3 is 5MB.
            max_num_parts: 10_000,
            multipart_threshold_num_bytes: 10_000_000,
            max_object_num_bytes: 5_000_000_000_000,
            max_concurrent_uploads: 100,
        });

        quickwit_storage::storage_test_multi_part_upload(&mut object_storage)
            .await
            .context("test multipart upload failed")
            .unwrap();
    }

    #[test]
    #[cfg_attr(not(feature = "ci-test"), ignore)]
    fn test_suite_on_s3_storage_path_style_access() {
        use quickwit_common::rand::append_random_suffix;

        let s3_storage_config = S3StorageConfig {
            force_path_style_access: true,
            ..Default::default()
        };
        let bucket_uri =
            append_random_suffix("s3://quickwit-integration-tests/test-path-style-access");
        let test_runtime = test_runtime_singleton();
        test_runtime.block_on(run_s3_storage_test_suite(s3_storage_config, &bucket_uri));
    }

    #[test]
    #[cfg_attr(not(feature = "ci-test"), ignore)]
    fn test_suite_on_s3_storage_virtual_hosted_style_access() {
        use quickwit_common::rand::append_random_suffix;

        let s3_storage_config = S3StorageConfig {
            force_path_style_access: false,
            ..Default::default()
        };
        let bucket_uri = append_random_suffix(
            "s3://quickwit-integration-tests/test-virtual-hosted-style-access",
        );
        let test_runtime = test_runtime_singleton();
        test_runtime.block_on(run_s3_storage_test_suite(s3_storage_config, &bucket_uri));
    }

    #[test]
    #[cfg_attr(not(feature = "ci-test"), ignore)]
    fn test_suite_on_s3_storage_bulk_delete_single_object_delete_api() {
        use std::str::FromStr;

        use anyhow::Context;
        use quickwit_common::rand::append_random_suffix;
        use quickwit_common::uri::Uri;

        let s3_storage_config = S3StorageConfig {
            disable_multi_object_delete: true,
            ..Default::default()
        };
        let bucket_uri = append_random_suffix(
            "s3://quickwit-integration-tests/test-bulk-delete-single-object-delete-api",
        );
        let storage_uri = Uri::from_str(&bucket_uri).unwrap();
        let test_runtime = test_runtime_singleton();
        test_runtime.block_on(async move {
            let mut object_storage =
                S3CompatibleObjectStorage::from_uri(&s3_storage_config, &storage_uri)
                    .await
                    .unwrap();
            quickwit_storage::test_write_and_bulk_delete(&mut object_storage)
                .await
                .context("test bulk delete single-object delete API failed")
                .unwrap();
        });
    }
}
