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

pub(crate) const CONFIGURATION_TEMPLATE: &str = r#"
version: 0.8
node_id: lambda-searcher
metastore_uri: s3://${QW_LAMBDA_METASTORE_BUCKET}/${QW_LAMBDA_METASTORE_PREFIX:-index}#polling_interval=${QW_LAMBDA_SEARCHER_METASTORE_POLLING_INTERVAL_SECONDS:-60}s
default_index_root_uri: s3://${QW_LAMBDA_INDEX_BUCKET}/${QW_LAMBDA_INDEX_PREFIX:-index}
data_dir: /tmp
searcher:
  partial_request_cache_capacity: ${QW_LAMBDA_PARTIAL_REQUEST_CACHE_CAPACITY:-64M}
"#;

#[cfg(test)]
mod tests {

    use bytesize::ByteSize;
    use quickwit_config::{ConfigFormat, NodeConfig};

    use super::*;

    #[tokio::test]
    #[serial_test::file_serial(with_env)]
    async fn test_load_config() {
        // SAFETY: this test may not be entirely sound if not run with nextest or --test-threads=1
        // as this is only a test, and it would be extremly inconvenient to run it in a different
        // way, we are keeping it that way
        // file_serial may not be enough, given other tests not ran serially could read env

        let bucket = "mock-test-bucket";
        unsafe {
            std::env::set_var("QW_LAMBDA_METASTORE_BUCKET", bucket);
            std::env::set_var("QW_LAMBDA_INDEX_BUCKET", bucket);
            std::env::set_var(
                "QW_LAMBDA_INDEX_CONFIG_URI",
                "s3://mock-index-config-bucket",
            );
            std::env::set_var("QW_LAMBDA_INDEX_ID", "lambda-test");
        }

        let node_config = NodeConfig::load(ConfigFormat::Yaml, CONFIGURATION_TEMPLATE.as_bytes())
            .await
            .unwrap();
        assert_eq!(
            node_config.data_dir_path.to_string_lossy(),
            "/tmp",
            "only `/tmp` is writeable in AWS Lambda"
        );
        assert_eq!(
            node_config.default_index_root_uri,
            "s3://mock-test-bucket/index"
        );
        assert_eq!(
            node_config.metastore_uri.to_string(),
            "s3://mock-test-bucket/index#polling_interval=60s"
        );
        assert_eq!(
            node_config.searcher_config.partial_request_cache_capacity,
            ByteSize::mb(64)
        );

        unsafe {
            std::env::remove_var("QW_LAMBDA_METASTORE_BUCKET");
            std::env::remove_var("QW_LAMBDA_INDEX_BUCKET");
            std::env::remove_var("QW_LAMBDA_INDEX_CONFIG_URI");
            std::env::remove_var("QW_LAMBDA_INDEX_ID");
        }
    }
}
