// Copyright (C) 2024 Quickwit, Inc.
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
        let bucket = "mock-test-bucket";
        std::env::set_var("QW_LAMBDA_METASTORE_BUCKET", bucket);
        std::env::set_var("QW_LAMBDA_INDEX_BUCKET", bucket);
        std::env::set_var(
            "QW_LAMBDA_INDEX_CONFIG_URI",
            "s3://mock-index-config-bucket",
        );
        std::env::set_var("QW_LAMBDA_INDEX_ID", "lambda-test");

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

        std::env::remove_var("QW_LAMBDA_METASTORE_BUCKET");
        std::env::remove_var("QW_LAMBDA_INDEX_BUCKET");
        std::env::remove_var("QW_LAMBDA_INDEX_CONFIG_URI");
        std::env::remove_var("QW_LAMBDA_INDEX_ID");
    }
}
