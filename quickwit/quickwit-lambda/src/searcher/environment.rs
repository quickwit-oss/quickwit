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
metastore_uri: ${QW_LAMBDA_METASTORE_URI}
default_index_root_uri: s3://${QW_LAMBDA_INDEX_BUCKET}/index
data_dir: /tmp
searcher:
  partial_request_cache_capacity: ${QW_LAMBDA_PARTIAL_REQUEST_CACHE_CAPACITY:-64M}
"#;
