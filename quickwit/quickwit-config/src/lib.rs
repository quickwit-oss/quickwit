// Copyright (C) 2022 Quickwit, Inc.
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

mod config;
mod index_config;
pub mod merge_policy_config;
pub mod service;
mod source_config;
mod templating;

pub use config::{IndexerConfig, QuickwitConfig, SearcherConfig, DEFAULT_QW_CONFIG_PATH};
pub use index_config::{
    build_doc_mapper, DocMapping, IndexConfig, IndexingResources, IndexingSettings,
    IndexingSettingsLegacy, RetentionPolicy, RetentionPolicyCutoffReference, SearchSettings,
};
pub use source_config::{
    ingest_api_default_source_config, FileSourceParams, IngestApiSourceParams, KafkaSourceParams,
    KinesisSourceParams, RegionOrEndpoint, SourceConfig, SourceParams, VecSourceParams,
    VoidSourceParams, CLI_INGEST_SOURCE_ID, INGEST_API_SOURCE_ID,
};

fn is_false(val: &bool) -> bool {
    !*val
}
