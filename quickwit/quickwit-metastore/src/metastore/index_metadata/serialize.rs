// Copyright (C) 2023 Quickwit, Inc.
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

use std::collections::HashMap;

use quickwit_config::{IndexConfig, SourceConfig};
use quickwit_proto::types::IndexUid;
use serde::{self, Deserialize, Serialize};

use crate::checkpoint::IndexCheckpoint;
use crate::split_metadata::utc_now_timestamp;
use crate::IndexMetadata;

#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "version")]
pub(crate) enum VersionedIndexMetadata {
    #[serde(rename = "0.6")]
    // Retro compatibility.
    #[serde(alias = "0.5")]
    #[serde(alias = "0.4")]
    V0_6(IndexMetadataV0_6),
}

impl From<IndexMetadata> for VersionedIndexMetadata {
    fn from(index_metadata: IndexMetadata) -> Self {
        VersionedIndexMetadata::V0_6(index_metadata.into())
    }
}

impl TryFrom<VersionedIndexMetadata> for IndexMetadata {
    type Error = anyhow::Error;

    fn try_from(index_metadata: VersionedIndexMetadata) -> anyhow::Result<Self> {
        match index_metadata {
            // When we have more than one version, you should chain version conversion.
            // ie. Implement conversion from V_k -> V_{k+1}
            VersionedIndexMetadata::V0_6(v6) => v6.try_into(),
        }
    }
}

impl From<IndexMetadata> for IndexMetadataV0_6 {
    fn from(index_metadata: IndexMetadata) -> Self {
        let sources: Vec<SourceConfig> = index_metadata.sources.values().cloned().collect();
        Self {
            index_uid: index_metadata.index_uid,
            index_config: index_metadata.index_config,
            checkpoint: index_metadata.checkpoint,
            create_timestamp: index_metadata.create_timestamp,
            sources,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub(crate) struct IndexMetadataV0_6 {
    #[schema(value_type = String)]
    // Defaults to nil for backward compatibility.
    #[serde(default, alias = "index_id")]
    pub index_uid: IndexUid,
    #[schema(value_type = VersionedIndexConfig)]
    pub index_config: IndexConfig,
    #[schema(value_type = Object)]
    pub checkpoint: IndexCheckpoint,
    #[serde(default = "utc_now_timestamp")]
    pub create_timestamp: i64,
    #[schema(value_type = Vec<VersionedSourceConfig>)]
    pub sources: Vec<SourceConfig>,
}

impl TryFrom<IndexMetadataV0_6> for IndexMetadata {
    type Error = anyhow::Error;

    fn try_from(v0_6: IndexMetadataV0_6) -> anyhow::Result<Self> {
        let mut sources: HashMap<String, SourceConfig> = Default::default();
        for source in v0_6.sources {
            if sources.contains_key(&source.source_id) {
                anyhow::bail!("source `{}` is defined more than once", source.source_id);
            }
            sources.insert(source.source_id.clone(), source);
        }
        Ok(Self {
            index_uid: if v0_6.index_uid.is_empty() {
                v0_6.index_config.index_id.clone().into()
            } else {
                v0_6.index_uid
            },
            index_config: v0_6.index_config,
            checkpoint: v0_6.checkpoint,
            create_timestamp: v0_6.create_timestamp,
            sources,
        })
    }
}
