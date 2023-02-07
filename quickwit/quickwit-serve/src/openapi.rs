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

use quickwit_common::metrics::MetricsApi;
use quickwit_config::ConfigApiSchemas;
use quickwit_doc_mapper::DocMapperApiSchemas;
use quickwit_indexing::IndexingApiSchemas;
use quickwit_janitor::JanitorApiSchemas;
use quickwit_metastore::MetastoreApiSchemas;
use utoipa::openapi::Server;
use utoipa::OpenApi;

use crate::cluster_api::ClusterApi;
use crate::delete_task_api::DeleteTaskApi;
use crate::health_check_api::HealthCheckApi;
use crate::index_api::IndexApi;
use crate::indexing_api::IndexingApi;
use crate::ingest_api::{IngestApi, IngestApiSchemas};
use crate::search_api::SearchApi;

/// Builds the OpenApi docs structure using the registered/merged docs.
pub fn build_docs() -> utoipa::openapi::OpenApi {
    let mut docs_base = utoipa::openapi::OpenApiBuilder::new()
        .info(
            utoipa::openapi::InfoBuilder::new()
                .title("Quickwit")
                .version(env!("CARGO_PKG_VERSION"))
                .description(Some(env!("CARGO_PKG_DESCRIPTION")))
                .license(Some(utoipa::openapi::License::new(env!(
                    "CARGO_PKG_LICENSE"
                ))))
                .contact(Some(
                    utoipa::openapi::ContactBuilder::new()
                        .name(Some("Quickwit, Inc."))
                        .email(Some("hello@quickwit.io"))
                        .build(),
                ))
                .build(),
        )
        .paths(utoipa::openapi::Paths::new())
        .components(Some(utoipa::openapi::Components::new()))
        .build();

    // Routing
    docs_base.merge_components_and_paths(HealthCheckApi::openapi().with_path_prefix("/health"));
    docs_base.merge_components_and_paths(MetricsApi::openapi().with_path_prefix("/metrics"));
    docs_base.merge_components_and_paths(ClusterApi::openapi().with_path_prefix("/api/v1"));
    docs_base.merge_components_and_paths(DeleteTaskApi::openapi().with_path_prefix("/api/v1"));
    docs_base.merge_components_and_paths(IndexApi::openapi().with_path_prefix("/api/v1"));
    docs_base.merge_components_and_paths(IndexingApi::openapi().with_path_prefix("/api/v1"));
    docs_base.merge_components_and_paths(IngestApi::openapi().with_path_prefix("/api/v1"));
    docs_base.merge_components_and_paths(SearchApi::openapi().with_path_prefix("/api/v1"));

    // Schemas
    docs_base.merge_components_and_paths(MetastoreApiSchemas::openapi());
    docs_base.merge_components_and_paths(ConfigApiSchemas::openapi());
    docs_base.merge_components_and_paths(JanitorApiSchemas::openapi());
    docs_base.merge_components_and_paths(DocMapperApiSchemas::openapi());
    docs_base.merge_components_and_paths(IndexingApiSchemas::openapi());
    docs_base.merge_components_and_paths(IngestApiSchemas::openapi());

    docs_base
}

pub trait OpenApiMerger {
    fn merge_components_and_paths(&mut self, schema: utoipa::openapi::OpenApi);

    fn with_path_prefix(self, path: &str) -> Self;
}

impl OpenApiMerger for utoipa::openapi::OpenApi {
    fn merge_components_and_paths(&mut self, schema: utoipa::openapi::OpenApi) {
        self.paths.paths.extend(schema.paths.paths);

        if let Some(tags) = &mut self.tags {
            tags.extend(schema.tags.unwrap_or_default());
        } else {
            self.tags = schema.tags;
        }

        if let Some(components) = &mut self.components {
            let other_components = schema.components.unwrap_or_default();

            components.responses.extend(other_components.responses);
            components.schemas.extend(other_components.schemas);
            components
                .security_schemes
                .extend(other_components.security_schemes);
        } else {
            self.components = schema.components;
        }
    }

    fn with_path_prefix(mut self, path: &str) -> Self {
        for details in self.paths.paths.values_mut() {
            details.servers = Some(vec![Server::new(path)]);
        }

        self
    }
}
