use utoipa::OpenApi;

use crate::cluster_api::ClusterApi;
use crate::delete_task_api::DeleteTaskApi;
use crate::health_check_api::HealthCheckApi;
use crate::index_api::IndexApi;
use crate::indexing_api::IndexingApi;
use quickwit_metastore::MetastoreApiSchemas;
use quickwit_config::ConfigApiSchemas;
use quickwit_janitor::JanitorApiSchemas;
use quickwit_doc_mapper::DocMapperApiSchemas;
use crate::ingest_api::IngestApi;

pub fn build_docs() -> utoipa::openapi::OpenApi {
    let mut docs_base = utoipa::openapi::OpenApiBuilder::new()
        .info(utoipa::openapi::InfoBuilder::new()
            .title("Quickwit")
            .version(env!("CARGO_PKG_VERSION"))
            .description(Some(env!("CARGO_PKG_DESCRIPTION")))
            .license(Some(utoipa::openapi::License::new(env!("CARGO_PKG_LICENSE"))))
            .contact(
                Some(utoipa::openapi::ContactBuilder::new()
                    .name(Some("Quickwit, Inc."))
                    .email(Some("hello@quickwit.io")).build()),
            ).build())
        .paths(utoipa::openapi::Paths::new())
        .components(Some(utoipa::openapi::Components::new()))
        .build();

    // Routing
    docs_base.merge(ClusterApi::openapi());
    docs_base.merge(DeleteTaskApi::openapi());
    docs_base.merge(HealthCheckApi::openapi());
    docs_base.merge(IndexApi::openapi());
    docs_base.merge(DocMapperApiSchemas::openapi());
    docs_base.merge(IndexingApi::openapi());
    docs_base.merge(IngestApi::openapi());

    // Schemas
    docs_base.merge(MetastoreApiSchemas::openapi());
    docs_base.merge(ConfigApiSchemas::openapi());
    docs_base.merge(JanitorApiSchemas::openapi());

    docs_base
}

pub trait OpenApiMerger {
    fn merge(&mut self, schema: utoipa::openapi::OpenApi);
}

impl OpenApiMerger for utoipa::openapi::OpenApi {
    fn merge(&mut self, schema: utoipa::openapi::OpenApi) {
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
            components.security_schemes.extend(other_components.security_schemes);
        } else {
            self.components = schema.components;
        }
    }
}