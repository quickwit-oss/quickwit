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

use std::mem;

use quickwit_common::metrics::MetricsApi;
use quickwit_config::ConfigApiSchemas;
use quickwit_doc_mapper::DocMapperApiSchemas;
use quickwit_indexing::IndexingApiSchemas;
use quickwit_janitor::JanitorApiSchemas;
use quickwit_metastore::MetastoreApiSchemas;
use utoipa::openapi::Tag;
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

    // Tags use for grouping and sorting routes.
    let tags = vec![
        Tag::new("Search"),
        Tag::new("Indexes"),
        Tag::new("Ingest"),
        Tag::new("Delete Tasks"),
        Tag::new("Node Health"),
        Tag::new("Sources"),
        Tag::new("Get Metrics"),
        Tag::new("Cluster Info"),
        Tag::new("Indexing"),
        Tag::new("Splits"),
    ];
    docs_base.tags = Some(tags);

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
    /// Merges a given [OpenApi] schema into another schema.
    fn merge_components_and_paths(&mut self, schema: utoipa::openapi::OpenApi);

    /// Modifies all of the paths for a given OpenAPI instance
    /// and appends the provided prefix to the paths.
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

    fn with_path_prefix(mut self, prefix: &str) -> Self {
        let paths = mem::take(&mut self.paths.paths);
        for (path, detail) in paths {
            // We can panic here as it will be raised during unit tests.
            assert!(
                path.starts_with('/'),
                "Path {path:?} does not start with `/`."
            );

            let adjusted_path = if path != "/" {
                format!("{prefix}{path}")
            } else {
                prefix.to_owned()
            };
            self.paths.paths.insert(adjusted_path, detail);
        }

        self
    }
}

#[cfg(test)]
mod openapi_schema_tests {
    use std::collections::BTreeSet;

    use itertools::Itertools;
    use utoipa::openapi::schema::AdditionalProperties;
    use utoipa::openapi::{RefOr, Schema};

    use super::*;

    #[test]
    fn ensure_schemas_resolve() {
        let docs = build_docs();
        resolve_openapi_schemas(&docs).expect("All schemas should be resolved");
    }

    fn resolve_openapi_schemas(openapi: &utoipa::openapi::OpenApi) -> anyhow::Result<()> {
        let schemas_lookup = if let Some(ref components) = openapi.components {
            resolve_component_schemas(components)?
        } else {
            BTreeSet::new()
        };

        let mut errors = Vec::new();
        for (path, detail) in openapi.paths.paths.iter() {
            let path = path.as_str();
            for (method, operation) in detail.operations.iter() {
                let method = serde_json::to_string(method).unwrap();
                let contents = operation
                    .request_body
                    .as_ref()
                    .map(|v| &v.content)
                    .cloned()
                    .unwrap_or_default();
                for (key, content) in contents {
                    let location = match content.schema {
                        RefOr::Ref(r) => r.ref_location,
                        RefOr::T(_) => continue,
                    };

                    if !schemas_lookup.contains(&location) {
                        let info = format!("key:{key:?}");
                        errors.push((location, method.clone(), path, info));
                    }
                }

                for (status, resp) in operation.responses.responses.iter() {
                    let location = match resp {
                        RefOr::Ref(r) => &r.ref_location,
                        RefOr::T(_) => continue,
                    };

                    if !schemas_lookup.contains(location) {
                        let info = format!("status:{status}");
                        errors.push((location.clone(), method.clone(), path, info));
                    }
                }

                for parameter in operation.parameters.as_deref().unwrap_or(&[]) {
                    let location = match &parameter.schema {
                        Some(RefOr::Ref(r)) => &r.ref_location,
                        Some(RefOr::T(schema)) => {
                            let parent = format!("param: {}", &parameter.name);
                            check_schema(
                                &method,
                                path,
                                &schemas_lookup,
                                &mut errors,
                                &parent,
                                schema,
                            );
                            continue;
                        }
                        _ => continue,
                    };

                    if !schemas_lookup.contains(location) {
                        let info = format!("param:{}", parameter.name);
                        errors.push((location.clone(), method.clone(), path, info));
                    }
                }
            }
        }

        if !errors.is_empty() {
            let errors = errors
                .into_iter()
                .map(|(location, method, path, info)| {
                    format!("{method} {path:?} {info} - Location: {location}")
                })
                .join("\n");

            anyhow::bail!(
                "Failed to resolve schemas, do these types implement `ToSchema`?:\n\n{errors}"
            )
        }

        Ok(())
    }

    /// Builds a lookup set of all of the schemas that can be referenced.
    fn resolve_component_schemas(
        components: &utoipa::openapi::Components,
    ) -> anyhow::Result<BTreeSet<String>> {
        // Loads the core schemas which is used by most references
        // This can have references in and of itself however, so we
        // need to track those to resolve later.
        let mut schema_lookup = BTreeSet::new();
        let mut pending_resolved = Vec::new();
        let mut resolve_once = Vec::new();

        for (schema_item, maybe_ref) in &components.schemas {
            let path = format!("#/components/schemas/{schema_item}");
            match maybe_ref {
                RefOr::Ref(r) => {
                    pending_resolved.push((path, r.ref_location.clone()));
                }
                RefOr::T(schema) => {
                    resolve_schema(&mut resolve_once, schema_item, schema);
                    schema_lookup.insert(path);
                }
            };
        }

        for schema_item in components.security_schemes.keys() {
            let path = format!("#/components/securitySchemes/{schema_item}");
            schema_lookup.insert(path);
        }

        // Although responses aren't technically a schema, they can be referenced and contain
        // references, so it's easier to merge them into one.
        for (schema_item, maybe_ref) in &components.responses {
            let path = format!("#/components/responses/{schema_item}");
            match maybe_ref {
                RefOr::Ref(r) => {
                    pending_resolved.push((path, r.ref_location.clone()));
                }
                RefOr::T(schema) => {
                    for (_, content) in &schema.content {
                        if let RefOr::Ref(r) = &content.schema {
                            if !schema_lookup.contains(&r.ref_location) {
                                resolve_once.push(CheckResolve::new(
                                    r.ref_location.clone(),
                                    schema_item.clone(),
                                ));
                            }
                        }
                    }
                    schema_lookup.insert(path);
                }
            };
        }

        // Walks through the list of references that need to be resolved.
        // Technically a reference can lead to a reference, so if one
        // location is resolved later on, we might then be able to resolve
        // others, hence the loop.
        loop {
            for (path, location) in mem::take(&mut pending_resolved) {
                if schema_lookup.contains(&location) {
                    schema_lookup.insert(path);
                } else {
                    pending_resolved.push((path, location));
                }
            }

            if pending_resolved.is_empty() {
                break;
            }
        }

        let mut failed_to_resolve = Vec::new();
        for resolve in resolve_once {
            if !schema_lookup.contains(&resolve.location) {
                failed_to_resolve.push(resolve);
            }
        }

        if !pending_resolved.is_empty() || !failed_to_resolve.is_empty() {
            let errors_pending = pending_resolved
                .into_iter()
                .map(|(path, _)| format!("{path:?}"))
                .join("\n");
            let errors_resolve_once = failed_to_resolve
                .into_iter()
                .map(|resolve| format!("Struct: {:?} - {:?}", resolve.parent, resolve.location,))
                .join("\n");
            anyhow::bail!(
                "Failed to resolve schemas for OpenAPI \
                 spec:\n{errors_pending}\n{errors_resolve_once}"
            );
        }

        Ok(schema_lookup)
    }

    fn resolve_schema(
        resolve_once: &mut Vec<CheckResolve>,
        parent_location: &str,
        schema: &Schema,
    ) {
        match schema {
            Schema::Array(array) => {
                let parent = format!("{parent_location}.Vec");
                match &*array.items {
                    RefOr::Ref(r) => {
                        resolve_once.push(CheckResolve::new(r.ref_location.clone(), parent))
                    }
                    RefOr::T(schema) => resolve_schema(resolve_once, &parent, schema),
                }
            }
            Schema::Object(object) => {
                for (key, r) in object.properties.iter() {
                    let parent = format!("{parent_location}.{key}");
                    match r {
                        RefOr::Ref(r) => {
                            resolve_once.push(CheckResolve::new(r.ref_location.clone(), parent))
                        }
                        RefOr::T(schema) => resolve_schema(resolve_once, &parent, schema),
                    }
                }

                if let Some(ref props) = object.additional_properties {
                    if let AdditionalProperties::RefOr(ref r) = **props {
                        match r {
                            RefOr::Ref(r) => resolve_once.push(CheckResolve::new(
                                r.ref_location.clone(),
                                parent_location.to_owned(),
                            )),
                            RefOr::T(schema) => {
                                resolve_schema(resolve_once, parent_location, schema)
                            }
                        }
                    }
                }
            }
            Schema::OneOf(one_of) => {
                let parent = format!("{parent_location}.Enum");
                for r in &one_of.items {
                    match r {
                        RefOr::Ref(r) => resolve_once
                            .push(CheckResolve::new(r.ref_location.clone(), parent.clone())),
                        RefOr::T(schema) => resolve_schema(resolve_once, &parent, schema),
                    }
                }
            }
            Schema::AllOf(all_of) => {
                for r in &all_of.items {
                    match r {
                        RefOr::Ref(r) => resolve_once.push(CheckResolve::new(
                            r.ref_location.clone(),
                            parent_location.to_owned(),
                        )),
                        RefOr::T(schema) => resolve_schema(resolve_once, parent_location, schema),
                    }
                }
            }
            _ => unimplemented!("Unknown schema variant"),
        }
    }

    fn check_schema<'a>(
        method: &str,
        path: &'a str,
        schemas_lookup: &BTreeSet<String>,
        errors: &mut Vec<(String, String, &'a str, String)>,
        parent_location: &str,
        schema: &Schema,
    ) {
        match schema {
            Schema::Array(array) => {
                let parent = format!("{parent_location}.Vec");
                match &*array.items {
                    RefOr::Ref(r) => {
                        if !schemas_lookup.contains(&r.ref_location) {
                            errors.push((parent, method.to_string(), path, String::new()));
                        }
                    }
                    RefOr::T(schema) => {
                        check_schema(method, path, schemas_lookup, errors, &parent, schema)
                    }
                }
            }
            Schema::Object(object) => {
                for (key, r) in object.properties.iter() {
                    let parent = format!("{parent_location}.{key}");
                    match r {
                        RefOr::Ref(r) => {
                            if !schemas_lookup.contains(&r.ref_location) {
                                errors.push((parent, method.to_string(), path, String::new()));
                            }
                        }
                        RefOr::T(schema) => {
                            check_schema(method, path, schemas_lookup, errors, &parent, schema)
                        }
                    }
                }

                if let Some(ref props) = object.additional_properties {
                    if let AdditionalProperties::RefOr(ref r) = **props {
                        match r {
                            RefOr::Ref(r) => {
                                if !schemas_lookup.contains(&r.ref_location) {
                                    errors.push((
                                        parent_location.to_string(),
                                        method.to_string(),
                                        path,
                                        String::new(),
                                    ));
                                }
                            }
                            RefOr::T(schema) => check_schema(
                                method,
                                path,
                                schemas_lookup,
                                errors,
                                parent_location,
                                schema,
                            ),
                        }
                    }
                }
            }
            Schema::OneOf(one_of) => {
                let parent = format!("{parent_location}.Enum");
                for r in &one_of.items {
                    match r {
                        RefOr::Ref(r) => {
                            if !schemas_lookup.contains(&r.ref_location) {
                                errors.push((
                                    parent.clone(),
                                    method.to_string(),
                                    path,
                                    String::new(),
                                ));
                            }
                        }
                        RefOr::T(schema) => {
                            check_schema(method, path, schemas_lookup, errors, &parent, schema)
                        }
                    }
                }
            }
            Schema::AllOf(all_of) => {
                for r in &all_of.items {
                    match r {
                        RefOr::Ref(r) => {
                            let (_, type_name) = r.ref_location.rsplit_once('/').unwrap();
                            let parent = format!("{parent_location}.{type_name}");
                            if !schemas_lookup.contains(&r.ref_location) {
                                errors.push((parent, method.to_string(), path, String::new()));
                            }
                        }
                        RefOr::T(schema) => check_schema(
                            method,
                            path,
                            schemas_lookup,
                            errors,
                            parent_location,
                            schema,
                        ),
                    }
                }
            }
            _ => unimplemented!("Unknown schema variant"),
        }
    }

    struct CheckResolve {
        location: String,
        parent: String,
    }

    impl CheckResolve {
        fn new(location: String, parent: String) -> Self {
            Self { location, parent }
        }
    }
}
