## Elastic Compatible API

To help implement Elasticsearch compatible API in Quickwit, we generate endpoints and related types using the `elastic-api-generator`.

To update the generated file, you will have to:
- Download the spec files from the official Elasticsearch repository by running `cargo run --bin elastic-api-generator download` command.
- Include the endpoint spec file if you want to support new endpoints (optional).
- Generate the corresponding rust code by running `cargo run --bin elastic-api-generator generate` command.

The generated code is located at `quickwit-serve/src/elastic_search_api/api_specs.rs`. `quickwit-serve/src/elastic_search_api/rest_handler.rs` is then used to create the warp endpoint handlers by using the generated warp filters.
