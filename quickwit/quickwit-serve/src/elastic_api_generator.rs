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

use std::{env, path};

use clap::Command;
use once_cell::sync::Lazy;
use quickwit_elastic_api_generation::{download_artifacts, generate_api};

pub const ES_ARTIFACTS_DIR_NAME: &str = "elastic-search-artifacts";
pub const GENERATED_FILE_NAME: &str = "api_specs.rs";
const GENERATED_FILE_HEADER: &str = r#"
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

/// This file is auto-generated, any change can be overridden.
"#;

pub static ROOT_DIR: Lazy<path::PathBuf> = Lazy::new(|| {
    let mf = env::var("CARGO_MANIFEST_DIR").expect("Should be run using 'cargo run ...'");
    path::Path::new(&mf).parent().unwrap().to_path_buf()
});

// Elasticsearch stack versions https://artifacts-api.elastic.co/v1/versions
pub static ES_STACK_VERSION: Lazy<String> =
    Lazy::new(|| env::var("ES_STACK_VERSION").unwrap_or_else(|_| "8.6.0".to_string()));

/// List of spec files for supported endpoint.
pub const SELECTED_SPEC_FILES: &[&str] = &[
    "search.json",
    // Add spec file for endpoints you want to support.
];

fn main() -> anyhow::Result<()> {
    let command_opt = Command::new("elastic-api-generator")
        .about("Quickwit Elasticsearch compatible API generator CLI")
        .subcommand_required(true)
        .subcommand(Command::new("download").about("Downloads the Elasticsearch API spec files."))
        .subcommand(Command::new("generate").about("Generates the Elasticsearch API types."));

    let matches = command_opt.get_matches();
    let artifact_download_dir = ROOT_DIR.join(ES_ARTIFACTS_DIR_NAME);

    match matches.subcommand() {
        Some(("download", _)) => {
            download_artifacts(artifact_download_dir.as_path(), &ES_STACK_VERSION)
        }
        Some(("generate", _)) => {
            let api_spec_dir = artifact_download_dir
                .join(&*ES_STACK_VERSION)
                .join("rest-api-spec/api");

            let target_file = ROOT_DIR
                .join("quickwit-serve/src/elastic_search_api")
                .join(GENERATED_FILE_NAME);

            generate_api(
                &api_spec_dir,
                SELECTED_SPEC_FILES,
                &target_file,
                GENERATED_FILE_HEADER,
            )
        }
        _ => unreachable!(),
    }
}
