mod api_generator;
mod api_spec_artifacts;

use std::{env, path};

pub use api_generator::generate_api;
pub use api_spec_artifacts::download_artifacts;
use once_cell::sync::Lazy;

pub static ROOT_DIR: Lazy<path::PathBuf> = Lazy::new(|| {
    let mf = env::var("CARGO_MANIFEST_DIR").expect("Should be run using 'cargo xtask ...'");
    path::Path::new(&mf).parent().unwrap().to_owned()
});

// Elastic stack versions https://artifacts-api.elastic.co/v1/versions
pub static ES_STACK_VERSION: Lazy<String> =
    Lazy::new(|| env::var("ES_STACK_VERSION").unwrap_or("8.6.0".to_string()));

pub const ES_ARTIFACTS_DIR_NAME: &'static str = "elastic-search-artifacts";

pub const SELECTED_SPEC_FILES: &'static [&str] = &["search"];

pub const GENERATED_FILE_NAME: &'static str = "api_specs.rs";
