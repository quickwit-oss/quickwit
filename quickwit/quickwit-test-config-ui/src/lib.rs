mod utils;

use quickwit_config::{
    build_doc_mapper, load_source_config_from_user_config, ConfigFormat, SearchSettings,
};
use wasm_bindgen::prelude::*;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
extern "C" {
    fn alert(s: &str);
}

#[wasm_bindgen]
pub fn greet() {
    alert("Hello, quickwit-test-config-ui!");
}

#[wasm_bindgen]
pub fn add(a: u32, b: u32) -> u32 {
    a + b
}

#[wasm_bindgen]
pub fn validate(conf: &str, source_config: &str, doc_json: &str) -> Result<String, JsValue> {
    validate_inner(conf, source_config, doc_json)
        .map_err(|err| JsValue::from_str(&format!("{:?}", err)))
}

pub fn validate_inner(conf: &str, source_config: &str, doc_json: &str) -> anyhow::Result<String> {
    console_error_panic_hook::set_once();
    let index_config = quickwit_config::load_index_config_from_user_config(
        ConfigFormat::Yaml,
        conf.as_bytes(),
        &quickwit_common::uri::Uri::from_well_formed("ram:///in-memory"),
    )?;

    let mut doc_json = doc_json.to_string();
    if source_config.is_empty() {
        let source_config =
            load_source_config_from_user_config(ConfigFormat::Yaml, source_config.as_bytes())?;
    }

    let doc_mapper = build_doc_mapper(&index_config.doc_mapping, &SearchSettings::default())?;
    let (_partition, doc) = doc_mapper.doc_from_json_str(&doc_json)?;
    let doc = doc_mapper.schema().to_json(&doc);
    Ok(doc)
}
