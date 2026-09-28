// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use quickwit_proto::search::SortOrder;
use serde_json::{Value, json};

use super::{
    DocMapping, IndexConfig, IndexingSettings, IndexingSortField, SearchSettings,
    validate_index_config,
};

fn validate_sort_field(field: Value, field_name: &str) -> anyhow::Result<()> {
    let mapping: DocMapping = serde_json::from_value(json!({"field_mappings": [field]}))?;
    let settings = IndexingSettings {
        sort_fields: vec![IndexingSortField {
            field: field_name.to_string(),
            order: Default::default(),
        }],
        ..Default::default()
    };
    validate_index_config(&mapping, &settings, &SearchSettings::default(), &None)
}

#[test]
fn test_sort_field_validation() {
    let valid = json!({"name": "service", "type": "text", "tokenizer": "raw", "fast": true});
    validate_sort_field(valid.clone(), "service").unwrap();
    assert!(validate_sort_field(valid.clone(), "unknown").is_err());
    assert!(validate_sort_field(valid.clone(), "").is_err());

    for (key, value) in [
        ("type", json!("array<text>")),
        ("tokenizer", json!("default")),
    ] {
        let mut invalid = valid.clone();
        invalid[key] = value;
        assert!(validate_sort_field(invalid, "service").is_err(), "{key}");
    }
    for (fast, expected_error) in [
        (
            json!(false),
            "sort field `service` must be a fast field; set `fast: true`",
        ),
        (
            json!({"normalizer": "lowercase"}),
            "sort field `service` must use the `raw` fast-field normalizer, got `lowercase`",
        ),
    ] {
        let mut invalid = valid.clone();
        invalid["fast"] = fast;
        assert_eq!(
            validate_sort_field(invalid, "service")
                .unwrap_err()
                .to_string(),
            expected_error
        );
    }
    assert!(
        validate_sort_field(
            json!({"name": "service", "type": "u64", "fast": true}),
            "service"
        )
        .is_err()
    );
    assert!(
        validate_sort_field(
            json!({"name": "service", "type": "text", "indexed": false, "fast": true}),
            "service"
        )
        .is_err()
    );
}

#[test]
fn test_sort_field_nested_and_escaped_names() {
    validate_sort_field(
        json!({"name": "resource", "type": "object", "field_mappings": [
            {"name": "service", "type": "text", "tokenizer": "raw", "fast": true}
        ]}),
        "resource.service",
    )
    .unwrap();
    validate_sort_field(
        json!({"name": "service.name", "type": "text", "tokenizer": "raw", "fast": true}),
        r"service\.name",
    )
    .unwrap();
}

#[test]
fn test_sort_fields_shorthand() {
    for (expression, field, order, canonical) in [
        ("service", "service", SortOrder::Asc, "service"),
        ("+service", "service", SortOrder::Asc, "service"),
        ("-service", "service", SortOrder::Desc, "-service"),
        (
            "-resource.service",
            "resource.service",
            SortOrder::Desc,
            "-resource.service",
        ),
        (
            r"service\.name",
            r"service\.name",
            SortOrder::Asc,
            r"service\.name",
        ),
    ] {
        let settings: IndexingSettings =
            serde_yaml::from_str(&format!("sort_fields: [{expression}]\n")).unwrap();
        assert_eq!(
            settings.sort_fields,
            vec![IndexingSortField {
                field: field.to_string(),
                order,
            }]
        );
        let serialized = serde_json::to_value(&settings).unwrap();
        assert_eq!(serialized["sort_fields"], json!([canonical]));
        assert_eq!(
            serde_json::from_value::<IndexingSettings>(serialized).unwrap(),
            settings
        );
    }
}

#[test]
fn test_sort_fields_invalid_shorthand() {
    for expression in [
        "",
        "+",
        "-",
        "--service",
        "++service",
        "-+service",
        "+-service",
        " service",
        "service desc",
    ] {
        let error = serde_json::from_value::<IndexingSettings>(json!({
            "sort_fields": [expression]
        }))
        .unwrap_err();
        assert!(
            error.to_string().contains("invalid sort field"),
            "{expression:?}: {error}"
        );
    }
    for value in [
        json!("service"),
        json!([42]),
        json!([null]),
        json!([{"field": "service"}]),
    ] {
        assert!(serde_json::from_value::<IndexingSettings>(json!({"sort_fields": value})).is_err());
    }
}

#[test]
fn test_sort_field_serialization_and_pipeline_fingerprint() {
    let legacy: IndexingSettings = serde_json::from_value(json!({})).unwrap();
    assert!(legacy.sort_fields.is_empty());
    assert!(
        serde_json::to_value(&legacy)
            .unwrap()
            .get("sort_fields")
            .is_none()
    );

    let mut config = IndexConfig::for_test("test-index", "ram://indexes/test-index");
    let original_fingerprint = config.indexing_params_fingerprint();
    config.indexing_settings.sort_fields = vec![IndexingSortField {
        field: "service".to_string(),
        order: Default::default(),
    }];
    assert_ne!(original_fingerprint, config.indexing_params_fingerprint());
    let serialized = serde_json::to_value(&config.indexing_settings).unwrap();
    let deserialized: IndexingSettings = serde_json::from_value(serialized).unwrap();
    assert_eq!(deserialized, config.indexing_settings);

    let ascending_fingerprint = config.indexing_params_fingerprint();
    config.indexing_settings.sort_fields =
        serde_json::from_value::<IndexingSettings>(json!({"sort_fields": ["-service"]}))
            .unwrap()
            .sort_fields;
    assert_ne!(ascending_fingerprint, config.indexing_params_fingerprint());
    config.indexing_settings.sort_fields.clear();
    assert_eq!(original_fingerprint, config.indexing_params_fingerprint());
}

#[test]
fn test_sort_fields_rejects_multiple_fields() {
    let settings: IndexingSettings =
        serde_json::from_value(json!({"sort_fields": ["service", "-host"]})).unwrap();
    let mapping: DocMapping = serde_json::from_value(json!({"field_mappings": [
        {"name": "service", "type": "text", "tokenizer": "raw", "fast": true},
        {"name": "host", "type": "text", "tokenizer": "raw", "fast": true}
    ]}))
    .unwrap();
    let error =
        validate_index_config(&mapping, &settings, &SearchSettings::default(), &None).unwrap_err();
    assert!(error.to_string().contains("at most one field"));
}
