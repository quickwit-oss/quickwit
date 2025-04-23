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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};

use once_cell::sync::OnceCell;
use quickwit_common::rate_limited_error;
use quickwit_common::thread_pool::run_cpu_intensive;
use quickwit_config::{DocMapping, SearchSettings, build_doc_mapper};
use quickwit_doc_mapper::DocMapper;
use quickwit_proto::ingest::{
    DocBatchV2, IngestV2Error, IngestV2Result, ParseFailure, ParseFailureReason,
};
use quickwit_proto::types::{DocMappingUid, DocUid};
use serde_json_borrow::Value as JsonValue;
use tracing::info;

use crate::DocBatchV2Builder;

/// Attempts to get the doc mapper identified by the given doc mapping UID `doc_mapping_uid` from
/// the `doc_mappers` cache. If it is not found, it is built from the specified JSON doc mapping
/// `doc_mapping_json` and inserted into the cache before being returned.
pub(super) fn get_or_try_build_doc_mapper(
    doc_mappers: &mut HashMap<DocMappingUid, Weak<DocMapper>>,
    doc_mapping_uid: DocMappingUid,
    doc_mapping_json: &str,
) -> IngestV2Result<Arc<DocMapper>> {
    if let Entry::Occupied(occupied) = doc_mappers.entry(doc_mapping_uid) {
        if let Some(doc_mapper) = occupied.get().upgrade() {
            return Ok(doc_mapper);
        }
        occupied.remove();
    }
    let doc_mapper = try_build_doc_mapper(doc_mapping_json)?;

    if doc_mapper.doc_mapping_uid() != doc_mapping_uid {
        let message = format!(
            "doc mapping UID mismatch: expected `{doc_mapping_uid}`, got `{}`",
            doc_mapper.doc_mapping_uid()
        );
        return Err(IngestV2Error::Internal(message));
    }
    doc_mappers.insert(doc_mapping_uid, Arc::downgrade(&doc_mapper));
    info!("inserted doc mapper `{doc_mapping_uid}` into cache`");

    Ok(doc_mapper)
}

/// Attempts to build a doc mapper from the specified JSON doc mapping `doc_mapping_json`.
pub(super) fn try_build_doc_mapper(doc_mapping_json: &str) -> IngestV2Result<Arc<DocMapper>> {
    let doc_mapping: DocMapping = serde_json::from_str(doc_mapping_json).map_err(|error| {
        IngestV2Error::Internal(format!("failed to parse doc mapping: {error}"))
    })?;
    let search_settings = SearchSettings::default();
    let doc_mapper = build_doc_mapper(&doc_mapping, &search_settings)
        .map_err(|error| IngestV2Error::Internal(format!("failed to build doc mapper: {error}")))?;
    Ok(doc_mapper)
}

fn validate_document(
    doc_mapper: &DocMapper,
    doc_bytes: &[u8],
) -> Result<(), (ParseFailureReason, String)> {
    let Ok(json_doc) = serde_json::from_slice::<serde_json_borrow::Value>(doc_bytes) else {
        return Err((
            ParseFailureReason::InvalidJson,
            "failed to parse JSON document".to_string(),
        ));
    };
    let JsonValue::Object(json_obj) = json_doc else {
        return Err((
            ParseFailureReason::InvalidJson,
            "JSON document is not an object".to_string(),
        ));
    };
    if let Err(error) = doc_mapper.validate_json_obj(&json_obj) {
        rate_limited_error!(
            limit_per_min = 6,
            "failed to validate JSON document: {}",
            error
        );
        return Err((ParseFailureReason::InvalidSchema, error.to_string()));
    }
    Ok(())
}

/// Validates a batch of docs.
///
/// Returns a batch of valid docs and the list of errors.
fn validate_doc_batch_impl(
    doc_batch: DocBatchV2,
    doc_mapper: &DocMapper,
) -> (DocBatchV2, Vec<ParseFailure>) {
    let mut parse_failures: Vec<ParseFailure> = Vec::new();
    let mut invalid_doc_ids: HashSet<DocUid> = HashSet::default();
    for (doc_uid, doc_bytes) in doc_batch.docs() {
        if let Err((reason, message)) = validate_document(doc_mapper, &doc_bytes) {
            let parse_failure = ParseFailure {
                doc_uid: Some(doc_uid),
                reason: reason as i32,
                message,
            };
            invalid_doc_ids.insert(doc_uid);
            parse_failures.push(parse_failure);
        }
    }
    if invalid_doc_ids.is_empty() {
        // All docs are valid! We don't need to build a valid doc batch.
        return (doc_batch, parse_failures);
    }
    let mut valid_doc_batch_builder = DocBatchV2Builder::default();
    for (doc_uid, doc_bytes) in doc_batch.docs() {
        if !invalid_doc_ids.contains(&doc_uid) {
            valid_doc_batch_builder.add_doc(doc_uid, &doc_bytes);
        }
    }
    let valid_doc_batch: DocBatchV2 = valid_doc_batch_builder.build().unwrap_or_default();
    assert_eq!(
        valid_doc_batch.num_docs() + parse_failures.len(),
        doc_batch.num_docs()
    );
    (valid_doc_batch, parse_failures)
}

fn is_document_validation_enabled() -> bool {
    static IS_DOCUMENT_VALIDATION_ENABLED: OnceCell<bool> = OnceCell::new();
    *IS_DOCUMENT_VALIDATION_ENABLED.get_or_init(|| {
        !quickwit_common::get_bool_from_env("QW_DISABLE_DOCUMENT_VALIDATION", false)
    })
}

/// Parses the JSON documents contained in the batch and applies the doc mapper. Returns the
/// original batch and a list of parse failures.
pub(super) async fn validate_doc_batch(
    doc_batch: DocBatchV2,
    doc_mapper: Arc<DocMapper>,
) -> IngestV2Result<(DocBatchV2, Vec<ParseFailure>)> {
    if is_document_validation_enabled() {
        run_cpu_intensive(move || validate_doc_batch_impl(doc_batch, &doc_mapper))
            .await
            .map_err(|error| {
                let message = format!("failed to validate documents: {error}");
                IngestV2Error::Internal(message)
            })
    } else {
        Ok((doc_batch, Vec::new()))
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::types::DocUid;

    use super::*;

    #[test]
    fn test_get_or_try_build_doc_mapper() {
        let mut doc_mappers: HashMap<DocMappingUid, Weak<DocMapper>> = HashMap::new();

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = r#"{
            "field_mappings": [{
                "name": "message",
                "type": "text"
            }]
        }"#;
        let error =
            get_or_try_build_doc_mapper(&mut doc_mappers, doc_mapping_uid, doc_mapping_json)
                .unwrap_err();
        assert!(
            matches!(error, IngestV2Error::Internal(message) if message.contains("doc mapping UID mismatch"))
        );

        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}",
                "field_mappings": [{{
                        "name": "message",
                        "type": "text"
                }}]
            }}"#
        );
        let doc_mapper =
            get_or_try_build_doc_mapper(&mut doc_mappers, doc_mapping_uid, &doc_mapping_json)
                .unwrap();
        assert_eq!(doc_mappers.len(), 1);
        assert_eq!(doc_mapper.doc_mapping_uid(), doc_mapping_uid);
        assert_eq!(Arc::strong_count(&doc_mapper), 1);

        drop(doc_mapper);
        assert!(
            doc_mappers
                .get(&doc_mapping_uid)
                .unwrap()
                .upgrade()
                .is_none()
        );

        let error = get_or_try_build_doc_mapper(&mut doc_mappers, doc_mapping_uid, "").unwrap_err();
        assert!(
            matches!(error, IngestV2Error::Internal(message) if message.contains("parse doc mapping"))
        );
        assert_eq!(doc_mappers.len(), 0);
    }

    #[test]
    fn test_try_build_doc_mapper() {
        let error = try_build_doc_mapper("").unwrap_err();
        assert!(
            matches!(error, IngestV2Error::Internal(message) if message.contains("parse doc mapping"))
        );

        let error = try_build_doc_mapper(r#"{"timestamp_field": ".timestamp"}"#).unwrap_err();
        assert!(
            matches!(error, IngestV2Error::Internal(message) if message.contains("build doc mapper"))
        );

        let doc_mapping_json = r#"{
            "mode": "strict",
            "field_mappings": [{
                "name": "message",
                "type": "text"
        }]}"#;
        let doc_mapper = try_build_doc_mapper(doc_mapping_json).unwrap();
        let schema = doc_mapper.schema();
        assert_eq!(schema.num_fields(), 2);

        let contains_message_field = schema
            .fields()
            .map(|(_field, entry)| entry.name())
            .any(|field_name| field_name == "message");
        assert!(contains_message_field);
    }

    #[test]
    fn test_validate_doc_batch() {
        let doc_mapping_json = r#"{
            "mode": "strict",
            "field_mappings": [
                {
                    "name": "doc",
                    "type": "text"
                }
            ]
        }"#;
        let doc_mapper = try_build_doc_mapper(doc_mapping_json).unwrap();
        let doc_batch = DocBatchV2::default();

        let (_, parse_failures) = validate_doc_batch_impl(doc_batch, &doc_mapper);
        assert_eq!(parse_failures.len(), 0);

        let doc_batch =
            DocBatchV2::for_test(["", "[]", r#"{"foo": "bar"}"#, r#"{"doc": "test-doc-000"}"#]);
        let (doc_batch, parse_failures) = validate_doc_batch_impl(doc_batch, &doc_mapper);
        assert_eq!(parse_failures.len(), 3);

        let parse_failure_0 = &parse_failures[0];
        assert_eq!(parse_failure_0.doc_uid(), DocUid::for_test(0));
        assert_eq!(parse_failure_0.reason(), ParseFailureReason::InvalidJson);
        assert!(parse_failure_0.message.contains("parse JSON document"));

        let parse_failure_1 = &parse_failures[1];
        assert_eq!(parse_failure_1.doc_uid(), DocUid::for_test(1));
        assert_eq!(parse_failure_1.reason(), ParseFailureReason::InvalidJson);
        assert!(parse_failure_1.message.contains("not an object"));

        let parse_failure_2 = &parse_failures[2];
        assert_eq!(parse_failure_2.doc_uid(), DocUid::for_test(2));
        assert_eq!(parse_failure_2.reason(), ParseFailureReason::InvalidSchema);
        assert!(parse_failure_2.message.contains("not declared"));

        assert_eq!(doc_batch.num_docs(), 1);
        assert_eq!(doc_batch.doc_uids[0], DocUid::for_test(3));
        let (valid_doc_uid, valid_doc_bytes) = doc_batch.docs().next().unwrap();
        assert_eq!(valid_doc_uid, DocUid::for_test(3));
        assert_eq!(&valid_doc_bytes, r#"{"doc": "test-doc-000"}"#.as_bytes());
    }
}
