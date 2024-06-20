// Copyright (C) 2024 Quickwit, Inc.
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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

use quickwit_common::thread_pool::run_cpu_intensive;
use quickwit_config::{build_doc_mapper, DocMapping, SearchSettings};
use quickwit_doc_mapper::DocMapper;
use quickwit_proto::ingest::{
    DocBatchV2, IngestV2Error, IngestV2Result, ParseFailure, ParseFailureReason,
};
use quickwit_proto::types::DocMappingUid;
use serde_json::Value as JsonValue;
use tracing::info;

/// Attempts to get the doc mapper identified by the given doc mapping UID `doc_mapping_uid` from
/// the `doc_mappers` cache. If it is not found, it is built from the specified JSON doc mapping
/// `doc_mapping_json` and inserted into the cache before being returned.
pub(super) fn get_or_try_build_doc_mapper(
    doc_mappers: &mut HashMap<DocMappingUid, Weak<dyn DocMapper>>,
    doc_mapping_uid: DocMappingUid,
    doc_mapping_json: &str,
) -> IngestV2Result<Arc<dyn DocMapper>> {
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
pub(super) fn try_build_doc_mapper(doc_mapping_json: &str) -> IngestV2Result<Arc<dyn DocMapper>> {
    let doc_mapping: DocMapping = serde_json::from_str(doc_mapping_json).map_err(|error| {
        IngestV2Error::Internal(format!("failed to parse doc mapping: {error}"))
    })?;
    let search_settings = SearchSettings::default();
    let doc_mapper = build_doc_mapper(&doc_mapping, &search_settings)
        .map_err(|error| IngestV2Error::Internal(format!("failed to build doc mapper: {error}")))?;
    Ok(doc_mapper)
}

/// Parses the JSON documents contained in the batch and applies the doc mapper. Returns the
/// original batch and a list of parse failures.
pub(super) async fn validate_doc_batch(
    doc_batch: DocBatchV2,
    doc_mapper: Arc<dyn DocMapper>,
) -> IngestV2Result<(DocBatchV2, Vec<ParseFailure>)> {
    let validate_doc_batch_fn = move || {
        let mut parse_failures: Vec<ParseFailure> = Vec::new();

        for (doc_uid, doc) in doc_batch.docs() {
            let Ok(json_doc) = serde_json::from_slice::<JsonValue>(&doc) else {
                let parse_failure = ParseFailure {
                    doc_uid: Some(doc_uid),
                    reason: ParseFailureReason::InvalidJson as i32,
                    message: "failed to parse JSON document".to_string(),
                };
                parse_failures.push(parse_failure);
                continue;
            };
            let JsonValue::Object(json_obj) = json_doc else {
                let parse_failure = ParseFailure {
                    doc_uid: Some(doc_uid),
                    reason: ParseFailureReason::InvalidJson as i32,
                    message: "JSON document is not an object".to_string(),
                };
                parse_failures.push(parse_failure);
                continue;
            };
            if let Err(error) = doc_mapper.doc_from_json_obj(json_obj, doc.len() as u64) {
                let parse_failure = ParseFailure {
                    doc_uid: Some(doc_uid),
                    reason: ParseFailureReason::InvalidSchema as i32,
                    message: error.to_string(),
                };
                parse_failures.push(parse_failure);
            }
        }
        (doc_batch, parse_failures)
    };
    run_cpu_intensive(validate_doc_batch_fn)
        .await
        .map_err(|error| {
            let message = format!("failed to validate documents: {error}");
            IngestV2Error::Internal(message)
        })
}

#[cfg(test)]
mod tests {
    use quickwit_proto::types::DocUid;

    use super::*;

    #[test]
    fn test_get_or_try_build_doc_mapper() {
        let mut doc_mappers: HashMap<DocMappingUid, Weak<dyn DocMapper>> = HashMap::new();

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
        assert!(doc_mappers
            .get(&doc_mapping_uid)
            .unwrap()
            .upgrade()
            .is_none());

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

    #[tokio::test]
    async fn test_validate_doc_batch() {
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

        let (_, parse_failures) = validate_doc_batch(doc_batch, doc_mapper.clone())
            .await
            .unwrap();
        assert_eq!(parse_failures.len(), 0);

        let doc_batch =
            DocBatchV2::for_test(["", "[]", r#"{"foo": "bar"}"#, r#"{"doc": "test-doc-000"}"#]);
        let (_, parse_failures) = validate_doc_batch(doc_batch, doc_mapper).await.unwrap();
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
    }
}
