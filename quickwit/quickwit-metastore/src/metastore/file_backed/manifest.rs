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

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use itertools::Itertools;
use quickwit_common::uri::Uri;
use quickwit_config::{IndexTemplate, IndexTemplateId};
use quickwit_proto::metastore::{MetastoreError, MetastoreResult, serde_utils};
use quickwit_proto::types::{DocMappingUid, IndexId};
use quickwit_storage::{OwnedBytes, Storage, StorageError, StorageErrorKind, StorageResult};
use serde::{Deserialize, Serialize};
use tracing::error;
use uuid::Uuid;

pub(super) const MANIFEST_FILE_NAME: &str = "manifest.json";

// The legacy manifest file was deprecated in 0.8.0, we can drop support for it in 0.10.0 or 0.11.0.
const LEGACY_MANIFEST_FILE_NAME: &str = "indexes_states.json";

#[derive(Clone, Debug, Deserialize)]
struct LegacyManifest {
    #[serde(default, flatten)]
    indexes: BTreeMap<IndexId, IndexStatus>,
}

impl LegacyManifest {
    fn into_manifest(self) -> Manifest {
        Manifest {
            indexes: self.indexes,
            templates: HashMap::new(),
            identity: Uuid::nil(),
        }
    }
}

// TODO: Remove the aliases once we drop support for the legacy manifest file.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum IndexStatus {
    #[serde(alias = "Creating")]
    Creating,
    #[serde(alias = "Alive")]
    Active,
    #[serde(alias = "Deleting")]
    Deleting,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(into = "VersionedManifest")]
#[serde(from = "VersionedManifest")]
pub(crate) struct Manifest {
    pub indexes: BTreeMap<IndexId, IndexStatus>,
    // The templates are serialized as a sorted `Vec<IndexTemplate>` so the btree map is
    // unnecessary here and we can pass the hash map as is to the `MetastoreState`
    pub templates: HashMap<IndexTemplateId, IndexTemplate>,
    pub identity: Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "version")]
enum VersionedManifest {
    // The two versions use the same format but for v0.8 and below, we need to set the
    // `doc_mapping_uid` to the nil value upon deserialization.
    #[serde(rename = "0.9")]
    V0_9(ManifestV0_8),
    #[serde(alias = "0.8")]
    #[serde(alias = "0.7")]
    V0_8(ManifestV0_8),
}

impl From<Manifest> for VersionedManifest {
    fn from(manifest: Manifest) -> Self {
        VersionedManifest::V0_9(manifest.into())
    }
}

impl From<VersionedManifest> for Manifest {
    fn from(versioned_manifest: VersionedManifest) -> Self {
        match versioned_manifest {
            VersionedManifest::V0_8(mut manifest) => {
                for template in &mut manifest.templates {
                    // Override the randomly generated doc mapping UID with the nil value.
                    template.doc_mapping.doc_mapping_uid = DocMappingUid::default();
                }
                manifest.into()
            }
            VersionedManifest::V0_9(manifest) => manifest.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ManifestV0_8 {
    indexes: BTreeMap<IndexId, IndexStatus>,
    templates: Vec<IndexTemplate>,
    #[serde(default, skip_serializing_if = "Uuid::is_nil")]
    identity: Uuid,
}

impl From<Manifest> for ManifestV0_8 {
    fn from(manifest: Manifest) -> Self {
        let templates = manifest
            .templates
            .into_values()
            .sorted_unstable_by(|left, right| left.template_id.cmp(&right.template_id))
            .collect();
        ManifestV0_8 {
            indexes: manifest.indexes,
            templates,
            identity: manifest.identity,
        }
    }
}

impl From<ManifestV0_8> for Manifest {
    fn from(manifest: ManifestV0_8) -> Self {
        let indexes = manifest.indexes.into_iter().collect();
        let templates = manifest
            .templates
            .into_iter()
            .map(|template| (template.template_id.clone(), template))
            .collect();
        Manifest {
            indexes,
            templates,
            identity: manifest.identity,
        }
    }
}

#[cfg(any(test, feature = "testsuite"))]
impl quickwit_config::TestableForRegression for Manifest {
    fn sample_for_regression() -> Self {
        let mut indexes = BTreeMap::new();
        indexes.insert("test-index-1".to_string(), IndexStatus::Creating);
        indexes.insert("test-index-2".to_string(), IndexStatus::Active);
        indexes.insert("test-index-3".to_string(), IndexStatus::Deleting);

        let mut templates = HashMap::new();
        templates.insert(
            "test-template-1".to_string(),
            IndexTemplate::sample_for_regression(),
        );
        Manifest {
            indexes,
            templates,
            identity: Uuid::nil(),
        }
    }

    fn assert_equality(&self, other: &Self) {
        assert_eq!(self.indexes, other.indexes);
        assert_eq!(self.templates, other.templates);
    }
}

pub(super) async fn load_or_create_manifest(storage: &dyn Storage) -> MetastoreResult<Manifest> {
    if file_exists(storage, MANIFEST_FILE_NAME).await? {
        let manifest_json = get_bytes(storage, MANIFEST_FILE_NAME).await?;
        let manifest: Manifest = serde_utils::from_json_bytes(&manifest_json)?;
        return Ok(manifest);
    }
    if file_exists(storage, LEGACY_MANIFEST_FILE_NAME).await? {
        let legacy_manifest_json = get_bytes(storage, LEGACY_MANIFEST_FILE_NAME).await?;
        let legacy_manifest: LegacyManifest = serde_utils::from_json_bytes(&legacy_manifest_json)?;
        let manifest = legacy_manifest.into_manifest();
        save_manifest(storage, &manifest).await?;

        if let Err(storage_error) = delete_file(storage, LEGACY_MANIFEST_FILE_NAME).await {
            error!(
                error=%storage_error,
                "failed to delete legacy manifest file located at `{}/{LEGACY_MANIFEST_FILE_NAME}`", storage.uri()
            );
        }
        return Ok(manifest);
    }
    let manifest = Manifest::default();
    save_manifest(storage, &manifest).await?;
    Ok(manifest)
}

pub(super) async fn save_manifest(
    storage: &dyn Storage,
    manifest: &Manifest,
) -> MetastoreResult<()> {
    let manifest_json_bytes = serde_utils::to_json_bytes_pretty(manifest)?;
    put_bytes(storage, MANIFEST_FILE_NAME, manifest_json_bytes).await?;
    Ok(())
}

async fn delete_file(storage: &dyn Storage, path: &str) -> StorageResult<()> {
    storage.delete(Path::new(path)).await?;
    Ok(())
}

async fn file_exists(storage: &dyn Storage, path_str: &str) -> MetastoreResult<bool> {
    let path = Path::new(path_str);
    let exists = storage.exists(path).await.map_err(|storage_error| {
        into_metastore_error(storage_error, storage.uri(), path, "list")
    })?;
    Ok(exists)
}

async fn get_bytes(storage: &dyn Storage, path_str: &str) -> MetastoreResult<OwnedBytes> {
    let path = Path::new(path_str);
    let bytes = storage.get_all(path).await.map_err(|storage_error| {
        into_metastore_error(storage_error, storage.uri(), path, "load")
    })?;
    Ok(bytes)
}

async fn put_bytes(storage: &dyn Storage, path_str: &str, content: Vec<u8>) -> MetastoreResult<()> {
    let path = Path::new(path_str);
    storage
        .put(path, Box::new(content))
        .await
        .map_err(|storage_error| {
            into_metastore_error(storage_error, storage.uri(), path, "save")
        })?;
    Ok(())
}

fn into_metastore_error(
    storage_error: StorageError,
    uri: &Uri,
    path: &Path,
    operation_name: &str,
) -> MetastoreError {
    match storage_error.kind() {
        StorageErrorKind::Unauthorized => MetastoreError::Forbidden {
            message: format!(
                "failed to access manifest file located at `{uri}/{}`: unauthorized",
                path.display()
            ),
        },
        _ => MetastoreError::Internal {
            message: format!(
                "failed to {operation_name} manifest file located at `{uri}/{}`",
                path.display()
            ),
            cause: storage_error.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_legacy_manifest_deserialization() {
        let legacy_manifest_json = r#"{
            "test-index-1": "Creating",
            "test-index-2": "Alive",
            "test-index-3": "Deleting"
        }
        "#;
        let legacy_manifest: LegacyManifest = serde_json::from_str(legacy_manifest_json).unwrap();
        assert_eq!(legacy_manifest.indexes.len(), 3);

        assert_eq!(
            legacy_manifest.indexes.get("test-index-1").unwrap(),
            &IndexStatus::Creating
        );
        assert_eq!(
            legacy_manifest.indexes.get("test-index-2").unwrap(),
            &IndexStatus::Active
        );
        assert_eq!(
            legacy_manifest.indexes.get("test-index-3").unwrap(),
            &IndexStatus::Deleting
        );
    }

    #[test]
    fn test_legacy_manifest_into_manifest() {
        let legacy_manifest = LegacyManifest {
            indexes: vec![
                ("test-index-1".to_string(), IndexStatus::Creating),
                ("test-index-2".to_string(), IndexStatus::Active),
                ("test-index-3".to_string(), IndexStatus::Deleting),
            ]
            .into_iter()
            .collect(),
        };
        let manifest = legacy_manifest.into_manifest();

        assert_eq!(manifest.indexes.len(), 3);
        assert_eq!(manifest.templates.len(), 0);

        assert_eq!(
            manifest.indexes.get("test-index-1").unwrap(),
            &IndexStatus::Creating
        );
        assert_eq!(
            manifest.indexes.get("test-index-2").unwrap(),
            &IndexStatus::Active
        );
        assert_eq!(
            manifest.indexes.get("test-index-3").unwrap(),
            &IndexStatus::Deleting
        );
    }

    #[test]
    fn test_manifest_serde() {
        let indexes = BTreeMap::from_iter([
            ("test-index-1".to_string(), IndexStatus::Creating),
            ("test-index-2".to_string(), IndexStatus::Active),
            ("test-index-3".to_string(), IndexStatus::Deleting),
        ]);
        let templates = HashMap::from_iter([
            (
                "test-template-1".to_string(),
                IndexTemplate::for_test("test-template-1", &["test-index-foo*"], 100),
            ),
            (
                "test-template-2".to_string(),
                IndexTemplate::for_test("test-template-2", &["test-index-bar*"], 200),
            ),
        ]);
        let manifest = Manifest {
            indexes,
            templates,
            identity: Uuid::nil(),
        };
        let manifest_json = serde_json::to_string_pretty(&manifest).unwrap();
        let manifest_deserialized: Manifest = serde_json::from_str(&manifest_json).unwrap();
        assert_eq!(manifest, manifest_deserialized);
    }

    #[tokio::test]
    async fn test_create_mutate_save_load_manifest() {
        let storage = quickwit_storage::storage_for_test();
        let mut manifest = load_or_create_manifest(&*storage).await.unwrap();

        assert_eq!(manifest.indexes.len(), 0);
        assert_eq!(manifest.templates.len(), 0);

        let empty_manifest_size = storage
            .get_all(Path::new(MANIFEST_FILE_NAME))
            .await
            .unwrap()
            .len();
        assert!(empty_manifest_size > 0);

        manifest
            .indexes
            .insert("test-index".to_string(), IndexStatus::Creating);
        manifest.templates.insert(
            "test-template".to_string(),
            IndexTemplate::for_test("test-template", &["test-index-*"], 100),
        );

        save_manifest(&*storage, &manifest).await.unwrap();

        let populated_manifest_size = storage
            .get_all(Path::new(MANIFEST_FILE_NAME))
            .await
            .unwrap()
            .len();
        assert!(populated_manifest_size > empty_manifest_size);

        let manifest = load_or_create_manifest(&*storage).await.unwrap();
        assert_eq!(manifest.indexes.len(), 1);
        assert_eq!(
            manifest.indexes.get("test-index").unwrap(),
            &IndexStatus::Creating
        );

        assert_eq!(manifest.templates.len(), 1);

        let template = manifest.templates.get("test-template").unwrap();
        assert_eq!(template.template_id, "test-template");
        assert_eq!(template.index_id_patterns, ["test-index-*"]);
        assert_eq!(template.priority, 100);
    }

    #[tokio::test]
    async fn test_legacy_manifest_migration() {
        let storage = quickwit_storage::storage_for_test();
        let legacy_manifest_json = json!(
            {
                "test-index-1": "Creating",
                "test-index-2": "Alive",
                "test-index-3": "Deleting"
            }
        );
        let legacy_manifest_json_bytes = serde_json::to_vec(&legacy_manifest_json).unwrap();

        put_bytes(
            &*storage,
            LEGACY_MANIFEST_FILE_NAME,
            legacy_manifest_json_bytes,
        )
        .await
        .unwrap();

        let manifest = load_or_create_manifest(&*storage).await.unwrap();
        assert_eq!(manifest.indexes.len(), 3);
        assert_eq!(manifest.templates.len(), 0);

        assert_eq!(
            manifest.indexes.get("test-index-1").unwrap(),
            &IndexStatus::Creating
        );
        assert_eq!(
            manifest.indexes.get("test-index-2").unwrap(),
            &IndexStatus::Active
        );
        assert_eq!(
            manifest.indexes.get("test-index-3").unwrap(),
            &IndexStatus::Deleting
        );

        let legacy_manifest_exists = file_exists(&*storage, LEGACY_MANIFEST_FILE_NAME)
            .await
            .unwrap();
        assert!(!legacy_manifest_exists);

        let manifest_exists = file_exists(&*storage, MANIFEST_FILE_NAME).await.unwrap();
        assert!(manifest_exists);
    }
}
