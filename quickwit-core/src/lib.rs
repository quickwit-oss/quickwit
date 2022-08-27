// Copyright (C) 2022 Quickwit, Inc.
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

mod index;

pub use index::{
    clear_cache_directory, get_cache_directory_path, remove_indexing_directory,
    validate_storage_uri, IndexService, IndexServiceError,
};

#[cfg(test)]
mod tests {
    use std::path::Path;

    use quickwit_common::uri::Uri;
    use quickwit_config::{IndexConfig, IndexingSettings, SearchSettings};
    use quickwit_indexing::{FileEntry, TestSandbox};
    use quickwit_metastore::quickwit_metastore_uri_resolver;
    use quickwit_storage::StorageUriResolver;

    use crate::IndexService;

    #[tokio::test]
    async fn test_file_entry_from_split_and_index_delete() -> anyhow::Result<()> {
        let index_id = "test-index";
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: title
                type: text
              - name: body
                type: text
              - name: url
                type: text
        "#;
        let test_sandbox =
            TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["title", "body"]).await?;
        test_sandbox.add_documents(vec![
            serde_json::json!({"title": "snoopy", "body": "Snoopy is an anthropomorphic beagle[5] in the comic strip...", "url": "http://snoopy"}),
        ]).await?;
        let splits = test_sandbox
            .metastore()
            .list_all_splits(index_id)
            .await?
            .into_iter()
            .map(|metadata| metadata.split_metadata)
            .collect::<Vec<_>>();
        let file_entries: Vec<FileEntry> = splits.iter().map(FileEntry::from).collect();
        assert_eq!(file_entries.len(), 1);
        for file_entry in file_entries {
            let split_num_bytes = test_sandbox
                .storage()
                .file_num_bytes(Path::new(file_entry.file_name.as_str()))
                .await?;
            assert_eq!(split_num_bytes, file_entry.file_size_in_bytes);
        }
        // Now delete the index.
        let index_service = IndexService::new(
            test_sandbox.metastore(),
            StorageUriResolver::for_test(),
            Uri::new("ram:///indexes".to_string()),
        );
        let deleted_file_entries = index_service.delete_index(index_id, false).await?;
        assert_eq!(deleted_file_entries.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_create_index_without_index_uri() -> anyhow::Result<()> {
        let index_id = "test-index--no-index-uri";
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: title
                type: text
        "#;
        let index_config = IndexConfig {
            version: 0,
            index_id: index_id.to_string(),
            index_uri: None,
            doc_mapping: serde_yaml::from_str(doc_mapping_yaml)?,
            retention_policy: None,
            indexing_settings: IndexingSettings::default(),
            search_settings: SearchSettings::default(),
            sources: Vec::new(),
        };
        let metastore_uri = Uri::new("ram:///metastore".to_string());
        let metastore = quickwit_metastore_uri_resolver()
            .resolve(&metastore_uri)
            .await
            .unwrap();
        let storage_resolver = StorageUriResolver::for_test();
        let index_service = IndexService::new(
            metastore,
            storage_resolver,
            Uri::new("ram:///indexes".to_string()),
        );
        let index_metadata = index_service.create_index(index_config, false).await?;
        assert_eq!(index_metadata.index_id, index_id);
        assert_eq!(
            index_metadata.index_uri,
            "ram:///indexes/test-index--no-index-uri"
        );
        Ok(())
    }
}
