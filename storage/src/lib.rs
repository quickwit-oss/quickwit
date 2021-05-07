/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

mod storage;
pub use self::storage::{PutPayload, Storage};

mod error;
mod object_storage;
mod ram_storage;
mod retry;
mod storage_resolver;

pub use self::object_storage::{MultiPartPolicy, S3CompatibleObjectStorage, S3MultiPartPolicy};
pub use self::ram_storage::RamStorage;
pub use self::storage_resolver::{
    DefaultStorageURIResolver, RamStorageURIResolver, StorageURIResolver,
};
pub use crate::error::{StorageFromURIError, StoreError, StoreErrorKind, StoreResult};

#[cfg(any(test, feature = "testsuite"))]
pub mod tests {

    use anyhow::Context;

    use crate::PutPayload;
    use std::path::Path;

    use crate::{Storage, StoreErrorKind};

    async fn test_get_inexistent_file(store: &mut dyn Storage) -> anyhow::Result<()> {
        let err = store
            .get_slice(Path::new("missingfile"), 0..3)
            .await
            .map_err(|err| err.kind());
        assert!(matches!(err, Err(StoreErrorKind::DoesNotExist)));
        Ok(())
    }

    async fn test_write_and_get_slice(store: &mut dyn Storage) -> anyhow::Result<()> {
        let test_path = Path::new("write_and_read_slice");
        store
            .put(
                test_path,
                PutPayload::from(b"abcdefghiklmnopqrstuvxyz".to_vec()),
            )
            .await?;
        let payload = store.get_slice(test_path, 3..6).await?;
        assert_eq!(&payload[..], b"def".as_ref());
        Ok(())
    }

    async fn test_write_get_all(store: &mut dyn Storage) -> anyhow::Result<()> {
        let test_path = Path::new("write_and_read_all");
        store
            .put(test_path, PutPayload::from(b"abcdef".to_vec()))
            .await?;
        let payload = store.get_all(test_path).await?;
        assert_eq!(&payload[..], &b"abcdef"[..]);
        Ok(())
    }

    async fn test_write_and_cp(store: &mut dyn Storage) -> anyhow::Result<()> {
        let test_path = Path::new("write_and_cp");
        let payload_bytes = b"abcdefghijklmnopqrstuvwxyz".as_ref();
        store
            .put(test_path, PutPayload::from(payload_bytes))
            .await?;
        let tempdir = tempfile::tempdir()?;
        let dest_path = tempdir.path().to_path_buf();
        let local_copy = dest_path.join("local_copy");
        store.copy_to_file(test_path, &local_copy).await?;
        let payload = std::fs::read(&local_copy)?;
        assert_eq!(&payload[..], payload_bytes);
        Ok(())
    }

    async fn test_write_and_delete(store: &mut dyn Storage) -> anyhow::Result<()> {
        let test_path = Path::new("write_and_delete");
        let payload_bytes = b"abcdefghijklmnopqrstuvwxyz".as_ref();
        store
            .put(test_path, PutPayload::from(payload_bytes))
            .await?;
        store.delete(test_path).await?;
        let slice_err = store.get_slice(test_path, 0..3).await.map_err(|e| e.kind());
        assert!(matches!(slice_err, Err(StoreErrorKind::DoesNotExist)));
        Ok(())
    }

    pub async fn storage_test_suite(store: &mut dyn Storage) -> anyhow::Result<()> {
        test_get_inexistent_file(store)
            .await
            .with_context(|| "get_inexistent_file")?;
        test_write_and_get_slice(store)
            .await
            .with_context(|| "write_and_get_slice")?;
        test_write_get_all(store)
            .await
            .with_context(|| "write_and_get_all")?;
        test_write_and_cp(store)
            .await
            .with_context(|| "write_and_cp")?;
        test_write_and_delete(store)
            .await
            .with_context(|| "write_and_delete")?;
        Ok(())
    }
}
