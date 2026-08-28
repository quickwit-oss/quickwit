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

use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::Debug;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, io};

use anyhow::{Context, bail, ensure};
use async_trait::async_trait;
use bytes::{Buf, BufMut};
use quickwit_common::chunk_range;
use quickwit_common::uri::Uri;
use serde::{Deserialize, Serialize};
use tantivy::HasLen;
use tantivy::directory::FileSlice;
use tokio::io::{AsyncRead, AsyncWriteExt};
use tracing::error;

use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, OwnedBytes, Storage, StorageError, StorageResult, VersionedComponent,
};

/// BundleStorage bundles together multiple files into a single file.
pub struct BundleStorage {
    storage: Arc<dyn Storage>,
    /// The file path of the bundle in the storage.
    bundle_filepath: PathBuf,
    file_ranges: BundleFileRanges,
}

impl BundleStorage {
    /// Opens a split bundle by locating its footer directly from object storage.
    ///
    /// New splits expose the footer start in a fixed-size trailer. Legacy splits are supported by
    /// walking backward through the hotcache and bundle-metadata length fields.
    pub async fn open_from_storage(
        storage: Arc<dyn Storage>,
        bundle_filepath: PathBuf,
    ) -> anyhow::Result<(Self, OwnedBytes, Range<u64>)> {
        let split_len = storage.file_num_bytes(&bundle_filepath).await?;
        let split_footer_range =
            locate_split_footer_range(storage.as_ref(), &bundle_filepath, split_len).await?;
        let split_footer_range_usize =
            split_footer_range.start as usize..split_footer_range.end as usize;
        let split_footer_bytes = storage
            .get_slice(&bundle_filepath, split_footer_range_usize)
            .await?;
        let (bundle_storage, hotcache) =
            Self::open_from_split_bytes(storage, bundle_filepath, split_footer_bytes)?;
        Ok((bundle_storage, hotcache, split_footer_range))
    }

    /// Opens a BundleStorage.
    ///
    /// The provided bytes must include the footer bytes at the end of the slice, but they can have
    /// more up front.
    ///
    /// Returns (Self, Hotcache)
    pub fn open_from_split_bytes(
        storage: Arc<dyn Storage>,
        bundle_filepath: PathBuf,
        split_bytes: OwnedBytes,
    ) -> anyhow::Result<(Self, OwnedBytes)> {
        let (file_ranges, hotcache) = BundleFileRanges::open_from_split_bytes(split_bytes)?;
        Ok((
            BundleStorage {
                storage,
                bundle_filepath,
                file_ranges,
            },
            hotcache,
        ))
    }

    /// Returns Iterator over files contained in the bundle.
    pub fn iter_files(&self) -> impl Iterator<Item = &PathBuf> {
        self.file_ranges.files.keys()
    }

    /// Fetches a bundled file from a split.
    ///
    /// Use this only when retrieving a single file from the split. To retrieve multiple files,
    /// prefer [`Self::open_from_storage`].
    ///
    /// The split length is provided by the caller (e.g. from object listing metadata) to avoid a
    /// separate metadata request.
    pub async fn fetch_file_from_split(
        storage: Arc<dyn Storage>,
        bundle_filepath: PathBuf,
        split_path: &Path,
        split_len: u64,
    ) -> anyhow::Result<(OwnedBytes, Range<u64>)> {
        let (split_bytes, footer_range) = fetch_split_tail(
            storage.as_ref(),
            split_path,
            split_len,
            DEFAULT_SPLIT_TAIL_WINDOW_NUM_BYTES,
        )
        .await?;

        // Parse the bundle file ranges from the split bytes.
        let tail_start = split_len - split_bytes.len() as u64;
        let (file_ranges, _hotcache) =
            BundleFileRanges::open_from_split_bytes(split_bytes.clone())?;
        let file_range = file_ranges.get(&bundle_filepath).ok_or_else(|| {
            anyhow::anyhow!(
                "missing file `{}` in split bundle",
                bundle_filepath.display()
            )
        })?;
        ensure!(
            file_range.start <= file_range.end,
            "bundled file range starts after it ends"
        );
        ensure!(
            file_range.end <= footer_range.start,
            "bundled file range overlaps split footer"
        );

        // If the initial tail also contains the file, reuse it and complete in one GET (at least).
        // Otherwise, fetch the file with an additional GET.
        let file_bytes = if file_range.start >= tail_start {
            let relative_start = (file_range.start - tail_start) as usize;
            let relative_end = (file_range.end - tail_start) as usize;
            split_bytes.slice(relative_start..relative_end)
        } else {
            let relative_start = file_range.start as usize;
            let relative_end = file_range.end as usize;
            storage
                .get_slice(split_path, relative_start..relative_end)
                .await?
        };
        Ok((file_bytes, footer_range))
    }
}

const HOTCACHE_LEN_NUM_BYTES: usize = std::mem::size_of::<u32>();
const BUNDLE_METADATA_LEN_NUM_BYTES: usize = std::mem::size_of::<u32>();
const SPLIT_FOOTER_TRAILER_MAGIC: &[u8; 4] = b"QWFT";
const SPLIT_FOOTER_TRAILER_VERSION: u32 = 1;
const SPLIT_FOOTER_START_NUM_BYTES: usize = std::mem::size_of::<u64>();
const SPLIT_FOOTER_TRAILER_VERSION_NUM_BYTES: usize = std::mem::size_of::<u32>();
pub(crate) const SPLIT_FOOTER_TRAILER_NUM_BYTES: usize = SPLIT_FOOTER_START_NUM_BYTES
    + SPLIT_FOOTER_TRAILER_VERSION_NUM_BYTES
    + SPLIT_FOOTER_TRAILER_MAGIC.len();
const DEFAULT_SPLIT_TAIL_WINDOW_NUM_BYTES: u64 = 1024 * 1024;

pub(crate) fn serialize_split_footer_trailer(
    footer_start_inclusive: u64,
) -> [u8; SPLIT_FOOTER_TRAILER_NUM_BYTES] {
    let mut trailer = [0u8; SPLIT_FOOTER_TRAILER_NUM_BYTES];
    let mut writer = trailer.as_mut_slice();
    writer.put_u64_le(footer_start_inclusive);
    writer.put_u32_le(SPLIT_FOOTER_TRAILER_VERSION);
    writer.put_slice(SPLIT_FOOTER_TRAILER_MAGIC);
    debug_assert!(!writer.has_remaining_mut());
    trailer
}

fn deserialize_split_footer_trailer(trailer: &[u8]) -> anyhow::Result<Option<u64>> {
    if trailer.len() != SPLIT_FOOTER_TRAILER_NUM_BYTES {
        return Ok(None);
    }
    let mut reader = trailer;
    let footer_start_inclusive = reader.get_u64_le();
    let version = reader.get_u32_le();

    if reader != SPLIT_FOOTER_TRAILER_MAGIC {
        return Ok(None);
    }
    ensure!(
        version == SPLIT_FOOTER_TRAILER_VERSION,
        "unsupported split footer trailer version {version}"
    );
    Ok(Some(footer_start_inclusive))
}

/// Locates a split footer range using its fixed trailer, with support for legacy split layouts.
pub async fn locate_split_footer_range(
    storage: &dyn Storage,
    split_path: &Path,
    split_len: u64,
) -> anyhow::Result<Range<u64>> {
    ensure!(
        split_len >= SPLIT_FOOTER_TRAILER_NUM_BYTES as u64,
        "split is too short to contain a footer"
    );
    let end = split_len as usize;
    let start = end - SPLIT_FOOTER_TRAILER_NUM_BYTES;
    let tail_bytes = storage.get_slice(split_path, start..end).await?;
    match locate_split_footer_range_in_tail(split_len, &tail_bytes)? {
        FooterLocation::Located(footer_range) => Ok(footer_range),
        FooterLocation::ReadBundleMetadataLen(bundle_metadata_len_range) => {
            let start = bundle_metadata_len_range.start as usize;
            let end = bundle_metadata_len_range.end as usize;
            let bundle_metadata_len_bytes = storage.get_slice(split_path, start..end).await?;
            locate_split_footer_range_from_metadata_len(
                split_len,
                bundle_metadata_len_range.start,
                bundle_metadata_len_bytes.as_slice(),
            )
        }
    }
}

enum FooterLocation {
    Located(Range<u64>),
    /// The exact range containing the bundle-metadata length.
    ReadBundleMetadataLen(Range<u64>),
}

fn locate_split_footer_range_from_metadata_len(
    split_len: u64,
    bundle_metadata_len_start: u64,
    bundle_metadata_len_bytes: &[u8],
) -> anyhow::Result<Range<u64>> {
    let bundle_metadata_len = u32::from_le_bytes(bundle_metadata_len_bytes.try_into()?) as u64;
    let footer_start = bundle_metadata_len_start
        .checked_sub(bundle_metadata_len)
        .context("split footer exceeds split length")?;
    Ok(footer_start..split_len)
}

fn locate_split_footer_range_in_tail(
    split_len: u64,
    tail_bytes: &[u8],
) -> anyhow::Result<FooterLocation> {
    // Legacy split layout:
    // [body][bundle metadata][metadata len][hotcache][hotcache len]
    ensure!(
        tail_bytes.len() as u64 <= split_len,
        "split tail is longer than the split itself"
    );
    let trailer_start = tail_bytes
        .len()
        .checked_sub(SPLIT_FOOTER_TRAILER_NUM_BYTES)
        .context("split tail is too short to contain a footer trailer")?;
    if let Some(footer_start) = deserialize_split_footer_trailer(&tail_bytes[trailer_start..])? {
        ensure!(
            footer_start <= split_len - SPLIT_FOOTER_TRAILER_NUM_BYTES as u64,
            "split footer starts after its trailer"
        );
        return Ok(FooterLocation::Located(footer_start..split_len));
    }

    let hotcache_len =
        u32::from_le_bytes(tail_bytes[tail_bytes.len() - HOTCACHE_LEN_NUM_BYTES..].try_into()?)
            as u64;
    let bundle_metadata_len_end = split_len
        .checked_sub(HOTCACHE_LEN_NUM_BYTES as u64)
        .and_then(|offset| offset.checked_sub(hotcache_len))
        .context("split footer exceeds split length")?;
    let bundle_metadata_len_start = bundle_metadata_len_end
        .checked_sub(BUNDLE_METADATA_LEN_NUM_BYTES as u64)
        .context("split footer exceeds split length")?;
    let bundle_metadata_len_range = bundle_metadata_len_start..bundle_metadata_len_end;
    let tail_start = split_len - tail_bytes.len() as u64;
    if tail_start > bundle_metadata_len_start {
        return Ok(FooterLocation::ReadBundleMetadataLen(
            bundle_metadata_len_range,
        ));
    }

    let relative_start = (bundle_metadata_len_start - tail_start) as usize;
    let relative_end = (bundle_metadata_len_end - tail_start) as usize;
    let footer_range = locate_split_footer_range_from_metadata_len(
        split_len,
        bundle_metadata_len_start,
        &tail_bytes[relative_start..relative_end],
    )?;
    Ok(FooterLocation::Located(footer_range))
}

/// Reads the tail of a split until it holds the complete bundle footer.
///
/// `initial_tail_window_num_bytes` is a jump-start hint: a larger window downloads more up front
/// but can save range GETs by covering the footer, and possibly the wanted file, in one read.
/// When the window falls short, the tail is re-read: once for a new split, whose trailer gives
/// the footer start, and up to twice for a legacy split, whose footer length is derived from the
/// trailing hotcache and bundle-metadata lengths.
///
/// Returns the tail, which may start before the footer, and the footer range within the split.
async fn fetch_split_tail(
    storage: &dyn Storage,
    split_path: &Path,
    split_len: u64,
    initial_tail_window_num_bytes: u64,
) -> anyhow::Result<(OwnedBytes, Range<u64>)> {
    ensure!(
        split_len >= SPLIT_FOOTER_TRAILER_NUM_BYTES as u64,
        "split is too short to contain a footer"
    );

    // Start with the requested window, but always cover the fixed trailer and never read before
    // the beginning of the split.
    let initial_tail_num_bytes =
        initial_tail_window_num_bytes.clamp(SPLIT_FOOTER_TRAILER_NUM_BYTES as u64, split_len);
    let mut tail_bytes =
        read_split_tail(storage, split_path, split_len, initial_tail_num_bytes).await?;

    // Locate the footer range.
    let footer_range = match locate_split_footer_range_in_tail(split_len, &tail_bytes)? {
        FooterLocation::Located(footer_range) => {
            // The range is known, but the initial tail may not contain the complete footer.
            footer_range
        }
        FooterLocation::ReadBundleMetadataLen(bundle_metadata_len_range) => {
            // Extend the tail through the legacy bundle-metadata length field, then use that
            // length to locate the beginning of the footer.
            let required_tail_num_bytes = split_len - bundle_metadata_len_range.start;
            let metadata_len_tail_bytes =
                read_split_tail(storage, split_path, split_len, required_tail_num_bytes).await?;
            match locate_split_footer_range_in_tail(split_len, &metadata_len_tail_bytes)? {
                FooterLocation::Located(footer_range) => footer_range,
                FooterLocation::ReadBundleMetadataLen(_) => {
                    bail!("failed to locate split footer after reading bundle metadata length");
                }
            }
        }
    };

    // If the initial tail does not contain the entire footer, fetch the exact footer range now
    // that its boundaries are known.
    let footer_num_bytes = footer_range.end - footer_range.start;
    if (tail_bytes.len() as u64) < footer_num_bytes {
        tail_bytes = read_split_tail(storage, split_path, split_len, footer_num_bytes).await?;
    }
    Ok((tail_bytes, footer_range))
}

async fn read_split_tail(
    storage: &dyn Storage,
    split_path: &Path,
    split_len: u64,
    tail_num_bytes: u64,
) -> anyhow::Result<OwnedBytes> {
    ensure!(
        tail_num_bytes <= split_len,
        "split tail exceeds split length"
    );
    let start = (split_len - tail_num_bytes) as usize;
    let tail_bytes = storage
        .get_slice(split_path, start..split_len as usize)
        .await?;
    Ok(tail_bytes)
}

/// Removes the fixed split footer trailer when it is present.
pub fn strip_split_footer_trailer(split_slice: FileSlice) -> anyhow::Result<FileSlice> {
    if split_slice.len() < SPLIT_FOOTER_TRAILER_NUM_BYTES {
        return Ok(split_slice);
    }
    let (split_slice_without_trailer, trailer) = split_slice
        .clone()
        .split_from_end(SPLIT_FOOTER_TRAILER_NUM_BYTES);
    if deserialize_split_footer_trailer(trailer.read_bytes()?.as_ref())?.is_some() {
        Ok(split_slice_without_trailer)
    } else {
        Ok(split_slice)
    }
}

#[derive(Copy, Clone, Default)]
#[repr(u32)]
pub enum BundleFileRangesVersions {
    #[default]
    V1 = 1,
}

impl VersionedComponent for BundleFileRangesVersions {
    const MAGIC_NUMBER: u32 = 403_881_646u32;

    type Component = BundleFileRanges;

    fn to_version_code(self) -> u32 {
        self as u32
    }

    fn try_from_version_code_impl(version_code: u32) -> Option<Self> {
        match version_code {
            1 => Some(Self::V1),
            _ => None,
        }
    }

    fn serialize_impl(component: &BundleFileRanges, output: &mut Vec<u8>) {
        let file_ranges_json = serde_json::to_string(component).unwrap();
        output.extend_from_slice(file_ranges_json.as_bytes());
    }

    fn deserialize_impl(&self, bytes: &mut OwnedBytes) -> anyhow::Result<Self::Component> {
        serde_json::from_reader(bytes).context("deserializing bundle file ranges failed")
    }
}

/// Maps files in a bundle to their byte ranges.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct BundleFileRanges {
    /// The files and their byte ranges in the bundle body.
    pub files: HashMap<PathBuf, Range<u64>>,
}

impl BundleFileRanges {
    /// File need to include split data (with hotcache at the end).
    /// See docs/internals/split-format.md
    /// [Files, BundleFileRanges, BundleMetadata Len, HotCache, HotCache Len, Split Footer Trailer]
    /// Returns (Self, Hotcache)
    fn open_from_split_bytes(split_bytes: OwnedBytes) -> anyhow::Result<(Self, OwnedBytes)> {
        let split_slice = FileSlice::new(Arc::new(split_bytes));
        let split_slice = strip_split_footer_trailer(split_slice)?;
        let (bundle_and_hotcache_bytes, hotcache_len_data) =
            split_slice.split_from_end(HOTCACHE_LEN_NUM_BYTES);
        let hotcache_len: u32 =
            u32::from_le_bytes(hotcache_len_data.read_bytes()?.as_ref().try_into().unwrap());
        let (bundle, hotcache) = bundle_and_hotcache_bytes.split_from_end(hotcache_len as usize);
        Ok((Self::open(bundle)?, hotcache.read_bytes()?))
    }

    /// FileSlice needs to end with the bundle (without hotcache from the split at the end).
    /// See docs/internals/split-format.md
    /// [Files, BundleFileRanges, BundleMetadata Len]
    pub fn open(file: FileSlice) -> anyhow::Result<Self> {
        let (bundle_and_metadata, bundle_metadata_len_data) =
            file.split_from_end(BUNDLE_METADATA_LEN_NUM_BYTES);
        let bundle_metadata_len: u32 = u32::from_le_bytes(
            bundle_metadata_len_data
                .read_bytes()?
                .as_slice()
                .try_into()
                .unwrap(),
        );

        let mut bundle_metadata_data = bundle_and_metadata
            .slice_from_end(bundle_metadata_len as usize)
            .read_bytes()?;
        BundleFileRangesVersions::try_read_component(&mut bundle_metadata_data)
    }

    /// Returns the byte range for a given path.
    pub fn get(&self, path: &Path) -> Option<Range<u64>> {
        self.files.get(path).cloned()
    }

    /// Returns whether the bundle contains a file at the given path.
    pub fn exists(&self, path: &Path) -> bool {
        self.files.contains_key(path)
    }
}

#[async_trait]
impl Storage for BundleStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        if !self
            .storage
            .exists(&self.bundle_filepath)
            .await
            .unwrap_or(false)
        {
            bail!("`{}` not found in storage", self.bundle_filepath.display())
        }
        Ok(())
    }

    async fn put(
        &self,
        path: &Path,
        _payload: Box<dyn crate::PutPayload>,
    ) -> crate::StorageResult<()> {
        Err(unsupported_operation(&[path]))
    }

    async fn copy_to(
        &self,
        path: &Path,
        output: &mut dyn SendableAsync,
    ) -> crate::StorageResult<()> {
        let file_len = self.file_num_bytes(path).await? as usize;
        let block_size = 100_000_000;
        for block in chunk_range(0..file_len, block_size) {
            let file_content = self.get_slice(path, block).await?;
            output.write_all(&file_content).await?;
        }
        output.flush().await?;
        Ok(())
    }

    async fn get_slice(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> crate::StorageResult<OwnedBytes> {
        let file_range = self.file_ranges.get(path).ok_or_else(|| {
            crate::StorageErrorKind::NotFound
                .with_error(anyhow::anyhow!("missing file `{}`", path.display()))
        })?;
        let new_range =
            file_range.start as usize + range.start..file_range.start as usize + range.end;
        self.storage
            .get_slice(&self.bundle_filepath, new_range)
            .await
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        _range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        Err(unsupported_operation(&[path]))
    }

    async fn get_all(&self, path: &Path) -> crate::StorageResult<OwnedBytes> {
        let file_range = self.file_ranges.get(path).ok_or_else(|| {
            crate::StorageErrorKind::NotFound
                .with_error(anyhow::anyhow!("missing file `{}`", path.display()))
        })?;
        self.storage
            .get_slice(
                &self.bundle_filepath,
                file_range.start as usize..file_range.end as usize,
            )
            .await
    }

    async fn delete(&self, path: &Path) -> crate::StorageResult<()> {
        Err(unsupported_operation(&[path]))
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        Err(BulkDeleteError {
            error: Some(unsupported_operation(paths)),
            ..Default::default()
        })
    }

    async fn exists(&self, path: &Path) -> crate::StorageResult<bool> {
        // also check if self.bundle_file_name exists ?
        Ok(self.file_ranges.exists(path))
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        let file_range = self.file_ranges.get(path).ok_or_else(|| {
            crate::StorageErrorKind::NotFound
                .with_error(anyhow::anyhow!("missing file `{}`", path.display()))
        })?;
        Ok(file_range.end - file_range.start)
    }

    fn uri(&self) -> &Uri {
        self.storage.uri()
    }
}

impl HasLen for BundleStorage {
    fn len(&self) -> usize {
        unimplemented!()
    }
}

impl fmt::Debug for BundleStorage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "BundleStorage({:?}, files={:?})",
            &self.bundle_filepath, self.file_ranges
        )
    }
}

fn unsupported_operation(paths: &[&Path]) -> StorageError {
    let msg = "Unsupported operation. BundleStorage only supports async reads";
    error!(paths=?paths, msg);
    io::Error::other(format!("{msg}: {paths:?}")).into()
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::io::Write;

    use super::*;
    use crate::{CountingStorage, PutPayload, RamStorageBuilder, SplitPayloadBuilder};

    const DEFAULT_SPLIT_TAIL_WINDOW_NUM_BYTES: u64 = 1024 * 1024;

    #[tokio::test]
    async fn bundle_storage_locates_footer_from_object_storage() {
        let mut split_payload_builder = SplitPayloadBuilder::default();
        split_payload_builder.add_payload("fields".to_string(), Box::new(b"fields".to_vec()));
        let split_payload = split_payload_builder
            .finalize_with_footer_trailer(b"hotcache", true)
            .unwrap();
        let expected_footer_range = split_payload.footer_range.clone();
        let split_bytes = split_payload.read_all().await.unwrap();
        let split_path = PathBuf::from("split");
        let storage = Arc::new(
            RamStorageBuilder::default()
                .put(&split_path.to_string_lossy(), &split_bytes)
                .build(),
        );

        let (_bundle_storage, hotcache, footer_range) =
            BundleStorage::open_from_storage(storage, split_path)
                .await
                .unwrap();

        assert_eq!(hotcache.as_ref(), b"hotcache");
        assert_eq!(footer_range, expected_footer_range);
    }

    #[tokio::test]
    async fn bundle_storage_locates_legacy_footer_from_object_storage() {
        let split_payload =
            SplitPayloadBuilder::get_split_payload(&[], b"fields", None, b"hotcache").unwrap();
        let expected_footer_range = split_payload.footer_range.clone();
        let split_bytes = split_payload.read_all().await.unwrap();
        let split_path = PathBuf::from("legacy-split");
        let inner_storage: Arc<dyn Storage> = Arc::new(
            RamStorageBuilder::default()
                .put(&split_path.to_string_lossy(), &split_bytes)
                .build(),
        );
        let (storage, counters) = CountingStorage::instrument_storage(inner_storage);

        let (_bundle_storage, hotcache, footer_range) =
            BundleStorage::open_from_storage(storage, split_path)
                .await
                .unwrap();

        assert_eq!(hotcache.as_ref(), b"hotcache");
        assert_eq!(footer_range, expected_footer_range);
        assert_eq!(counters.snapshot().1, 2);
    }

    #[tokio::test]
    async fn bundle_storage_locates_legacy_footer_with_large_hotcache() {
        let hotcache_bytes = vec![0u8; 1024];
        let split_payload =
            SplitPayloadBuilder::get_split_payload(&[], b"fields", None, &hotcache_bytes).unwrap();
        let expected_footer_range = split_payload.footer_range.clone();
        let split_bytes = split_payload.read_all().await.unwrap();
        let split_path = PathBuf::from("legacy-split");
        let inner_storage: Arc<dyn Storage> = Arc::new(
            RamStorageBuilder::default()
                .put(&split_path.to_string_lossy(), &split_bytes)
                .build(),
        );
        let (storage, counters) = CountingStorage::instrument_storage(inner_storage);

        let (_bundle_storage, hotcache, footer_range) =
            BundleStorage::open_from_storage(storage, split_path)
                .await
                .unwrap();

        assert_eq!(hotcache.as_ref(), hotcache_bytes.as_slice());
        assert_eq!(footer_range, expected_footer_range);
        assert_eq!(counters.snapshot().1, 3);
    }

    #[tokio::test]
    async fn bundle_file_ranges() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let test_filepath1 = temp_dir.path().join("f1");
        let test_filepath2 = temp_dir.path().join("f2");

        let mut file1 = File::create(&test_filepath1)?;
        file1.write_all(&[123, 76])?;

        let mut file2 = File::create(&test_filepath2)?;
        file2.write_all(&[99, 55, 44])?;

        let buffer = SplitPayloadBuilder::get_split_payload(
            &[test_filepath1.clone(), test_filepath2.clone()],
            &[],
            None,
            &[5, 5, 5],
        )?
        .read_all()
        .await?;

        let bundle_filepath = Path::new("bundle");
        let (file_ranges, hotcache) = BundleFileRanges::open_from_split_bytes(buffer.clone())?;
        assert_eq!(hotcache.as_ref(), &[5, 5, 5]);
        let ram_storage = RamStorageBuilder::default()
            .put(&bundle_filepath.to_string_lossy(), &buffer)
            .build();

        let bundle_storage = BundleStorage {
            file_ranges,
            bundle_filepath: bundle_filepath.to_path_buf(),
            storage: Arc::new(ram_storage),
        };
        let f1_data = bundle_storage.get_all(Path::new("f1")).await?;
        assert_eq!(&*f1_data, &[123u8, 76u8]);

        let f2_data = bundle_storage.get_all(Path::new("f2")).await?;
        assert_eq!(&f2_data[..], &[99, 55, 44]);

        Ok(())
    }

    #[tokio::test]
    async fn bundle_storage_test() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let test_filepath1 = temp_dir.path().join("f1");
        let test_filepath2 = temp_dir.path().join("f2");

        let mut file1 = File::create(&test_filepath1)?;
        file1.write_all(&[123, 76])?;

        let mut file2 = File::create(&test_filepath2)?;
        file2.write_all(&[99, 55, 44])?;

        let buffer = SplitPayloadBuilder::get_split_payload(
            &[test_filepath1.clone(), test_filepath2.clone()],
            &[],
            None,
            &[1, 3, 3, 7],
        )?
        .read_all()
        .await?;

        let (file_ranges, hotcache) = BundleFileRanges::open_from_split_bytes(buffer.clone())?;
        assert_eq!(hotcache.as_ref(), &[1, 3, 3, 7]);

        let bundle_filepath = Path::new("bundle");
        let ram_storage = RamStorageBuilder::default()
            .put(&bundle_filepath.to_string_lossy(), &buffer)
            .build();

        let bundle_storage = BundleStorage {
            file_ranges,
            bundle_filepath: bundle_filepath.to_path_buf(),
            storage: Arc::new(ram_storage),
        };
        let f1_data = bundle_storage.get_all(Path::new("f1")).await?;
        assert_eq!(&*f1_data, &[123u8, 76u8]);

        let f2_data = bundle_storage.get_all(Path::new("f2")).await?;
        assert_eq!(&f2_data[..], &[99, 55, 44]);

        let copy_to_file = temp_dir.path().join("copy_file");
        bundle_storage
            .copy_to_file(Path::new("f2"), &copy_to_file)
            .await?;
        let file_content = fs::read(copy_to_file).unwrap();
        assert_eq!(&f2_data[..], file_content);

        Ok(())
    }

    #[tokio::test]
    async fn bundlestorage_test_empty() -> anyhow::Result<()> {
        let buffer = SplitPayloadBuilder::get_split_payload(&[], &[], None, &[])?
            .read_all()
            .await?;

        let (file_ranges, _hotcache) = BundleFileRanges::open_from_split_bytes(buffer.clone())?;

        let bundle_filepath = PathBuf::from("bundle");
        let ram_storage = RamStorageBuilder::default()
            .put(&bundle_filepath.to_string_lossy(), &buffer)
            .build();
        let bundle_storage = BundleStorage {
            file_ranges,
            bundle_filepath,
            storage: Arc::new(ram_storage),
        };

        assert_eq!(bundle_storage.exists(Path::new("blub")).await?, false);

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_file_from_split_uses_one_tail_read() {
        let mut split_builder = SplitPayloadBuilder::default();
        split_builder.add_payload(
            "large-file".to_string(),
            Box::new(vec![0u8; DEFAULT_SPLIT_TAIL_WINDOW_NUM_BYTES as usize + 1]),
        );
        split_builder.add_payload("target".to_string(), Box::new(b"target-bytes".to_vec()));
        let split_payload = split_builder
            .finalize_with_footer_trailer(b"hotcache", true)
            .unwrap();
        let expected_footer_range = split_payload.footer_range.clone();
        let split_bytes = split_payload.read_all().await.unwrap();
        let inner_storage: Arc<dyn Storage> = Arc::new(
            RamStorageBuilder::default()
                .put("split", &split_bytes)
                .build(),
        );
        let (storage, counters) = CountingStorage::instrument_storage(inner_storage);

        let (target_bytes, footer_range) = BundleStorage::fetch_file_from_split(
            storage,
            PathBuf::from("target"),
            Path::new("split"),
            split_bytes.len() as u64,
        )
        .await
        .unwrap();

        assert_eq!(target_bytes.as_slice(), b"target-bytes");
        assert_eq!(footer_range, expected_footer_range);
        assert_eq!(counters.snapshot().1, 1);
    }

    #[tokio::test]
    async fn test_fetch_file_from_split_widens_then_fetches_file_range() {
        let mut split_builder = SplitPayloadBuilder::default();
        split_builder.add_payload("target".to_string(), Box::new(b"target-bytes".to_vec()));
        let hotcache = vec![0u8; DEFAULT_SPLIT_TAIL_WINDOW_NUM_BYTES as usize + 1];
        let split_payload = split_builder
            .finalize_with_footer_trailer(&hotcache, false)
            .unwrap();
        let expected_footer_range = split_payload.footer_range.clone();
        let split_bytes = split_payload.read_all().await.unwrap();
        let inner_storage: Arc<dyn Storage> = Arc::new(
            RamStorageBuilder::default()
                .put("split", &split_bytes)
                .build(),
        );
        let (storage, counters) = CountingStorage::instrument_storage(inner_storage);

        let (target_bytes, footer_range) = BundleStorage::fetch_file_from_split(
            storage,
            PathBuf::from("target"),
            Path::new("split"),
            split_bytes.len() as u64,
        )
        .await
        .unwrap();

        assert_eq!(target_bytes.as_slice(), b"target-bytes");
        assert_eq!(footer_range, expected_footer_range);
        assert_eq!(counters.snapshot().1, 4);
    }
}
