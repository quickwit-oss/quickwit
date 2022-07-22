use std::collections::HashMap;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::ErrorKind;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::vec::IntoIter;
use std::{cmp, io};

use fasthash::FastHasher;
use parking_lot::{Mutex, RwLock};

use super::access_log::{open_access_log, AccessLogWriter, Metadata};
use super::file::{FileEntry, FileKey};

/// Opens a given directory as a file backed cache.
///
/// Note:
///  This will purge any files not explicitly managed by the
///  cache as part of a cleanup process on startup.
pub(crate) fn open_disk_store(
    base_path: &Path,
    max_fd: usize,
) -> io::Result<Store<FileBackedDirectory>> {
    let log = open_access_log(base_path)?;

    purge_unknown_files(base_path, &log.metadata)?;

    let directory = FileBackedDirectory::new(base_path, max_fd, log.metadata);
    let store = Store::new(directory, log.writer);

    Ok(store)
}

fn purge_unknown_files(base_path: &Path, metadata: &Metadata) -> io::Result<()> {
    for entry in base_path.read_dir()? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };

        if !entry.path().is_file() {
            continue;
        }

        let valid_file = entry
            .file_name()
            .to_string_lossy()
            .parse::<FileKey>()
            .map(|key| metadata.contains_key(&key))
            .unwrap_or_default();

        if !valid_file {
            // We don't want to prevent cleaning up other files.
            let _ = std::fs::remove_file(entry.path());
        }
    }

    Ok(())
}

pub(crate) struct Store<D: Directory + Sync + Send + 'static> {
    directory: Arc<D>,
    access_log: AccessLogWriter,
}

impl<D: Directory + Sync + Send + 'static> Clone for Store<D> {
    fn clone(&self) -> Self {
        Self {
            directory: self.directory.clone(),
            access_log: self.access_log.clone(),
        }
    }
}

impl<D: Directory + Sync + Send + 'static> Store<D> {
    fn new(directory: D, access_log: AccessLogWriter) -> Self {
        Self {
            directory: Arc::new(directory),
            access_log,
        }
    }

    /// Gets the metadata associated with the given file key.
    ///
    /// If the file does not exist `None` is returned.
    pub(crate) fn get_metadata(&self, key: FileKey) -> Option<FileEntry> {
        self.directory.get_metadata(key)
    }

    pub(crate) fn entries(&self) -> impl Iterator<Item = FileEntry> {
        self.directory.entries()
    }

    /// Reads a range of bytes from the given file.
    ///
    /// If the file does not exist `Ok(None)` is returned.
    ///
    /// The contents returned for the given key is an implementation detail of
    /// the given `Directory`.
    pub(crate) fn get_range(
        &self,
        path_key: &Path,
        range: Range<usize>,
    ) -> io::Result<Option<Vec<u8>>> {
        let computed_key = Self::compute_key(path_key);

        match self.directory.read_range(computed_key, range) {
            Ok(data) => {
                self.access_log
                    .register_read(computed_key, super::time_now());
                Ok(Some(data))
            }
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(other) => Err(other),
        }
    }

    /// Reads all the data from the file into a buffer.
    ///
    /// If the file does not exist `Ok(None)` is returned.
    ///
    /// The contents returned for the given key is an implementation detail of
    /// the given `Directory`.
    pub(crate) fn get_all(&self, path_key: &Path) -> io::Result<Option<Vec<u8>>> {
        let computed_key = Self::compute_key(path_key);

        match self.directory.read_all(computed_key) {
            Ok(data) => {
                self.access_log
                    .register_read(computed_key, super::time_now());
                Ok(Some(data))
            }
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(other) => Err(other),
        }
    }

    /// Writes a given buffer to an existing file or creates a new file between the given range.
    ///
    /// Note:
    ///  If a file does not already exist a new file will be created. Padding the file upto
    ///  the given range start.
    ///
    ///  TODO:
    ///   This seems very implementation dependant on whether this is actually a good idea
    ///   or not. Probably best to change this after testing with a reasonable big dataset.
    pub(crate) fn put_range(
        &self,
        path_key: &Path,
        range: Range<usize>,
        bytes: impl AsRef<[u8]>,
    ) -> io::Result<()> {
        let computed_key = Self::compute_key(path_key);

        let entry = self
            .directory
            .write_range(computed_key, range, bytes.as_ref())?;
        self.access_log.register_write(entry);

        Ok(())
    }

    /// Writes a given buffer to an existing or new file.
    pub(crate) fn put_all(&self, path_key: &Path, bytes: impl AsRef<[u8]>) -> io::Result<()> {
        let computed_key = Self::compute_key(path_key);

        let entry = self.directory.write_all(computed_key, bytes.as_ref())?;
        self.access_log.register_write(entry);

        Ok(())
    }

    /// Removes a file from the cache.
    pub(crate) fn remove(&self, path_key: &Path) -> io::Result<()> {
        let computed_key = Self::compute_key(path_key);

        self.directory.remove_file(computed_key)?;
        self.access_log.register_removal(computed_key);

        Ok(())
    }

    /// Removes a file from the cache with it's given file key.
    pub(crate) fn remove_with_key(&self, key: FileKey) -> io::Result<()> {
        self.directory.remove_file(key)?;
        self.access_log.register_removal(key);

        Ok(())
    }

    fn compute_key(path: &Path) -> FileKey {
        let mut hasher = fasthash::city::Hasher64::new();
        path.hash(&mut hasher);
        hasher.finish()
    }
}

pub(crate) trait Directory {
    type Entries: Iterator<Item = FileEntry>;

    /// Gets the metadata associated with the given file key.
    ///
    /// If the file does not exist `None` is returned.
    fn get_metadata(&self, key: FileKey) -> Option<FileEntry>;

    /// Produces an iterator of file metadata.
    fn entries(&self) -> Self::Entries;

    /// Attempts to read a slice of data from the given file.
    ///
    /// This does not validate the file checksum.
    /// This also does not
    fn read_range(&self, key: FileKey, range: Range<usize>) -> io::Result<Vec<u8>>;

    /// Attempts to read all data from the given file.
    ///
    /// If the file does not exist or the checksums do not match,
    /// a `ErrorKind::NotFound` io error will be returned.
    fn read_all(&self, key: FileKey) -> io::Result<Vec<u8>>;

    /// Writes a range of bytes to an existing file or creates on.
    ///
    /// Returns the new checksum of the file.
    fn write_range(&self, key: FileKey, range: Range<usize>, bytes: &[u8])
        -> io::Result<FileEntry>;

    /// Writes a buffer to a given file, creating a new file if it doesn't already exist.
    ///
    /// Returns the new checksum of the file.
    fn write_all(&self, key: FileKey, bytes: &[u8]) -> io::Result<FileEntry>;

    /// Removes a file from the directory.
    ///
    /// If the file does not exist the op is ignored.
    fn remove_file(&self, key: FileKey) -> io::Result<()>;

    /// Computes the checksum and length of a given file.
    fn compute_file_info(&self, key: FileKey) -> io::Result<FileInfo>;

    /// Computes the checksum of a buffer of bytes.
    fn compute_raw_checksum(&self, bytes: &[u8]) -> u32 {
        crc32fast::hash(bytes)
    }
}

pub struct FileBackedDirectory {
    base_path: PathBuf,
    opened_file_cache: Mutex<lru::LruCache<FileKey, Arc<File>>>,
    file_metadata: RwLock<HashMap<FileKey, FileEntry>>,
}

impl FileBackedDirectory {
    pub(crate) fn new(
        base_path: impl AsRef<Path>,
        max_fd: usize,
        existing_metadata: HashMap<FileKey, FileEntry>,
    ) -> Self {
        Self {
            base_path: base_path.as_ref().to_path_buf(),
            opened_file_cache: Mutex::new(lru::LruCache::new(max_fd)),
            file_metadata: RwLock::new(existing_metadata),
        }
    }

    fn get_file_path(&self, key: FileKey) -> PathBuf {
        self.base_path.join(key.to_string()).with_extension("data")
    }

    fn get_open_file(&self, key: FileKey) -> io::Result<Arc<File>> {
        // Explicitly defining the scope of the lock.
        {
            if let Some(file) = self.opened_file_cache.lock().get(&key) {
                return Ok(file.clone());
            }
        }

        let path = self.get_file_path(key);
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        Ok(self.put_open_file(key, file))
    }

    fn put_open_file(&self, key: FileKey, file: File) -> Arc<File> {
        let immutable_file = Arc::new(file);
        self.opened_file_cache
            .lock()
            .put(key, immutable_file.clone());
        immutable_file
    }

    fn try_get_file_metadata(&self, key: FileKey) -> io::Result<FileEntry> {
        self.file_metadata
            .read()
            .get(&key)
            .copied()
            .ok_or_else(not_found_error)
    }

    fn register_file_write(&self, key: FileKey, checksum: u32, file_size: u64) -> FileEntry {
        let entry = FileEntry::new(key, checksum, super::time_now(), file_size);
        self.file_metadata.write().insert(entry.key, entry);
        entry
    }

    fn register_file_removal(&self, key: FileKey) {
        {
            self.opened_file_cache.lock().get(&key);
        }

        {
            self.file_metadata.write().remove(&key);
        }
    }

    fn write_to_file(&self, file: &File, bytes: &[u8], offset: u64) -> io::Result<()> {
        file.write_all_at(bytes, offset)?;
        file.sync_data()?;
        Ok(())
    }

    fn write_range_inner(&self, key: FileKey, bytes: &[u8], offset: u64) -> io::Result<FileInfo> {
        let file = self.get_open_file(key)?;
        self.write_to_file(&file, bytes, offset)?;
        self.compute_file_info(key)
    }

    fn write_all_inner(&self, key: FileKey, bytes: &[u8]) -> io::Result<()> {
        let file = self.get_open_file(key)?;

        // If the file already exists, this will truncate the file to the new length
        // letting us directly overwrite the file contents. This will also
        // pre-allocate the blocks required when creating a new file.
        file.set_len(bytes.len() as u64)?;

        self.write_to_file(&file, bytes, 0)
    }
}

impl Directory for FileBackedDirectory {
    type Entries = IntoIter<FileEntry>;

    fn get_metadata(&self, key: FileKey) -> Option<FileEntry> {
        self.try_get_file_metadata(key).ok()
    }

    fn entries(&self) -> Self::Entries {
        self.file_metadata
            .read()
            .values()
            .copied()
            .collect::<Vec<FileEntry>>()
            .into_iter()
    }

    /// Reads a range of data from the file with the given key.
    ///
    /// Note:
    ///   It is possible for the buffer to be filled with intermediate `0` if the
    ///   selected range spans across parts of the file which 'effectively' do not
    ///   exist, this can occur when a file is made up purely of slices where the
    ///   OS will infill missing parts of the file with `0`s if the writer's slice
    ///   range lies beyond the end of the file.
    ///
    /// TODO: Come up with a smarter solution to storing slices.
    fn read_range(&self, key: FileKey, range: Range<usize>) -> io::Result<Vec<u8>> {
        let metadata = self.try_get_file_metadata(key)?;

        // Short circuit if the range is beyond bounds.
        if range.start as u64 >= metadata.file_size {
            return Ok(vec![]);
        }

        let start = range.start as u64;
        let end_pos = cmp::min(range.end as u64, metadata.file_size);
        let data_len = end_pos - start;

        let file = self.get_open_file(key)?;

        let mut buffer = vec![0; data_len as usize];
        read_to_fill_buff(&file, &mut buffer, start)?;

        Ok(buffer)
    }

    /// Reads all data from the file with the given key.
    ///
    /// Note:
    ///   It is possible for the buffer to be filled with intermediate `0` if the
    ///   selected range spans across parts of the file which 'effectively' do not
    ///   exist, this can occur when a file is made up purely of slices where the
    ///   OS will infill missing parts of the file with `0`s if the writer's slice
    ///   range lies beyond the end of the file.
    ///
    /// TODO: Come up with a smarter solution to storing slices.
    fn read_all(&self, key: FileKey) -> io::Result<Vec<u8>> {
        let metadata = self.try_get_file_metadata(key)?;

        let file = self.get_open_file(key)?;

        let mut buffer = vec![0; metadata.file_size as usize];
        read_to_fill_buff(&file, &mut buffer, 0)?;

        // Validate the checksum of the file because we have all the data.
        let checksum = crc32fast::hash(&buffer);
        if metadata.file_checksum != checksum {
            self.remove_file(key)?;
            return Err(not_found_error());
        }

        Ok(buffer)
    }

    fn write_range(
        &self,
        key: FileKey,
        range: Range<usize>,
        bytes: &[u8],
    ) -> io::Result<FileEntry> {
        // Although unlikely, we don't want to overwrite data we shouldn't be overwriting.
        // this just prevents us from affecting data we dont want to touch.
        let sub_slice = &bytes[0..range.len()];

        let info = match self.write_range_inner(key, sub_slice, range.start as u64) {
            Ok(info) => info,
            Err(primary_error) => {
                // We don't want to leave a random file laying around.
                return if let Err(secondary_error) = self.remove_file(key) {
                    Err(propagate_errors(primary_error, secondary_error))
                } else {
                    Err(primary_error)
                };
            }
        };

        let entry = self.register_file_write(key, info.checksum, info.file_size);

        Ok(entry)
    }

    fn write_all(&self, key: FileKey, bytes: &[u8]) -> io::Result<FileEntry> {
        if let Err(primary_error) = self.write_all_inner(key, bytes) {
            return if let Err(secondary_error) = self.remove_file(key) {
                Err(propagate_errors(primary_error, secondary_error))
            } else {
                Err(primary_error)
            };
        }

        let checksum = self.compute_raw_checksum(bytes);
        let entry = self.register_file_write(key, checksum, bytes.len() as u64);

        Ok(entry)
    }

    fn remove_file(&self, key: FileKey) -> io::Result<()> {
        let path = self.get_file_path(key);
        self.register_file_removal(key);

        match std::fs::remove_file(path) {
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            other => other,
        }
    }

    fn compute_file_info(&self, key: FileKey) -> io::Result<FileInfo> {
        let file = self.get_open_file(key)?;
        calculate_file_info(&file)
    }
}

fn not_found_error() -> io::Error {
    io::Error::new(ErrorKind::NotFound, "file not found")
}

fn propagate_errors(err1: io::Error, err2: io::Error) -> io::Error {
    io::Error::new(
        err1.kind(),
        format!(
            "Failed to complete IO operation: {}, during the handling of this error, another \
             error occurred: {}",
            err1, err2
        ),
    )
}

pub struct FileInfo {
    pub checksum: u32,
    pub file_size: u64,
}

fn calculate_file_info(file: &File) -> io::Result<FileInfo> {
    let mut hasher = crc32fast::Hasher::new();

    let file_size = file.metadata()?.len();
    let mut cursor = 0;

    let mut buffer = [0; 32 << 10];
    while cursor < file_size {
        let n = file.read_at(&mut buffer, cursor)?;

        if n == 0 {
            break;
        }

        hasher.write(&buffer[..n]);
        cursor += n as u64;
    }

    Ok(FileInfo {
        checksum: hasher.finalize(),
        file_size,
    })
}

fn read_to_fill_buff(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !buf.is_empty() {
        match file.read_at(buf, offset) {
            Ok(0) => break,
            Ok(n) => {
                let tmp = buf;
                buf = &mut tmp[n..];
                offset += n as u64;
            }
            Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::path::PathBuf;

    use super::*;
    use crate::cache::disk::time_now;

    fn random_tmp_dir() -> PathBuf {
        let path = temp_dir().join(rand::random::<u16>().to_string());
        std::fs::create_dir_all(&path).expect("create random path for test");

        path
    }

    #[test]
    fn test_directory_write_all() {
        let data = b"Hello, world. This is some sample data!";
        let expected_entry =
            FileEntry::new(1, crc32fast::hash(data), time_now(), data.len() as u64);

        let dir = random_tmp_dir();

        let directory = FileBackedDirectory::new(&dir, 2, HashMap::new());

        directory
            .write_all(expected_entry.key, data.as_ref())
            .expect("write data to disk.");

        {
            let lock = directory.opened_file_cache.lock();
            assert_eq!(lock.len(), 1, "Expected file to be cached.");
        }

        {
            let lock = directory.file_metadata.read();
            assert_eq!(
                lock.get(&1),
                Some(&expected_entry),
                "Expected entry stored in metadata to be the same."
            );
        }
    }

    #[test]
    fn test_directory_write_range() {
        let data = b"Hello, world. This is some sample data!";

        let expected_entry = FileEntry::new(1, crc32fast::hash(&data[..13]), time_now(), 13);

        let dir = random_tmp_dir();

        let directory = FileBackedDirectory::new(&dir, 2, HashMap::new());

        // We expect the writer to implicitly crop the input data to only
        // write out the given range.
        directory
            .write_range(expected_entry.key, 0..13, data.as_ref())
            .expect("write data to disk.");

        {
            let lock = directory.opened_file_cache.lock();
            assert_eq!(lock.len(), 1, "Expected file to be cached.");
        }

        {
            let lock = directory.file_metadata.read();
            assert_eq!(
                lock.get(&1),
                Some(&expected_entry),
                "Expected entry stored in metadata to be the same."
            );
        }

        let written_buffer = directory
            .read_all(expected_entry.key)
            .expect("read all data");
        assert_eq!(
            &written_buffer,
            &data[..13],
            "Expected on the first 13 bytes to be written to disk."
        );
        assert_eq!(
            crc32fast::hash(&written_buffer),
            expected_entry.file_checksum,
            "Expected buffer checksums to match."
        );
    }

    fn write_basic_contents(buffer: &[u8]) -> (FileBackedDirectory, FileEntry) {
        let expected_entry =
            FileEntry::new(1, crc32fast::hash(buffer), time_now(), buffer.len() as u64);

        let dir = random_tmp_dir();

        let directory = FileBackedDirectory::new(&dir, 2, HashMap::new());
        directory
            .write_all(expected_entry.key, buffer)
            .expect("write data to disk.");

        (directory, expected_entry)
    }

    #[test]
    fn test_directory_remove_file() {
        let data = b"Hello, world. This is some sample data!";

        let expected_entry =
            FileEntry::new(1, crc32fast::hash(data), time_now(), data.len() as u64);

        let dir = random_tmp_dir();

        let directory = FileBackedDirectory::new(&dir, 2, HashMap::new());
        directory
            .write_all(expected_entry.key, data.as_ref())
            .expect("write data to disk.");

        let expected_file_path = dir.join("1").with_extension("data");

        assert!(
            expected_file_path.exists(),
            "Expected file to exist after writing buffer."
        );

        directory.remove_file(1).expect("remove file");
        assert!(
            !expected_file_path.exists(),
            "Expected file to not exist after removing file from directory."
        );

        {
            let lock = directory.file_metadata.read();
            assert!(
                lock.is_empty(),
                "Expected metadata lookup to be empty after deleting file."
            );
        }

        let result = directory.read_all(expected_entry.key);
        assert!(
            result.is_err(),
            "Expected directory to return error after deleting file."
        );

        let err = result.unwrap_err();
        assert_eq!(
            err.kind(),
            ErrorKind::NotFound,
            "Expected directory to return NotFound ErrorKind after deleting file."
        );
    }

    #[test]
    fn test_read_all() {
        let data = b"Hello, world. This is some sample data!";
        let (directory, expected_entry) = write_basic_contents(data);

        let buffer = directory
            .read_all(expected_entry.key)
            .expect("read all data");

        assert_eq!(
            buffer.as_slice(),
            data.as_ref(),
            "Expected buffer read to equal buffer written."
        );
        assert_eq!(
            expected_entry.file_checksum,
            crc32fast::hash(data),
            "Expected buffer checksums to match."
        );
    }

    #[test]
    fn test_read_range() {
        let data = b"Hello, world. This is some sample data!";
        let (directory, expected_entry) = write_basic_contents(data);

        let buffer = directory
            .read_range(expected_entry.key, 0..13)
            .expect("read all data");

        assert_eq!(
            buffer.as_slice(),
            b"Hello, world.".as_ref(),
            "Expected buffer read to equal buffer slice."
        );
    }

    #[test]
    fn test_read_write_range_with_limitation() {
        let data = b"Hello, world. This is some sample data!";

        // This emulates the current limitation of writing a slice of data beyond
        // the length of a file will cause the system to infill with `0`s
        let mut expected_file_buffer = [0; 49];
        expected_file_buffer[10..].copy_from_slice(data.as_ref());

        let dir = random_tmp_dir();

        let directory = FileBackedDirectory::new(&dir, 2, HashMap::new());
        directory
            .write_range(1, 10..49, data.as_ref())
            .expect("write data to disk.");

        // Demonstrates the limitation where reading outside of previously written bounds
        // will cause the buffer to be passed with intermediate values.
        let buffer = directory.read_range(1, 0..23).expect("read all data");

        assert_eq!(
            &buffer[..10],
            &[0; 10],
            "Expected start of buffer to be zeroed."
        );
        assert_eq!(
            &buffer[10..],
            b"Hello, world.".as_ref(),
            "Expected rest of buffer to equal sample buffer slice."
        );
    }

    #[test]
    fn test_open_file_caching() {
        let data = b"Hello, world. This is some sample data!";

        let dir = random_tmp_dir();
        let directory = FileBackedDirectory::new(&dir, 2, HashMap::new());

        directory
            .write_all(1, data.as_ref())
            .expect("write data to disk.");
        directory
            .write_all(2, data.as_ref())
            .expect("write data to disk.");

        {
            let lock = directory.opened_file_cache.lock();
            assert_eq!(lock.len(), 2, "Expected 2 files to be cached.");
            assert_eq!(
                lock.peek_lru().map(|v| v.0),
                Some(&1),
                "File key `1` to be the first to be evicted."
            );
        }

        directory
            .write_all(3, data.as_ref())
            .expect("write data to disk.");
        directory
            .write_all(2, data.as_ref())
            .expect("write data to disk.");
        directory
            .write_all(3, data.as_ref())
            .expect("write data to disk.");
        directory
            .write_all(3, data.as_ref())
            .expect("write data to disk.");

        {
            let lock = directory.opened_file_cache.lock();
            assert_eq!(lock.len(), 2, "Expected 2 files to be cached.");
            assert_eq!(
                lock.peek_lru().map(|v| v.0),
                Some(&2),
                "File key `2` to be the first to be evicted."
            );
        }
    }
}
