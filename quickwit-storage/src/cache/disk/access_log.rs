use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::Read;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::time::Duration;

use crossbeam::channel::{self, Receiver, Sender};
use tracing::{error, info, warn};

use super::file::{FileEntry, FileKey};

pub(crate) type Metadata = HashMap<FileKey, FileEntry>;

/// Opens an existing access log contained within the given directory or creates a new log.
pub(crate) fn open_access_log(base_path: &Path) -> io::Result<Log> {
    let mut file = open_log_file(base_path)?;

    let data = load_and_verify_file(&mut file)?;
    let metadata = get_files(&data);

    let (tx, rx) = channel::bounded(5);

    start_background_task(rx, data, file);

    Ok(Log {
        writer: AccessLogWriter(tx),
        metadata,
    })
}

pub(crate) struct Log {
    pub(crate) writer: AccessLogWriter,
    pub(crate) metadata: Metadata,
}

fn open_log_file(base_path: &Path) -> io::Result<File> {
    let file = File::options()
        .create(true)
        .read(true)
        .write(true)
        .open(base_path.join("files").with_extension("meta"))?;

    Ok(file)
}

fn get_files(data: &LoadedData) -> Metadata {
    let mut metadata = Metadata::new();

    for (key, index) in data.key_mapping.iter() {
        let entry = data.entries[*index];
        metadata.insert(*key, entry);
    }

    metadata
}

pub(crate) struct LoadedData {
    free_slots: Vec<usize>,
    entries: Vec<FileEntry>,
    key_mapping: HashMap<FileKey, usize>,
}

fn load_and_verify_file(file: &mut File) -> io::Result<LoadedData> {
    let mut free_slots = vec![];
    let mut entries = vec![];
    let mut key_mapping = HashMap::new();

    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    for chunk in buffer.chunks(FileEntry::RAW_SIZE) {
        if chunk.len() < FileEntry::RAW_SIZE {
            continue;
        }

        match FileEntry::from_bytes(chunk) {
            Ok(entry) => {
                let slot_index = entries.len();
                if entry.is_empty() {
                    free_slots.push(slot_index);
                }

                key_mapping.insert(entry.key, slot_index);
                entries.push(entry);
            }
            Err(e) => {
                warn!(
                    "Cache Corruption: Corrupted row detected within the persistent cache log. \
                     Skipping row, this may affect initial cache performance."
                );

                // A corrupted row can be overwritten.
                free_slots.push(entries.len());
                entries.push(FileEntry::EMPTY);
            }
        }
    }

    Ok(LoadedData {
        free_slots,
        entries,
        key_mapping,
    })
}

#[derive(Copy, Clone)]
pub(crate) enum Event {
    ReadAccess { file_key: u64, accessed_at: u64 },
    Write { entry: FileEntry },
    Remove { file_key: u64 },
    Shutdown,
}

#[derive(Clone)]
pub(crate) struct AccessLogWriter(Sender<Event>);

impl AccessLogWriter {
    fn send_event(&self, event: Event) {
        // TODO: Should this be handled?
        let _ = self.0.send(event);
    }

    pub(crate) fn register_read(&self, file_key: u64, accessed_at: Duration) {
        self.send_event(Event::ReadAccess {
            file_key,
            accessed_at: accessed_at.as_secs(),
        })
    }

    pub(crate) fn register_write(&self, entry: FileEntry) {
        self.send_event(Event::Write { entry })
    }

    pub(crate) fn register_removal(&self, file_key: u64) {
        self.send_event(Event::Remove { file_key })
    }
}

impl Drop for AccessLogWriter {
    fn drop(&mut self) {
        let _ = self.0.send(Event::Shutdown);
    }
}

fn start_background_task(changes: Receiver<Event>, data: LoadedData, writer: File) {
    std::thread::Builder::new()
        .name("disk-cache-background-worker".to_string())
        .spawn(move || {
            let instance = FileAccessTask {
                pending_changes: changes,
                free_slots: data.free_slots,
                entries: data.entries,
                key_mapping: data.key_mapping,
                writer,
            };

            instance.start()
        })
        .expect("spawn background thread"); // TODO: Handle error.
}

struct FileAccessTask {
    pending_changes: Receiver<Event>,

    /// A queue of free slots that can be overwritten
    /// without needing to extend the file.
    free_slots: Vec<usize>,

    /// A set of file entry rows.
    /// This essentially mirrors the layout of rows on disk including their order.
    entries: Vec<FileEntry>,

    /// A lookup dictionary to map a given file key to their relevant index where
    /// the file entry lies within `entries`.
    key_mapping: HashMap<FileKey, usize>,

    writer: File,
}

impl FileAccessTask {
    fn start(mut self) {
        loop {
            // Attempt to pop events off of the backlog.
            while let Ok(event) = self.pending_changes.try_recv() {
                if let Event::Shutdown = event {
                    info!("Cache Shutdown: Received terminating event. Flushing buffers.");
                    self.try_flush_data();
                    return;
                }

                if let Err(e) = self.handle_event(event) {
                    error!(error = ?e, "IO Cache Error: Failed to handle event due to the the given IO error.");
                }
            }

            self.try_flush_data();

            // Wait until the next set of events come through.
            match self.pending_changes.recv() {
                Ok(event) => {
                    if let Event::Shutdown = event {
                        info!("Cache Shutdown: Received terminating event. Flushing buffers.");
                        self.try_flush_data();
                        return;
                    }

                    if let Err(e) = self.handle_event(event) {
                        error!(error = ?e, "IO Cache Error: Failed to handle event due to the the given IO error.");
                    }
                }
                Err(_) => return,
            }
        }
    }

    fn try_flush_data(&mut self) {
        if let Err(e) = self.writer.sync_data() {
            error!(
                error = ?e,
                "IO Cache Flush Error: Failed to flush cache entries log, some data \
                may not be persisted and affect cache performance on fresh start."
            );
        }
    }

    fn handle_event(&mut self, event: Event) -> io::Result<()> {
        match event {
            Event::ReadAccess {
                file_key,
                accessed_at,
            } => self.handle_read(file_key, accessed_at),
            Event::Write { entry } => self.handle_write(entry),
            Event::Remove { file_key } => self.handle_remove(file_key),
            _ => Ok(()),
        }
    }

    fn handle_read(&mut self, file_key: u64, accessed_at: u64) -> io::Result<()> {
        let slot_index = match self.key_mapping.get(&file_key) {
            None => return Ok(()),
            Some(file_id) => *file_id,
        };

        let metadata = match self.entries.get_mut(slot_index) {
            None => {
                self.key_mapping.remove(&file_key);
                return Ok(());
            }
            Some(metadata) => {
                // Update the last access timestamp within the entries.
                metadata.last_accessed = accessed_at;

                *metadata
            }
        };

        self.write_at_index(slot_index, metadata)?;

        Ok(())
    }

    fn handle_write(&mut self, entry: FileEntry) -> io::Result<()> {
        // Get a valid slot index that is garenteed to be within bound of the entries vector.
        let slot_index = match self.key_mapping.get(&entry.key).copied() {
            Some(slot_index) => {
                if slot_index < self.entries.len() {
                    slot_index
                } else {
                    self.get_next_free_slot()
                }
            }
            None => self.get_next_free_slot(),
        };

        if let Err(e) = self.write_at_index(slot_index, entry) {
            // The file operation has failed, we should mark the location where
            // the entry would have been stored as free so future operations can
            // make use of this space.
            self.free_slots.push(slot_index);
            Err(e)
        } else {
            self.entries[slot_index] = entry;
            self.key_mapping.insert(entry.key, slot_index);
            Ok(())
        }
    }

    fn handle_remove(&mut self, file_key: u64) -> io::Result<()> {
        let slot_index = match self.key_mapping.remove(&file_key) {
            // If the file doesn't already exist then we don't attempt to track it.
            // We only track files registered via the Write event.
            None => return Ok(()),
            Some(file_id) => file_id,
        };

        // This should never happen, but we really don't want to panic if this
        // ever is out of bounds, nore do we want to be needlessly extending our file.
        if slot_index >= self.entries.len() {
            return Ok(());
        }

        self.entries[slot_index] = FileEntry::EMPTY;

        self.write_at_index(slot_index, FileEntry::EMPTY)?;

        // Mark the slot as available to overwrite.
        self.free_slots.push(slot_index);

        Ok(())
    }

    fn write_at_index(&mut self, slot_index: usize, data: FileEntry) -> io::Result<()> {
        // The writer will extend the file if the offset lies beyond the length of the file,
        // although the offset shouldn't normally lie beyond the length of the file for anything
        // larger than one row (length_of_file + FileEntry::RAW_SIZE)
        //
        // In the event that the offset calculated lies beyond more than one row, we assume
        // that the empty rows will be infilled over time as new files are added.
        let file_offset = slot_index * FileEntry::RAW_SIZE;
        let raw_data = data.to_bytes();

        self.writer.write_all_at(&raw_data, file_offset as u64)?;

        Ok(())
    }

    fn get_next_free_slot(&mut self) -> usize {
        // Keep popping off free slots until a valid slot is found.
        // This protects against any out of bounds panics due to a free slot
        // not being within range. Although this should never happen.
        while let Some(free_slot) = self.free_slots.pop() {
            if free_slot < self.entries.len() {
                return free_slot;
            }
        }

        self.entries.push(FileEntry::EMPTY);
        self.entries.len() - 1
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::io::{Seek, SeekFrom};
    use std::os::unix::fs::FileExt;
    use std::path::PathBuf;

    use super::*;

    fn random_tmp_dir() -> PathBuf {
        let path = temp_dir().join(rand::random::<u16>().to_string());
        std::fs::create_dir_all(&path).expect("create random path for test");

        path
    }

    #[test]
    fn test_create_new_log_file() {
        let dir = random_tmp_dir();

        open_access_log(&dir).expect("create new access log file.");
    }

    #[test]
    fn test_write_basic_log_data_to_file() {
        let entry = FileEntry::new(1, 1234, Duration::from_secs(1), 325254);

        let dir = random_tmp_dir();

        let log = open_access_log(&dir).expect("create new access log file.");

        log.writer.register_write(entry);

        // Drop the log handle, let it shut the background writer down.
        drop(log);

        // Let the background worker handle the task.
        std::thread::sleep(Duration::from_millis(100));

        let mut file = open_log_file(&dir).expect("open existing log file");

        let mut all_data = vec![];
        file.read_to_end(&mut all_data).expect("read all data");

        assert_eq!(
            all_data.len(),
            FileEntry::RAW_SIZE,
            "Expected access log file to have written one file entry. Expected {} bytes, found {} \
             bytes",
            FileEntry::RAW_SIZE,
            all_data.len(),
        );

        let stored_entry =
            FileEntry::from_bytes(&all_data).expect("deserialize file entry from buffer");
        assert_eq!(
            stored_entry, entry,
            "Expected entry stored on disk to be the same as sample entry."
        );
    }

    fn write_entries_to_tmp_file(entries: impl Iterator<Item = FileEntry>) -> PathBuf {
        let dir = random_tmp_dir();

        let log = open_access_log(&dir).expect("create new access log file.");

        for entry in entries {
            log.writer.register_write(entry);
        }

        // Drop the log handle, let it shut the background writer down.
        drop(log);

        // Let the background worker handle the task.
        std::thread::sleep(Duration::from_millis(100));

        dir
    }

    #[test]
    fn test_write_many_entries_data_from_file() {
        let entry1 = FileEntry::new(1, 1234, Duration::from_secs(1), 325254);
        let entry2 = FileEntry::new(2, 43535, Duration::from_secs(2), 21141);
        let entry3 = FileEntry::new(3, 364577, Duration::from_secs(3), 3453);

        let entries = [entry1, entry2, entry3];

        let dir = write_entries_to_tmp_file(entries.iter().copied());

        let mut file = open_log_file(&dir).expect("open existing log file");
        let existing_data = load_and_verify_file(&mut file).expect("successful read");

        assert_eq!(
            existing_data.entries.len(),
            entries.len(),
            "Expected {} entries within the file. Got {}",
            entries.len(),
            existing_data.entries.len(),
        );

        assert!(
            existing_data.free_slots.is_empty(),
            "Expected no free slots to be present. This indicated corruption of the file data. \
             Slots: {:?}",
            existing_data.free_slots
        );

        // We can assume because no file deletions have occurred that the order of the rows
        // are the order that we inserted them.
        // Note: This immediately becomes invalid when any removal operators occur.
        let iterator = existing_data.entries.into_iter().zip(entries).enumerate();

        for (index, (on_disk, sample)) in iterator {
            assert_ne!(
                on_disk,
                FileEntry::EMPTY,
                "Expected no empty rows at index {}",
                index
            );
            assert_eq!(
                on_disk, sample,
                "Expected on disk entry to match sample entry expected at index {}",
                index
            );
        }
    }

    #[test]
    fn test_remove_entries_from_file() {
        let entry1 = FileEntry::new(1, 1234, Duration::from_secs(1), 325254);
        let entry2 = FileEntry::new(2, 43535, Duration::from_secs(2), 21141);
        let entry3 = FileEntry::new(3, 364577, Duration::from_secs(3), 3453);

        let entries = [entry1, entry2, entry3];

        let dir = random_tmp_dir();

        let log = open_access_log(&dir).expect("create new access log file.");

        for entry in entries {
            log.writer.register_write(entry);
        }

        // Remove the middle row.
        log.writer.register_removal(entry2.key);

        // Drop the log handle, let it shut the background writer down.
        drop(log);

        // Let the background worker handle the task.
        std::thread::sleep(Duration::from_millis(100));

        let mut file = open_log_file(&dir).expect("open existing log file");
        let mut existing_data = load_and_verify_file(&mut file).expect("successful read");

        // Our entries length should be the same, as one should be an empty row.
        assert_eq!(
            existing_data.entries.len(),
            entries.len(),
            "Expected {} entries within the file. Got {}",
            entries.len(),
            existing_data.entries.len(),
        );
        assert_eq!(
            existing_data.free_slots.len(),
            1,
            "Expected 1 free slots to be present. Got {}",
            existing_data.free_slots.len()
        );
        assert_eq!(
            existing_data.free_slots.pop(),
            Some(1),
            "Expected marked free slot to be located at index position 1."
        );
    }

    #[test]
    fn test_corruption_recovery_basic_log_data() {
        let entry = FileEntry::new(1, 1234, Duration::from_secs(1), 325254);
        let dir = write_entries_to_tmp_file([entry].into_iter());

        let mut file = open_log_file(&dir).expect("open existing log file");

        // Overwrite the file contents with some arbitrary data. Simulating the state
        // corruption or partial writes would leave the rows in.
        file.write_all_at(&[0, 12, 234, 3, 3, 5], 0)
            .expect("overwrite file contents");

        let mut all_data = vec![];
        file.read_to_end(&mut all_data).expect("read all data");

        // We don't expect the file length to change.
        assert_eq!(
            all_data.len(),
            FileEntry::RAW_SIZE,
            "Expected access log file to have written one file entry. Expected {} file length of \
             file, found {} length of file",
            FileEntry::RAW_SIZE,
            all_data.len(),
        );

        file.seek(SeekFrom::Start(0)).expect("seek back to start");
        let mut existing_data = load_and_verify_file(&mut file).expect("successful read");

        // We still expect a entry to be present. We just expect it to be marked as free to be
        // overwritten.
        assert_eq!(
            existing_data.entries.len(),
            1,
            "Expected 1 entry to be present within the existing data."
        );
        assert_eq!(
            existing_data.free_slots.len(),
            1,
            "Expected the 1 entry to be marked as free."
        );
        assert!(
            existing_data.key_mapping.is_empty(),
            "Expected no file keys to be mapped to any entries."
        );
        assert_eq!(
            existing_data.entries.pop(),
            Some(FileEntry::EMPTY),
            "Expected entry to be marked as empty."
        );
    }
}
