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

use std::io::{BufWriter, IntoInnerError};
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use std::{fmt, io};

use arc_swap::ArcSwap;
use quickwit_common::io::{ControlledWrite, IoControls, IoControlsAccess};
use quickwit_common::ProtectedZoneGuard;
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    AntiCallToken, FileHandle, TerminatingWrite, WatchCallback, WatchHandle, WritePtr,
};
use tantivy::Directory;

/// Buffer capacity.
///
/// This is the current default for the BufWriter, but considering this constant
/// will have a direct impact on health check, we'd better fix it.
const BUFFER_NUM_BYTES: usize = 8_192;

/// The `ControlledDirectory` wraps another directory and enhances it
/// with functionalities such as
/// - records progress everytime a write (Note there is however a buffer writer above it)
/// - if the killswitch is activated, returns an error on the first write happening after it
/// - in the future, record a writing speed, possibly introduce some throttling, etc.
#[derive(Clone)]
pub struct ControlledDirectory {
    underlying: Arc<dyn Directory>,
    io_controls: HotswappableIoControls,
}

impl ControlledDirectory {
    pub fn new(directory: Box<dyn Directory>, io_controls: IoControls) -> ControlledDirectory {
        ControlledDirectory {
            underlying: directory.into(),
            io_controls: HotswappableIoControls::new(io_controls),
        }
    }

    pub fn check_if_alive(&self) -> io::Result<ProtectedZoneGuard> {
        self.io_controls.load().check_if_alive()
    }

    pub fn set_io_controls(&self, io_controls: IoControls) {
        self.io_controls.store(Arc::new(io_controls));
    }
}

impl fmt::Debug for ControlledDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ControlledDirectory").finish()
    }
}

impl Directory for ControlledDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        self.check_if_alive()
            .map_err(|io_err| OpenReadError::wrap_io_error(io_err, path.to_path_buf()))?;
        self.underlying.get_file_handle(path)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        self.check_if_alive()
            .map_err(|io_error| DeleteError::IoError {
                io_error: Arc::new(io_error),
                filepath: path.to_path_buf(),
            })?;
        self.underlying.delete(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.check_if_alive()
            .map_err(|io_err| OpenReadError::wrap_io_error(io_err, path.to_path_buf()))?;
        self.underlying.exists(path)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        self.check_if_alive()
            .map_err(|io_err| OpenWriteError::wrap_io_error(io_err, path.to_path_buf()))?;

        let underlying_wrt: Box<dyn TerminatingWrite> = self
            .underlying
            .open_write(path)?
            .into_inner()
            .map_err(IntoInnerError::into_error)
            .map_err(|io_err| OpenWriteError::wrap_io_error(io_err, path.to_path_buf()))?;
        let controlled_wrt = self.io_controls.clone().wrap_write(underlying_wrt);
        Ok(BufWriter::with_capacity(
            BUFFER_NUM_BYTES,
            Box::new(AdoptedControlledWrite(controlled_wrt)),
        ))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.check_if_alive()
            .map_err(|io_err| OpenReadError::wrap_io_error(io_err, path.to_path_buf()))?;
        self.underlying.atomic_read(path)
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.check_if_alive()?;
        self.underlying.atomic_write(path, data)
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.check_if_alive()?;
        self.underlying.watch(watch_callback)
    }

    fn sync_directory(&self) -> io::Result<()> {
        self.check_if_alive()?;
        self.underlying.sync_directory()
    }
}

#[derive(Clone)]
struct HotswappableIoControls(Arc<ArcSwap<IoControls>>);

impl Deref for HotswappableIoControls {
    type Target = ArcSwap<IoControls>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl HotswappableIoControls {
    pub fn new(io_controls: IoControls) -> Self {
        Self(Arc::new(ArcSwap::new(Arc::new(io_controls))))
    }
}

impl IoControlsAccess for HotswappableIoControls {
    fn apply<F, R>(&self, f: F) -> R
    where F: Fn(&IoControls) -> R {
        let guard = self.0.load();
        f(&guard)
    }
}

// Wrapper to work around the orphan rule. (hence the word "Adopted").
struct AdoptedControlledWrite(ControlledWrite<HotswappableIoControls, Box<dyn TerminatingWrite>>);

impl io::Write for AdoptedControlledWrite {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl TerminatingWrite for AdoptedControlledWrite {
    #[inline]
    fn terminate_ref(&mut self, token: AntiCallToken) -> io::Result<()> {
        let underlying_wrt = self.0.underlying_wrt();
        underlying_wrt.flush()?;
        underlying_wrt.terminate_ref(token)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tantivy::directory::RamDirectory;

    use super::*;

    #[test]
    fn test_records_progress_on_write() -> anyhow::Result<()> {
        let directory = RamDirectory::default();
        let io_controls = IoControls::default();
        let controlled_directory =
            ControlledDirectory::new(Box::new(directory), io_controls.clone());
        let progress = io_controls.progress().clone();
        assert!(progress.registered_activity_since_last_call());
        assert!(!progress.registered_activity_since_last_call());
        let mut wrt = controlled_directory.open_write(Path::new("test"))?;
        assert!(progress.registered_activity_since_last_call());
        // We use a large buffer to force the buf writer to flush at least once.
        let large_buffer = vec![0u8; wrt.capacity() + 1];
        assert_eq!(io_controls.num_bytes(), 0u64);
        wrt.write_all(&large_buffer)?;
        assert_eq!(io_controls.num_bytes(), 8_193u64);
        assert!(progress.registered_activity_since_last_call());
        wrt.write_all(b"small payload")?;
        // The buffering makes it so that this last write does not
        // get actually written right away.
        assert_eq!(io_controls.num_bytes(), 8_193u64);
        // Here we check that the progress only concerns is only
        // trigger when the BufWriter flushes.
        assert!(!progress.registered_activity_since_last_call());
        wrt.write_all(&large_buffer)?;
        assert_eq!(io_controls.num_bytes(), 16_399);
        assert!(progress.registered_activity_since_last_call());
        assert!(!progress.registered_activity_since_last_call());
        wrt.write_all(&b"aa"[..])?;
        assert_eq!(io_controls.num_bytes(), 16_399u64);
        wrt.terminate()?;
        // Flush works as expected and makes sure all data buffered goes through
        assert_eq!(io_controls.num_bytes(), 16_401u64);
        assert!(progress.registered_activity_since_last_call());
        Ok(())
    }

    #[test]
    fn test_records_kill_switch_triggers_io_error() -> anyhow::Result<()> {
        let directory = RamDirectory::default();
        let io_controls = IoControls::default();
        let controlled_directory =
            ControlledDirectory::new(Box::new(directory), io_controls.clone());
        let mut wrt = controlled_directory.open_write(Path::new("test"))?;
        // We use a large buffer to force the buf writer to flush at least once.
        let large_buffer = vec![0u8; wrt.capacity() + 1];
        wrt.write_all(&large_buffer)?;
        io_controls.kill();
        let err = wrt.write_all(&large_buffer).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        wrt.terminate()?;
        Ok(())
    }
}
