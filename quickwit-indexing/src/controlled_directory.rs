// Copyright (C) 2021 Quickwit, Inc.
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
use std::path::Path;
use std::sync::Arc;
use std::{fmt, io};

use quickwit_actors::is_thread_local_kill_switch_alive;
use tantivy::common::{AntiCallToken, TerminatingWrite};
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{FileHandle, WatchCallback, WatchHandle, WritePtr};
use tantivy::Directory;
use tracing::info;

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
    inner: Arc<dyn Directory>,
}

fn check_if_alive() -> io::Result<()> {
    info!("checkifalive");
    if is_thread_local_kill_switch_alive() {
        Ok(())
    } else {
        Err(io::Error::new(io::ErrorKind::Other, ""))
    }
}

impl ControlledDirectory {
    pub fn new(directory: Box<dyn Directory>) -> ControlledDirectory {
        ControlledDirectory {
            inner: directory.into(),
        }
    }
}

impl fmt::Debug for ControlledDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ControlledDirectory").finish()
    }
}

struct ControlledWrite {
    underlying_wrt: Box<dyn TerminatingWrite>,
}

impl io::Write for ControlledWrite {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        check_if_alive()?;
        self.underlying_wrt.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        // We voluntarily avoid to check the kill switch on flush.
        // This is because the RAMDirectory currently panics if flush
        // is not called before Drop.
        let _ = check_if_alive();
        self.underlying_wrt.flush()
    }

    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        check_if_alive()?;
        self.underlying_wrt.write_vectored(bufs)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        check_if_alive()?;
        self.underlying_wrt.write_all(buf)
    }

    fn write_fmt(&mut self, fmt: fmt::Arguments<'_>) -> io::Result<()> {
        check_if_alive()?;
        self.underlying_wrt.write_fmt(fmt)
    }
}

impl TerminatingWrite for ControlledWrite {
    #[inline]
    fn terminate_ref(&mut self, token: AntiCallToken) -> io::Result<()> {
        self.underlying_wrt.flush()?;
        self.underlying_wrt.terminate_ref(token)
    }
}

impl Directory for ControlledDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Box<dyn FileHandle>, OpenReadError> {
        check_if_alive()
            .map_err(|io_err| OpenReadError::wrap_io_error(io_err, path.to_path_buf()))?;
        self.inner.get_file_handle(path)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        check_if_alive().map_err(|io_error| DeleteError::IoError {
            io_error,
            filepath: path.to_path_buf(),
        })?;
        self.inner.delete(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        check_if_alive()
            .map_err(|io_err| OpenReadError::wrap_io_error(io_err, path.to_path_buf()))?;
        self.inner.exists(path)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        check_if_alive()
            .map_err(|io_err| OpenWriteError::wrap_io_error(io_err, path.to_path_buf()))?;

        let underlying_wrt: Box<dyn TerminatingWrite> = self
            .inner
            .open_write(path)?
            .into_inner()
            .map_err(IntoInnerError::into_error)
            .map_err(|io_err| OpenWriteError::wrap_io_error(io_err, path.to_path_buf()))?;
        let controlled_wrt = ControlledWrite { underlying_wrt };
        Ok(BufWriter::with_capacity(
            BUFFER_NUM_BYTES,
            Box::new(controlled_wrt),
        ))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        check_if_alive()
            .map_err(|io_err| OpenReadError::wrap_io_error(io_err, path.to_path_buf()))?;
        self.inner.atomic_read(path)
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        check_if_alive()?;
        self.inner.atomic_write(path, data)
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        check_if_alive()?;
        self.inner.watch(watch_callback)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use quickwit_actors::{set_thread_locals_controls, KillSwitch, Progress};
    use tantivy::directory::RamDirectory;

    use super::*;

    #[test]
    fn test_records_progress_on_write() -> anyhow::Result<()> {
        let directory = RamDirectory::default();
        let progress = Progress::default();
        set_thread_locals_controls(progress.clone(), KillSwitch::default());
        let controlled_directory = ControlledDirectory::new(Box::new(directory));
        assert!(progress.registered_activity_since_last_call());
        assert!(!progress.registered_activity_since_last_call());
        let mut wrt = controlled_directory.open_write(Path::new("test"))?;
        assert!(progress.registered_activity_since_last_call());
        // We use a large buffer to force the buf writer to flush at least once.
        let large_buffer = vec![0u8; wrt.capacity() + 1];
        wrt.write_all(&large_buffer)?;
        assert!(progress.registered_activity_since_last_call());
        wrt.write_all(b"small payload")?;
        // Here we check that the progress only concerns is only
        // trigger when the BufWriter flushes.
        assert!(!progress.registered_activity_since_last_call());
        wrt.write_all(&large_buffer)?;
        assert!(progress.registered_activity_since_last_call());
        assert!(!progress.registered_activity_since_last_call());
        wrt.terminate()?;
        assert!(progress.registered_activity_since_last_call());
        Ok(())
    }

    #[test]
    fn test_records_kill_switch_triggers_io_error() -> anyhow::Result<()> {
        let directory = RamDirectory::default();
        let kill_switch = KillSwitch::default();
        set_thread_locals_controls(Progress::default(), kill_switch.clone());
        let controlled_directory = ControlledDirectory::new(Box::new(directory));
        let mut wrt = controlled_directory.open_write(Path::new("test"))?;
        // We use a large buffer to force the buf writer to flush at least once.
        let large_buffer = vec![0u8; wrt.capacity() + 1];
        wrt.write_all(&large_buffer)?;
        kill_switch.kill();
        let err = wrt.write_all(&large_buffer).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        wrt.terminate()?;
        Ok(())
    }
}
