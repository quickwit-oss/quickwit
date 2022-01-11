use std::io::{BufWriter, IntoInnerError};
use std::path::Path;
use std::sync::Arc;
use std::{fmt, io};

use arc_swap::ArcSwap;
use quickwit_actors::{KillSwitch, Progress, ProtectedZoneGuard};
use tantivy::common::{AntiCallToken, TerminatingWrite};
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{FileHandle, WatchCallback, WatchHandle, WritePtr};
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
    inner: Inner,
}



#[cfg(test)]
mod tests {
    use std::io::Write;
    use tantivy::directory::RamDirectory;
    use super::*;

    #[test]
    fn test_records_progress_on_write() -> anyhow::Result<()> {
        let directory = RamDirectory::default();
        let progress = Progress::default();
        let controlled_directory = 
            ControlledDirectory::new(Box::new(directory), progress.clone(), KillSwitch::default());
        assert!(progress.registered_activity_since_last_call());
        assert!(!progress.registered_activity_since_last_call());
        let mut wrt = controlled_directory.open_write(Path::new("test"))?;
        assert!(progress.registered_activity_since_last_call());
        //
        let large_buffer = vec![0u8; wrt.capacity() +  1];
        wrt.write_all(&large_buffer)?;


    }

    #[test]
    fn test_records_kill_switch_triggers_io_error() -> anyhow::Result<()> {
        let directory = RamDirectory::default();
        let kill_switch = KillSwitch::default();
        let controlled_directory = ControlledDirectory::new(
            Box::new(directory),
            Progress::default(),
            kill_switch.clone(),
        );
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