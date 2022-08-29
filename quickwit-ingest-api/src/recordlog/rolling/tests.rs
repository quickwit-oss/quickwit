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
//

use tempfile::tempdir;

use super::RecordLogReader;
use crate::recordlog::Record;

#[tokio::test]
async fn test_record_log_reader_empty() {
    let tempdir = tempdir().unwrap();
    let mut record_log_reader = RecordLogReader::open(tempdir.path()).await.unwrap();
    assert!(record_log_reader.read_record().await.unwrap().is_none());
}

#[tokio::test]
async fn test_record_log_reader_simple() {
    let tempdir = tempdir().unwrap();
    {
        let mut record_log_reader = RecordLogReader::open(tempdir.path()).await.unwrap();
        assert!(record_log_reader.read_record().await.unwrap().is_none());
        let mut record_log_writer = record_log_reader.into_writer();
        let record0 = Record::AddRecord {
            position: 0,
            queue: "queue",
            payload: b"hello0",
        };
        let record1 = Record::AddRecord {
            position: 1,
            queue: "queue",
            payload: b"hello1",
        };
        record_log_writer.write_record(record0).await.unwrap();
        record_log_writer.write_record(record1).await.unwrap();
        record_log_writer.flush().await.unwrap();
        let mut record_log_reader = RecordLogReader::open(tempdir.path()).await.unwrap();
        assert_eq!(
            record_log_reader.read_record().await.unwrap(),
            Some(record0)
        );
        assert_eq!(
            record_log_reader.read_record().await.unwrap(),
            Some(record1)
        );
        let mut record_log_writer = record_log_reader.into_writer();
        record_log_writer.flush().await.unwrap()
    }
    {
        let mut record_log_reader = RecordLogReader::open(tempdir.path()).await.unwrap();
        let record0 = Record::AddRecord {
            position: 0,
            queue: "queue",
            payload: b"hello0",
        };
        let record1 = Record::AddRecord {
            position: 1,
            queue: "queue",
            payload: b"hello1",
        };
        assert_eq!(
            record_log_reader.read_record().await.unwrap(),
            Some(record0)
        );
        assert_eq!(
            record_log_reader.read_record().await.unwrap(),
            Some(record1)
        );
        let mut record_log_writer = record_log_reader.into_writer();
        let record2 = Record::AddRecord {
            position: 2,
            queue: "queue",
            payload: b"hello2",
        };
        record_log_writer.write_record(record2).await.unwrap();
        record_log_writer.flush().await.unwrap()
    }
}
