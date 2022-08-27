use std::iter;

use super::{ReadRecordError, RecordReader, RecordWriter};
use crate::recordlog::frame::{BLOCK_LEN, HEADER_LEN};

#[tokio::test]
async fn test_no_data() {
    let mut reader = RecordReader::open(&b""[..]);
    assert_eq!(reader.read_record::<&str>().await.unwrap(), None);
}

#[tokio::test]
async fn test_empty_record() {
    let mut buffer = Vec::new();
    let mut writer = RecordWriter::open(&mut buffer);
    writer.write_record("").await.unwrap();
    writer.flush().await.unwrap();
    let mut reader = RecordReader::open(&buffer[..]);
    assert_eq!(reader.read_record::<&str>().await.unwrap(), Some(""));
    assert_eq!(reader.read_record::<&str>().await.unwrap(), None);
}

#[tokio::test]
async fn test_simple_record() {
    let mut buffer = Vec::new();
    let mut writer = RecordWriter::open(&mut buffer);
    let record = "hello";
    writer.write_record(record).await.unwrap();
    writer.flush().await.unwrap();
    let mut reader = RecordReader::open(&buffer[..]);
    assert!(matches!(
        reader.read_record::<&str>().await,
        Ok(Some("hello"))
    ));
    assert!(matches!(reader.read_record::<&str>().await, Ok(None)));
}

fn make_long_entry(len: usize) -> String {
    iter::repeat('A').take(len).collect()
}

#[tokio::test]
async fn test_spans_over_more_than_one_block() {
    let mut buffer = Vec::new();
    let long_entry: String = make_long_entry(80_000);
    let mut writer = RecordWriter::open(&mut buffer);
    writer.write_record(long_entry.as_str()).await.unwrap();
    writer.flush().await.unwrap();
    let mut reader = RecordReader::open(&buffer[..]);
    let record_payload: &str = reader.read_record().await.unwrap().unwrap();
    assert_eq!(record_payload, &long_entry);
    assert_eq!(reader.read_record::<&str>().await.unwrap(), None);
}

#[tokio::test]
async fn test_block_requires_padding() {
    let mut buffer = Vec::new();
    // We'll miss 1 byte to be able to fit our next chunk header in the
    // first block.
    let long_record = make_long_entry(BLOCK_LEN - HEADER_LEN - HEADER_LEN - 1 - 8);
    let short_record = "hello";
    let mut writer = RecordWriter::open(&mut buffer);
    writer.write_record(long_record.as_str()).await.unwrap();
    writer.write_record(short_record).await.unwrap();
    writer.flush().await.unwrap();
    let mut reader = RecordReader::open(&buffer[..]);
    assert_eq!(
        reader.read_record::<&str>().await.unwrap(),
        Some(long_record.as_str())
    );
    assert_eq!(
        reader.read_record::<&str>().await.unwrap(),
        Some(short_record)
    );
    assert_eq!(reader.read_record::<&str>().await.unwrap(), None);
}

#[tokio::test]
async fn test_first_chunk_empty() {
    let mut buffer = Vec::new();
    // We'll miss 1 byte to be able to fit our next chunk header in the
    // first block.
    let long_record = make_long_entry(BLOCK_LEN - HEADER_LEN - HEADER_LEN);
    let short_record = "hello";
    let mut writer = RecordWriter::open(&mut buffer);
    writer.write_record(&long_record[..]).await.unwrap();
    writer.write_record(short_record).await.unwrap();
    writer.flush().await.unwrap();
    let mut reader = RecordReader::open(&buffer[..]);
    assert_eq!(
        reader.read_record::<&str>().await.unwrap(),
        Some(long_record.as_str())
    );
    assert_eq!(
        reader.read_record::<&str>().await.unwrap(),
        Some(short_record)
    );
    assert_eq!(reader.read_record::<&str>().await.unwrap(), None);
}

#[tokio::test]
async fn test_behavior_upon_corruption() {
    let mut buffer = Vec::new();
    let records: Vec<String> = (0..1_000).map(|i| format!("hello{}", i)).collect();
    {
        let mut writer = RecordWriter::open(&mut buffer);
        for record in &records {
            writer.write_record(record.as_str()).await.unwrap();
        }
        writer.flush().await.unwrap();
    }
    {
        let mut reader = RecordReader::open(&buffer[..]);
        for record in &records {
            assert_eq!(
                reader.read_record::<&str>().await.unwrap(),
                Some(record.as_str())
            );
        }
        assert_eq!(reader.read_record::<&str>().await.unwrap(), None);
    }
    buffer[1_000] = 3;
    {
        let mut reader = RecordReader::open(&buffer[..]);
        for record in &records[0..72] {
            // bug at i=72
            assert_eq!(
                reader.read_record::<&str>().await.unwrap(),
                Some(record.as_str())
            );
        }
        assert!(matches!(
            reader.read_record::<&str>().await,
            Err(ReadRecordError::Corruption)
        ));
    }
}
