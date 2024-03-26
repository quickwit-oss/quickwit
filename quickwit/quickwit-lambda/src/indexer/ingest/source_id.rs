// Copyright (C) 2024 Quickwit, Inc.
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

use std::collections::BTreeMap;

use anyhow::{bail, Context};
use chrono::{DateTime, LocalResult, TimeZone, Utc};

const LAMBDA_SOURCE_ID_PREFIX: &str = "ingest-lambda-source-";

/// This duration should be large enough to prevent repeated notification
/// deliveries from causing duplicates
const FILE_SOURCE_RETENTION_HOURS: usize = 6;

/// Create a source id for a Lambda file source, with the provided timestamp encoded in it
pub(crate) fn create_lambda_source_id(time: DateTime<Utc>) -> String {
    format!("{}{}", LAMBDA_SOURCE_ID_PREFIX, time.timestamp())
}

fn parse_source_id_timestamp(source_id: &str) -> anyhow::Result<DateTime<Utc>> {
    let ts = source_id[LAMBDA_SOURCE_ID_PREFIX.len()..]
        .parse::<i64>()
        .context("Lambda source id doesn't contain a UNIX timestamp")?;
    if let LocalResult::Single(parsed_ts) = Utc.timestamp_opt(ts, 0) {
        Ok(parsed_ts)
    } else {
        bail!("Lambda source id contains an invalid UNIX timestamp")
    }
}

/// Parse the provided source ids and return those where the file source is
/// older than `MIN_FILE_SOURCE_RETENTION_HOURS` hours
pub(crate) fn filter_prunable_lambda_source_ids<'a>(
    source_ids: impl Iterator<Item = &'a String>,
) -> anyhow::Result<impl Iterator<Item = &'a String>> {
    let src_timestamps = source_ids
        .filter(|src_id| src_id.starts_with(LAMBDA_SOURCE_ID_PREFIX))
        .map(|src_id| Ok((parse_source_id_timestamp(src_id)?, src_id)))
        .collect::<anyhow::Result<BTreeMap<_, _>>>()?;

    let prunable_sources = src_timestamps
        .into_iter()
        .rev()
        .filter(|(ts, _)| (Utc::now() - *ts).num_hours() > FILE_SOURCE_RETENTION_HOURS as i64)
        .map(|(_, src_id)| src_id);

    Ok(prunable_sources)
}

/// Check whether the provided source was created by the Lambda indexer
pub(crate) fn is_lambda_source_id(source_id: &str) -> bool {
    source_id.starts_with(LAMBDA_SOURCE_ID_PREFIX)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use quickwit_config::{CLI_SOURCE_ID, INGEST_API_SOURCE_ID, INGEST_V2_SOURCE_ID};

    use super::*;

    #[test]
    fn test_basic_roundtrip() {
        let time = Utc.timestamp_opt(1711034138, 0).unwrap();
        let source_id = create_lambda_source_id(time);
        let parsed_time = parse_source_id_timestamp(&source_id).unwrap();
        assert_eq!(time, parsed_time);
    }

    #[test]
    fn test_dont_filter_recent() {
        let source_ids: Vec<String> = (0..20)
            .map(|i| {
                // only recent timestamps
                Utc::now() - chrono::Duration::try_seconds(i as i64).unwrap()
            })
            .map(create_lambda_source_id)
            .collect();
        let prunable = filter_prunable_lambda_source_ids(source_ids.iter())
            .unwrap()
            .count();
        assert_eq!(prunable, 0);
    }

    #[test]
    fn test_filter_old() {
        let source_ids: Vec<String> = (0..5)
            .map(|i| {
                let hours_ago = i * FILE_SOURCE_RETENTION_HOURS * 2;
                Utc::now() - chrono::Duration::try_hours(hours_ago as i64).unwrap()
            })
            .map(create_lambda_source_id)
            .collect();

        // Prune source ids that happen to be from newest to oldest
        let prunable_sources = filter_prunable_lambda_source_ids(source_ids.iter())
            .unwrap()
            .collect::<HashSet<_>>();
        assert_eq!(prunable_sources.len(), 4);
        assert!(!prunable_sources.contains(&source_ids[0]));
        for source_id in source_ids.iter().skip(1) {
            assert!(prunable_sources.contains(source_id));
        }

        // Prune source ids that happen to be from oldest to newst
        let prunable_sources = filter_prunable_lambda_source_ids(source_ids.iter().rev())
            .unwrap()
            .collect::<HashSet<_>>();
        assert_eq!(prunable_sources.len(), 4);
        assert!(!prunable_sources.contains(&source_ids[0]));
        for source_id in source_ids.iter().skip(1) {
            assert!(prunable_sources.contains(source_id));
        }
    }

    #[test]
    fn test_reserverd_are_not_lambda_source_id() {
        assert!(!is_lambda_source_id(INGEST_V2_SOURCE_ID));
        assert!(!is_lambda_source_id(CLI_SOURCE_ID));
        assert!(!is_lambda_source_id(INGEST_API_SOURCE_ID));
    }
}
