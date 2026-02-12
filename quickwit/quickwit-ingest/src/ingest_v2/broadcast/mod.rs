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

#[allow(dead_code)]
mod ingester_capacity;
mod local_shards;

use std::time::Duration;

use quickwit_proto::types::SourceUid;

pub(in crate::ingest_v2) const BROADCAST_INTERVAL_PERIOD: Duration = if cfg!(test) {
    Duration::from_millis(50)
} else {
    Duration::from_secs(5)
};

pub use local_shards::{
    BroadcastLocalShardsTask, LocalShardsUpdate, ShardInfo, ShardInfos,
    setup_local_shards_update_listener,
};

fn make_key(prefix: &str, source_uid: &SourceUid) -> String {
    format!("{prefix}{}:{}", source_uid.index_uid, source_uid.source_id)
}

fn parse_key(key: &str) -> Option<SourceUid> {
    let (index_uid_str, source_id_str) = key.rsplit_once(':')?;
    Some(SourceUid {
        index_uid: index_uid_str.parse().ok()?,
        source_id: source_id_str.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use quickwit_common::shared_consts::INGESTER_PRIMARY_SHARDS_PREFIX;
    use quickwit_proto::types::{IndexUid, SourceId, SourceUid};

    use super::*;

    #[test]
    fn test_make_key() {
        let source_uid = SourceUid {
            index_uid: IndexUid::for_test("test-index", 0),
            source_id: SourceId::from("test-source"),
        };
        let key = make_key(INGESTER_PRIMARY_SHARDS_PREFIX, &source_uid);
        assert_eq!(
            key,
            "ingester.primary_shards:test-index:00000000000000000000000000:test-source"
        );
    }

    #[test]
    fn test_parse_key() {
        let key = "test-index:00000000000000000000000000:test-source";
        let source_uid = parse_key(key).unwrap();
        assert_eq!(
            &source_uid.index_uid.to_string(),
            "test-index:00000000000000000000000000"
        );
        assert_eq!(source_uid.source_id, "test-source".to_string());
    }
}
