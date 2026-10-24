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

use std::collections::BTreeSet;

use quickwit_config::{CLI_SOURCE_ID, INGEST_API_SOURCE_ID, INGEST_V2_SOURCE_ID};
use quickwit_proto::metastore::SourceType;

use crate::model::ControlPlaneModel;

const RESERVED_SOURCE_IDS: [&str; 3] = [CLI_SOURCE_ID, INGEST_API_SOURCE_ID, INGEST_V2_SOURCE_ID];

pub(crate) fn update_telemetry_cluster_summary(model: &ControlPlaneModel) {
    let (num_indexes, source_types) = cluster_summary(model);
    quickwit_telemetry::set_cluster_summary(num_indexes, source_types);
}

fn cluster_summary(model: &ControlPlaneModel) -> (usize, BTreeSet<String>) {
    let source_types = model
        .source_configs()
        .filter(|(source_uid, _)| !RESERVED_SOURCE_IDS.contains(&source_uid.source_id.as_str()))
        .filter_map(|(_, source_config)| source_type_name(source_config.source_type()))
        .map(str::to_string)
        .collect();
    (model.num_indexes(), source_types)
}

fn source_type_name(source_type: SourceType) -> Option<&'static str> {
    match source_type {
        SourceType::Cli => Some("cli"),
        SourceType::File => Some("file"),
        SourceType::IngestV1 => Some("ingest_v1"),
        SourceType::IngestV2 => Some("ingest_v2"),
        SourceType::Kafka => Some("kafka"),
        SourceType::Kinesis => Some("kinesis"),
        SourceType::Nats => Some("nats"),
        SourceType::PubSub => Some("pubsub"),
        SourceType::Pulsar => Some("pulsar"),
        SourceType::Stdin => Some("stdin"),
        SourceType::Vec => Some("vec"),
        SourceType::Void => Some("void"),
        SourceType::Unspecified => None,
    }
}

#[cfg(test)]
mod tests {
    use quickwit_config::{SourceConfig, SourceParams};
    use quickwit_metastore::IndexMetadata;

    use super::*;

    #[test]
    fn test_cluster_summary_leaves_out_reserved_sources() {
        let mut model = ControlPlaneModel::default();
        assert_eq!(cluster_summary(&model), (0, BTreeSet::new()));

        let mut index_metadata = IndexMetadata::for_test("test-index", "ram:///indexes");
        index_metadata.add_source(SourceConfig::cli()).unwrap();
        index_metadata
            .add_source(SourceConfig::ingest_v2())
            .unwrap();
        index_metadata
            .add_source(SourceConfig::for_test("my-source", SourceParams::void()))
            .unwrap();
        model.add_index(index_metadata);
        model.add_index(IndexMetadata::for_test("other-index", "ram:///indexes"));

        assert_eq!(
            cluster_summary(&model),
            (2, BTreeSet::from(["void".to_string()]))
        );
    }
}
