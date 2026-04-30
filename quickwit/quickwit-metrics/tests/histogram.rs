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

mod common;

use common::with_recorder;
use metrics::with_local_recorder;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use quickwit_metrics::histogram;

#[test]
fn base_records_value() {
    let entries = with_recorder(|| {
        let h = histogram!(
            name: "h_base",
            description: "test histogram",
            subsystem: "test",
            buckets: vec![1.0, 5.0, 10.0]
        );
        h.record(3.5);
    });

    assert_eq!(entries.len(), 1);
    let (name, labels, value) = &entries[0];
    assert_eq!(name, "quickwit_test_h_base");
    assert!(labels.is_empty());
    match value {
        DebugValue::Histogram(vals) => {
            assert_eq!(vals.len(), 1);
            assert_eq!(vals[0].into_inner(), 3.5);
        }
        other => panic!("expected Histogram, got {other:?}"),
    }
}

#[test]
fn base_with_static_labels() {
    let entries = with_recorder(|| {
        let h = histogram!(
            name: "h_labels",
            description: "labeled histogram",
            subsystem: "test",
            buckets: vec![1.0],
            "env" => "prod",
            "region" => "us",
        );
        h.record(0.5);
    });

    assert_eq!(entries.len(), 1);
    let (name, labels, _) = &entries[0];
    assert_eq!(name, "quickwit_test_h_labels");
    assert_eq!(
        labels,
        &[
            ("env".to_string(), "prod".to_string()),
            ("region".to_string(), "us".to_string()),
        ]
    );
}

#[test]
fn parent_extends_labels() {
    let entries = with_recorder(|| {
        let parent = histogram!(
            name: "h_parent",
            description: "parent histogram",
            subsystem: "test",
            buckets: vec![1.0],
            "env" => "prod",
        );
        let child = histogram!(parent: parent, "region" => "eu");
        child.record(0.1);
    });

    let child_entry = entries.iter().find(|(_, labels, _)| labels.len() == 2);
    assert!(child_entry.is_some(), "child metric not found");
    let (name, labels, _) = child_entry.unwrap();
    assert_eq!(name, "quickwit_test_h_parent");
    assert_eq!(
        labels,
        &[
            ("env".to_string(), "prod".to_string()),
            ("region".to_string(), "eu".to_string()),
        ]
    );
}

#[test]
fn config_stored() {
    let recorder = DebuggingRecorder::new();
    let h = with_local_recorder(&recorder, || {
        histogram!(
            name: "h_cfg",
            description: "config test",
            subsystem: "sub",
            buckets: vec![1.0, 2.0]
        )
    });

    let config = h.__info();
    assert_eq!(config.info.name, "h_cfg");
    assert_eq!(config.info.subsystem, "sub");
    assert_eq!(config.info.key_name, "quickwit_sub_h_cfg");
    assert_eq!(config.info.description, "config test");
    assert_eq!((config.buckets_fn)(), vec![1.0, 2.0]);
}
