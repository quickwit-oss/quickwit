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
use quickwit_metrics::counter;

#[test]
fn base_increments() {
    let entries = with_recorder(|| {
        let c = counter!(
            name: "c_base",
            description: "test counter",
            subsystem: "test",
        );
        c.increment(5);
    });

    assert_eq!(entries.len(), 1);
    let (name, labels, value) = &entries[0];
    assert_eq!(name, "quickwit_test_c_base");
    assert!(labels.is_empty());
    assert_eq!(*value, DebugValue::Counter(5));
}

#[test]
fn base_with_static_labels() {
    let entries = with_recorder(|| {
        let c = counter!(
            name: "c_labels",
            description: "labeled counter",
            subsystem: "test",
            "env" => "staging",
        );
        c.increment(1);
    });

    assert_eq!(entries.len(), 1);
    let (_, labels, _) = &entries[0];
    assert_eq!(labels, &[("env".to_string(), "staging".to_string())]);
}

#[test]
fn parent_extends_labels() {
    let entries = with_recorder(|| {
        let parent = counter!(
            name: "c_parent",
            description: "parent counter",
            subsystem: "test",
            "env" => "prod",
        );
        let child = counter!(parent: parent, "region" => "us-east");
        child.increment(10);
    });

    let child_entry = entries.iter().find(|(_, labels, _)| labels.len() == 2);
    assert!(child_entry.is_some(), "child metric not found");
    let (name, labels, value) = child_entry.unwrap();
    assert_eq!(name, "quickwit_test_c_parent");
    assert_eq!(
        labels,
        &[
            ("env".to_string(), "prod".to_string()),
            ("region".to_string(), "us-east".to_string()),
        ]
    );
    assert_eq!(*value, DebugValue::Counter(10));
}

#[test]
fn absolute_sets_value() {
    let entries = with_recorder(|| {
        let c = counter!(
            name: "c_abs",
            description: "absolute counter",
            subsystem: "test",
        );
        c.absolute(42);
    });

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].2, DebugValue::Counter(42));
}

#[test]
fn parent_with_dynamic_label_value() {
    let entries = with_recorder(|| {
        let parent = counter!(
            name: "c_dyn",
            description: "dynamic label parent",
            subsystem: "test",
        );
        let region = String::from("ap-south");
        let child = counter!(parent: parent, "region" => region);
        child.increment(7);
    });

    let child_entry = entries.iter().find(|(_, labels, _)| labels.len() == 1);
    assert!(child_entry.is_some());
    let (_, labels, value) = child_entry.unwrap();
    assert_eq!(labels, &[("region".to_string(), "ap-south".to_string())]);
    assert_eq!(*value, DebugValue::Counter(7));
}

#[test]
fn nested_parent_extension() {
    let entries = with_recorder(|| {
        let base = counter!(
            name: "c_nested",
            description: "nested parent",
            subsystem: "test",
            "env" => "prod",
        );
        let child = counter!(parent: base, "region" => "eu");
        let grandchild = counter!(parent: child, "az" => "eu-1a");
        grandchild.increment(1);
    });

    let gc = entries.iter().find(|(_, labels, _)| labels.len() == 3);
    assert!(gc.is_some(), "grandchild metric not found");
    let (name, labels, value) = gc.unwrap();
    assert_eq!(name, "quickwit_test_c_nested");
    assert_eq!(
        labels,
        &[
            ("env".to_string(), "prod".to_string()),
            ("region".to_string(), "eu".to_string()),
            ("az".to_string(), "eu-1a".to_string()),
        ]
    );
    assert_eq!(*value, DebugValue::Counter(1));
}

// ── Observable counter ──

#[test]
fn observable_get_matches_recorder() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let c = with_local_recorder(&recorder, || {
        let c = counter!(
            name: "oc_get",
            description: "observable counter",
            subsystem: "test",
        );
        c.increment(3);
        c.increment(7);
        c
    });

    assert_eq!(c.get(), 10);
    let snap = snapshotter.snapshot().into_vec();
    assert_eq!(snap[0].3, DebugValue::Counter(10));
}

#[test]
fn observable_absolute_matches_recorder() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let c = with_local_recorder(&recorder, || {
        let c = counter!(
            name: "oc_abs",
            description: "observable absolute counter",
            subsystem: "test",
        );
        c.absolute(42);
        c
    });

    assert_eq!(c.get(), 42);
    let snap = snapshotter.snapshot().into_vec();
    assert_eq!(snap[0].3, DebugValue::Counter(42));
}

#[test]
fn observable_parent_children_share_shadow() {
    with_recorder(|| {
        let parent = counter!(
            name: "oc_shared",
            description: "shared shadow counter",
            subsystem: "test",
        );
        let child_a = counter!(parent: parent, "region" => "us-east");
        let child_b = counter!(parent: parent, "region" => "us-east");

        child_a.increment(3);
        child_b.increment(7);

        assert_eq!(child_a.get(), 10);
        assert_eq!(child_b.get(), 10);
    });
}

#[test]
fn observable_parent_distinct_labels_separate_shadow() {
    with_recorder(|| {
        let parent = counter!(
            name: "oc_distinct",
            description: "distinct shadow counter",
            subsystem: "test",
        );
        let child_a = counter!(parent: parent, "region" => "us-east");
        let child_b = counter!(parent: parent, "region" => "eu-west");

        child_a.increment(3);
        child_b.increment(7);

        assert_eq!(child_a.get(), 3);
        assert_eq!(child_b.get(), 7);
    });
}
