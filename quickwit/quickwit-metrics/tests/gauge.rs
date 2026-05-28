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
use quickwit_metrics::{Gauge, GaugeGuard, SYSTEM, gauge, label_names, label_values, labels};

#[test]
fn set() {
    let entries = with_recorder(|| {
        let g = gauge!(
            name: "g_set",
            description: "test gauge",
            subsystem: "test",
        );
        g.set(42.0);
    });

    assert_eq!(entries.len(), 1);
    let (name, labels, value) = &entries[0];
    assert_eq!(name, "quickwit_test_g_set");
    assert!(labels.is_empty());
    assert_eq!(*value, DebugValue::Gauge(42.0.into()));
}

#[test]
fn increment_decrement() {
    let entries = with_recorder(|| {
        let g = gauge!(
            name: "g_inc_dec",
            description: "inc/dec gauge",
            subsystem: "test",
        );
        g.set(10.0);
        g.inc_by(5.0);
        g.dec_by(3.0);
    });

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].2, DebugValue::Gauge(12.0.into()));
}

#[test]
fn parent_extends_labels() {
    let entries = with_recorder(|| {
        let parent = gauge!(
            name: "g_parent",
            description: "parent gauge",
            subsystem: "test",
            "env" => "prod",
        );
        let child = gauge!(parent: parent, "region" => "us-east");
        child.set(99.0);
    });

    let child_entry = entries.iter().find(|(_, labels, _)| labels.len() == 2);
    assert!(child_entry.is_some(), "child metric not found");
    let (name, labels, value) = child_entry.unwrap();
    assert_eq!(name, "quickwit_test_g_parent");
    assert_eq!(
        labels,
        &[
            ("env".to_string(), "prod".to_string()),
            ("region".to_string(), "us-east".to_string()),
        ]
    );
    assert_eq!(*value, DebugValue::Gauge(99.0.into()));
}

#[test]
fn guard_decrements_on_drop() {
    let entries = with_recorder(|| {
        let g = gauge!(
            name: "g_guard",
            description: "guard gauge",
            subsystem: "test",
        );
        g.set(0.0);
        {
            let _guard = GaugeGuard::new(&g, 5.0);
        }
    });

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].2, DebugValue::Gauge(0.0.into()));
}

#[test]
fn guard_after_set() {
    let entries = with_recorder(|| {
        let g = gauge!(
            name: "g_set_guard",
            description: "set then guard",
            subsystem: "test",
        );
        g.set(10.0);
        {
            let guard = GaugeGuard::new(&g, 3.0);
            assert_eq!(guard.delta(), 3.0);
        }
    });

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].2, DebugValue::Gauge(10.0.into()));
}

#[test]
fn guard_tracks_delta() {
    let entries = with_recorder(|| {
        let g = gauge!(
            name: "g_guard_delta",
            description: "guard delta",
            subsystem: "test",
        );
        g.set(0.0);
        {
            let guard = GaugeGuard::new(&g, 2.0);
            assert_eq!(guard.delta(), 2.0);
            guard.increment(5.0);
            guard.increment(-2.0);
            guard.increment(0.5);
            guard.increment(-1.5);
            assert_eq!(guard.delta(), 4.0);
        }
    });

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].2, DebugValue::Gauge(0.0.into()));
}

#[test]
fn multiple_guards() {
    let entries = with_recorder(|| {
        let g = gauge!(
            name: "g_multi_guard",
            description: "multiple guards",
            subsystem: "test",
        );
        g.set(0.0);
        let guard_a = GaugeGuard::new(&g, 2.0);
        let guard_b = GaugeGuard::new(&g, 5.0);
        drop(guard_b);
        drop(guard_a);
    });

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].2, DebugValue::Gauge(0.0.into()));
}

// ── Label composition ──

#[test]
fn label_composition_two_labels() {
    let entries = with_recorder(|| {
        let parent = gauge!(
            name: "g_compose_two",
            description: "two-label composition",
            subsystem: "test",
            "env" => "prod",
        );
        const REGION: quickwit_metrics::LabelNames<1> = label_names!("region");
        const STATUS: quickwit_metrics::LabelNames<1> = label_names!("status");
        let child = gauge!(parent: parent, labels: [
            label_values!(REGION => "us-east"),
            label_values!(STATUS => "ok"),
        ]);
        child.set(42.0);
    });

    let child_entry = entries.iter().find(|(_, labels, _)| labels.len() == 3);
    assert!(child_entry.is_some(), "composed child not found");
    let (name, labels, value) = child_entry.unwrap();
    assert_eq!(name, "quickwit_test_g_compose_two");
    assert_eq!(
        labels,
        &[
            ("env".to_string(), "prod".to_string()),
            ("region".to_string(), "us-east".to_string()),
            ("status".to_string(), "ok".to_string()),
        ]
    );
    assert_eq!(*value, DebugValue::Gauge(42.0.into()));
}

#[test]
fn label_composition_same_hash_as_single() {
    with_recorder(|| {
        let parent = gauge!(
            name: "g_compose_hash",
            description: "hash equivalence",
            subsystem: "test",
        );
        const REGION: quickwit_metrics::LabelNames<1> = label_names!("region");
        const STATUS: quickwit_metrics::LabelNames<1> = label_names!("status");

        let via_compose = gauge!(parent: parent, labels: [
            label_values!(REGION => "us"),
            label_values!(STATUS => "ok"),
        ]);
        let via_single = gauge!(
            parent: parent,
            labels: [labels!("region" => "us", "status" => "ok")],
        );
        via_compose.set(5.0);
        via_single.inc_by(3.0);

        assert_eq!(via_compose.get(), 8.0);
        assert_eq!(via_single.get(), 8.0);
    });
}

// ── Observable gauge ──

#[test]
fn observable_set_matches_recorder() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let g = with_local_recorder(&recorder, || {
        let g = gauge!(
            name: "og_set",
            description: "observable gauge",
            subsystem: "test",
        );
        g.set(42.0);
        g
    });

    assert_eq!(g.get(), 42.0);
    let snap = snapshotter.snapshot().into_vec();
    assert_eq!(snap[0].3, DebugValue::Gauge(42.0.into()));
}

#[test]
fn observable_inc_dec_matches_recorder() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let g = with_local_recorder(&recorder, || {
        let g = gauge!(
            name: "og_inc_dec",
            description: "observable inc/dec gauge",
            subsystem: "test",
        );
        g.set(10.0);
        g.inc_by(5.0);
        g.dec_by(3.0);
        g
    });

    assert_eq!(g.get(), 12.0);
    let snap = snapshotter.snapshot().into_vec();
    assert_eq!(snap[0].3, DebugValue::Gauge(12.0.into()));
}

#[test]
fn observable_guard_matches_recorder() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let g = with_local_recorder(&recorder, || {
        let g = gauge!(
            name: "og_guard",
            description: "observable gauge with guard",
            subsystem: "test",
        );
        g.set(0.0);
        {
            let _guard = GaugeGuard::new(&g, 5.0);
            assert_eq!(g.get(), 5.0);
        }
        g
    });

    assert_eq!(g.get(), 0.0);
    let snap = snapshotter.snapshot().into_vec();
    assert_eq!(snap[0].3, DebugValue::Gauge(0.0.into()));
}

#[test]
fn observable_parent_children_share_shadow() {
    with_recorder(|| {
        let parent = gauge!(
            name: "og_shared",
            description: "shared shadow gauge",
            subsystem: "test",
        );
        let child_a = gauge!(parent: parent, "region" => "us-east");
        let child_b = gauge!(parent: parent, "region" => "us-east");

        child_a.set(5.0);
        child_b.inc_by(3.0);

        assert_eq!(child_a.get(), 8.0);
        assert_eq!(child_b.get(), 8.0);
    });
}

#[test]
fn local_set_and_get() {
    let g = Gauge::local();
    assert_eq!(g.get(), 0.0);
    g.set(42.0);
    assert_eq!(g.get(), 42.0);
}

#[test]
fn local_increment_decrement() {
    let g = Gauge::local();
    g.inc_by(10.0);
    g.dec_by(3.0);
    assert_eq!(g.get(), 7.0);
}

#[test]
fn local_gauges_are_independent() {
    let a = Gauge::local();
    let b = Gauge::local();
    a.set(5.0);
    b.set(9.0);
    assert_eq!(a.get(), 5.0);
    assert_eq!(b.get(), 9.0);
}

#[test]
fn local_gauges_are_never_equal() {
    let a = Gauge::local();
    let b = Gauge::local();
    assert_ne!(a, b);
}

#[test]
fn local_gauge_clone_is_equal() {
    let a = Gauge::local();
    let b = a.clone();
    assert_eq!(a, b);
    a.set(7.0);
    assert_eq!(b.get(), 7.0);
}

#[test]
fn custom_system_key_name() {
    let entries = with_recorder(|| {
        let g = gauge!(
            name: "connections",
            description: "active connections",
            system: "myapp",
            subsystem: "db",
        );
        g.set(5.0);
    });

    assert_eq!(entries.len(), 1);
    let (name, _, value) = &entries[0];
    assert_eq!(name, "myapp_db_connections");
    assert_eq!(*value, DebugValue::Gauge(5.0.into()));
}

#[test]
fn default_system_key_name() {
    let entries = with_recorder(|| {
        let g = gauge!(
            name: "connections",
            description: "active connections",
            subsystem: "db",
        );
        g.set(1.0);
    });

    assert_eq!(entries.len(), 1);
    let (name, _, value) = &entries[0];
    assert_eq!(name, &format!("{SYSTEM}_db_connections"));
    assert_eq!(*value, DebugValue::Gauge(1.0.into()));
}

#[test]
fn empty_system_key_name() {
    let entries = with_recorder(|| {
        let g = gauge!(
            name: "connections",
            description: "active connections",
            system: "",
            subsystem: "db",
        );
        g.set(1.0);
    });

    assert_eq!(entries.len(), 1);
    let (name, _, value) = &entries[0];
    assert_eq!(name, "db_connections");
    assert_eq!(*value, DebugValue::Gauge(1.0.into()));
}

#[test]
fn empty_subsystem_key_name() {
    let entries = with_recorder(|| {
        let g = gauge!(
            name: "connections",
            description: "active connections",
            system: "myapp",
            subsystem: "",
        );
        g.set(1.0);
    });

    assert_eq!(entries.len(), 1);
    let (name, _, value) = &entries[0];
    assert_eq!(name, "myapp_connections");
    assert_eq!(*value, DebugValue::Gauge(1.0.into()));
}

#[test]
fn empty_system_and_subsystem_key_name() {
    let entries = with_recorder(|| {
        let g = gauge!(
            name: "connections",
            description: "active connections",
            system: "",
            subsystem: "",
        );
        g.set(1.0);
    });

    assert_eq!(entries.len(), 1);
    let (name, _, value) = &entries[0];
    assert_eq!(name, "connections");
    assert_eq!(*value, DebugValue::Gauge(1.0.into()));
}

#[test]
fn const_system_key_name() {
    const MY_SYSTEM: &str = "custom";
    let entries = with_recorder(|| {
        let g = gauge!(
            name: "pool_size",
            description: "pool size",
            system: MY_SYSTEM,
            subsystem: "db",
        );
        g.set(8.0);
    });

    assert_eq!(entries.len(), 1);
    let (name, _, value) = &entries[0];
    assert_eq!(name, "custom_db_pool_size");
    assert_eq!(*value, DebugValue::Gauge(8.0.into()));
}

#[test]
fn custom_separator_key_name() {
    let entries = with_recorder(|| {
        let g = gauge!(
            name: "connections",
            description: "active connections",
            system: "myapp",
            subsystem: "db",
            separator: ".",
        );
        g.set(5.0);
    });

    assert_eq!(entries.len(), 1);
    let (name, _, value) = &entries[0];
    assert_eq!(name, "myapp.db.connections");
    assert_eq!(*value, DebugValue::Gauge(5.0.into()));
}

#[test]
fn default_system_custom_separator_key_name() {
    let entries = with_recorder(|| {
        let g = gauge!(
            name: "connections",
            description: "active connections",
            subsystem: "db",
            separator: ".",
        );
        g.set(5.0);
    });

    assert_eq!(entries.len(), 1);
    let (name, _, value) = &entries[0];
    assert_eq!(name, "quickwit.db.connections");
    assert_eq!(*value, DebugValue::Gauge(5.0.into()));
}
