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
use quickwit_metrics::{GaugeGuard, gauge};

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
        g.increment(5.0);
        g.decrement(3.0);
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
            let mut _guard = GaugeGuard::from_gauge(&g);
            _guard.increment(5.0);
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
            let mut guard = GaugeGuard::from_gauge(&g);
            guard.increment(3.0);
            assert_eq!(guard.get(), 3.0);
        }
    });

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].2, DebugValue::Gauge(10.0.into()));
}

#[test]
fn mutable_guard_tracks_delta() {
    let entries = with_recorder(|| {
        let g = gauge!(
            name: "g_mutable_guard",
            description: "mutable guard",
            subsystem: "test",
        );
        g.set(0.0);
        {
            let mut guard = GaugeGuard::from_gauge(&g);
            assert_eq!(guard.get(), 0.0);
            guard.increment(5.0);
            guard.increment(-2.0);
            guard.increment(0.5);
            guard.increment(-1.5);
            assert_eq!(guard.get(), 2.0);
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
        let mut guard_a = GaugeGuard::from_gauge(&g);
        guard_a.increment(2.0);
        let mut guard_b = GaugeGuard::from_gauge(&g);
        guard_b.increment(5.0);
        drop(guard_b);
        drop(guard_a);
    });

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].2, DebugValue::Gauge(0.0.into()));
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
            observable: true
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
            observable: true
        );
        g.set(10.0);
        g.increment(5.0);
        g.decrement(3.0);
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
            observable: true
        );
        g.set(0.0);
        {
            let mut _guard = GaugeGuard::from_gauge(&g);
            _guard.increment(5.0);
            assert_eq!(g.get(), 5.0);
        }
        g
    });

    assert_eq!(g.get(), 0.0);
    let snap = snapshotter.snapshot().into_vec();
    assert_eq!(snap[0].3, DebugValue::Gauge(0.0.into()));
}

#[test]
fn non_observable_returns_nan() {
    with_recorder(|| {
        let g = gauge!(
            name: "nog_get",
            description: "non-observable gauge",
            subsystem: "test",
        );
        g.set(99.0);
        assert!(g.get().is_nan());
    });
}

#[test]
fn observable_parent_children_share_shadow() {
    with_recorder(|| {
        let parent = gauge!(
            name: "og_shared",
            description: "shared shadow gauge",
            subsystem: "test",
            observable: true
        );
        let child_a = gauge!(parent: parent, "region" => "us-east");
        let child_b = gauge!(parent: parent, "region" => "us-east");

        child_a.set(5.0);
        child_b.increment(3.0);

        assert_eq!(child_a.get(), 8.0);
        assert_eq!(child_b.get(), 8.0);
    });
}
