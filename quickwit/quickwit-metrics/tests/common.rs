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

use metrics::with_local_recorder;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};

pub type MetricEntry = (String, Vec<(String, String)>, DebugValue);

/// Installs a thread-local `DebuggingRecorder`, runs the closure,
/// then returns a snapshot of all recorded metrics as
/// `(name, labels, value)` tuples.
pub fn with_recorder(f: impl FnOnce()) -> Vec<MetricEntry> {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    with_local_recorder(&recorder, f);
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .map(|(composite_key, _unit, _desc, value)| {
            let (_, key) = composite_key.into_parts();
            let name = key.name().to_string();
            let labels: Vec<(String, String)> = key
                .labels()
                .map(|l| (l.key().to_string(), l.value().to_string()))
                .collect();
            (name, labels, value)
        })
        .collect()
}
