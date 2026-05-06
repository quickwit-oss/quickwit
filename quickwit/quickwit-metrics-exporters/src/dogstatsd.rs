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

use anyhow::Context;
use metrics_exporter_dogstatsd::DogStatsDRecorder;

pub(crate) fn build_recorder(service_version: &str) -> anyhow::Result<DogStatsDRecorder> {
    // Reading both `CLOUDPREM_*` and `CP_*` env vars for backward compatibility. The former is
    // deprecated and can be removed after 2026-04-01.
    let host: String = quickwit_common::get_from_env_opt("CLOUDPREM_DOGSTATSD_SERVER_HOST", false)
        .unwrap_or_else(|| {
            quickwit_common::get_from_env(
                "CP_DOGSTATSD_SERVER_HOST",
                "127.0.0.1".to_string(),
                false,
            )
        });
    let port: u16 = quickwit_common::get_from_env_opt("CLOUDPREM_DOGSTATSD_SERVER_PORT", false)
        .unwrap_or_else(|| quickwit_common::get_from_env("CP_DOGSTATSD_SERVER_PORT", 8125, false));
    let addr = format!("{host}:{port}");

    let mut global_labels = vec![::metrics::Label::new(
        "version",
        service_version.to_string(),
    )];
    let keys = [
        ("IMAGE_NAME", "image_name"),
        ("IMAGE_TAG", "image_tag"),
        ("KUBERNETES_COMPONENT", "kube_component"),
        ("KUBERNETES_NAMESPACE", "kube_namespace"),
        ("KUBERNETES_POD_NAME", "kube_pod_name"),
        ("QW_CLUSTER_ID", "cloudprem_cluster_id"),
        ("QW_NODE_ID", "cloudprem_node_id"),
    ];
    for (env_var_key, label_key) in keys {
        if let Some(label_val) = quickwit_common::get_from_env_opt::<String>(env_var_key, false) {
            global_labels.push(::metrics::Label::new(label_key, label_val));
        }
    }
    let recorder = metrics_exporter_dogstatsd::DogStatsDBuilder::default()
        .set_global_prefix("cloudprem")
        .with_global_labels(global_labels)
        .with_remote_address(addr)
        .context("failed to parse DogStatsD server address")?
        .build()
        .context("failed to build DogStatsD exporter")?;
    quickwit_dst::invariants::set_invariant_recorder(invariant_recorder);
    Ok(recorder)
}

fn invariant_recorder(invariant_id: quickwit_dst::invariants::InvariantId, passed: bool) {
    let name = invariant_id.as_str();
    metrics::counter!("pomsky.invariant.checked", "invariant" => name).increment(1);
    if !passed {
        metrics::counter!("pomsky.invariant.violated", "invariant" => name).increment(1);
    }
}
