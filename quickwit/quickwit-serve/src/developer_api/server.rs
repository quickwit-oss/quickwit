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

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use bytesize::ByteSize;
use prometheus::proto::MetricType;
use quickwit_actors::Mailbox;
use quickwit_cluster::Cluster;
use quickwit_config::NodeConfig;
use quickwit_config::service::QuickwitService;
use quickwit_control_plane::control_plane::{ControlPlane, GetDebugInfo};
use quickwit_ingest::{IngestRouter, Ingester};
use quickwit_proto::cloudprem::metrics::metric::MetricValue;
use quickwit_proto::cloudprem::metrics::*;
use quickwit_proto::developer::{
    DeveloperError, DeveloperResult, DeveloperService, GetDebugInfoRequest, GetDebugInfoResponse,
    GetNodeDiagnosticsRequest, GetNodeDiagnosticsResponse, PullMetricsResponse,
};
use serde_json::json;

use crate::{BuildInfo, EnvInfo, QuickwitServices, RuntimeInfo};

#[derive(Clone)]
pub(crate) struct DeveloperApiServer {
    node_config: Arc<NodeConfig>,
    cluster: Cluster,
    control_plane_mailbox_opt: Option<Mailbox<ControlPlane>>,
    ingest_router_opt: Option<IngestRouter>,
    ingester_opt: Option<Ingester>,
}

impl fmt::Debug for DeveloperApiServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeveloperApiServer").finish()
    }
}

impl DeveloperApiServer {
    pub const MAX_GRPC_MESSAGE_SIZE: ByteSize = ByteSize::mib(100);

    pub fn from_services(services: &QuickwitServices) -> Self {
        Self {
            node_config: services.node_config.clone(),
            cluster: services.cluster.clone(),
            control_plane_mailbox_opt: services.control_plane_server_opt.clone(),
            ingest_router_opt: services.ingest_router_opt.clone(),
            ingester_opt: services.ingester_opt.clone(),
        }
    }
}

#[async_trait]
impl DeveloperService for DeveloperApiServer {
    async fn get_debug_info(
        &self,
        request: GetDebugInfoRequest,
    ) -> DeveloperResult<GetDebugInfoResponse> {
        let roles: HashSet<QuickwitService> = request
            .roles
            .into_iter()
            .map(|role| role.parse())
            .collect::<anyhow::Result<_>>()
            .map_err(|error| DeveloperError::InvalidArgument(error.to_string()))?;

        let cluster_snapshot = self.cluster.snapshot().await;

        // We must redact sensitive information such as credentials.
        let mut node_config = (*self.node_config).clone();
        node_config.redact();

        let mut debug_info = json!({
            "build_info": BuildInfo::get(),
            "env_info": EnvInfo::get(),
            "runtime_info": RuntimeInfo::get(),
            "node_config": node_config,
            "cluster_membership_info": json!({
                "ready_nodes": cluster_snapshot.ready_nodes,
                "live_nodes": cluster_snapshot.live_nodes,
                "dead_nodes": cluster_snapshot.dead_nodes,
                "chitchat_state": cluster_snapshot.chitchat_state_snapshot.node_states,
            })
        });
        if let Some(control_plane_mailbox) = &self.control_plane_mailbox_opt
            && (roles.is_empty() || roles.contains(&QuickwitService::ControlPlane))
        {
            debug_info["control_plane"] = match control_plane_mailbox.ask(GetDebugInfo).await {
                Ok(debug_info) => debug_info,
                Err(error) => {
                    json!({"error": error.to_string()})
                }
            };
        }
        if let Some(ingest_router) = &self.ingest_router_opt {
            debug_info["ingest_router"] = ingest_router.debug_info().await;
        }
        if let Some(ingester) = &self.ingester_opt
            && (roles.is_empty() || roles.contains(&QuickwitService::Indexer))
        {
            debug_info["ingester"] = ingester.debug_info().await;
        };
        let debug_info_json = serde_json::to_vec(&debug_info).map_err(|error| {
            let message = format!("failed to JSON serialize debug info: {error}");
            DeveloperError::Internal(message)
        })?;
        let response = GetDebugInfoResponse {
            debug_info_json: Bytes::from(debug_info_json),
        };
        Ok(response)
    }

    async fn pull_metrics(
        &self,
        _: quickwit_proto::developer::PullMetricsRequest,
    ) -> DeveloperResult<PullMetricsResponse> {
        let metric_families_proto: Vec<MetricFamily> = prometheus::default_registry()
            .gather()
            .into_iter()
            .flat_map(convert_metric_family)
            .collect();
        Ok(PullMetricsResponse {
            metric_families: metric_families_proto,
        })
    }

    async fn get_node_diagnostics(
        &self,
        _: GetNodeDiagnosticsRequest,
    ) -> DeveloperResult<GetNodeDiagnosticsResponse> {
        let build_info = crate::BuildInfo::get();
        let build_info_json = serde_json::to_string(build_info)
            .map_err(|e| DeveloperError::Internal(e.to_string()))?;

        let runtime_info = crate::RuntimeInfo::get();
        let runtime_info_json = serde_json::to_string(runtime_info)
            .map_err(|e| DeveloperError::Internal(e.to_string()))?;

        let mut node_config = (*self.node_config).clone();
        node_config.redact();
        let node_config_json = serde_json::to_string(&node_config)
            .map_err(|e| DeveloperError::Internal(e.to_string()))?;

        Ok(GetNodeDiagnosticsResponse {
            build_info_json,
            runtime_info_json,
            node_config_json,
        })
    }
}

fn convert_metric(
    metric_type: prometheus::proto::MetricType,
    mut metric: prometheus::proto::Metric,
) -> Option<Metric> {
    let metric_value = match metric_type {
        MetricType::COUNTER => {
            let counter = metric.counter.take().unwrap_or_default();
            let counter_value = safe_f64_to_u64(counter.value())?;
            MetricValue::Counter(counter_value)
        }
        MetricType::GAUGE => {
            let gauge = metric.gauge.take().unwrap_or_default();
            let gauge_value = gauge.value();
            MetricValue::Gauge(gauge_value)
        }
        MetricType::HISTOGRAM => {
            let histogram = metric.histogram.take().unwrap_or_default();
            let sample_count = histogram.sample_count();
            let sample_sum = histogram.sample_sum();
            let buckets: Vec<HistogramBucket> = histogram
                .bucket
                .into_iter()
                .map(|bucket| HistogramBucket {
                    cumulative_count: bucket.cumulative_count(),
                    upper_bound: bucket.upper_bound(),
                })
                .collect();
            MetricValue::Histogram(Histogram {
                sample_count,
                sample_sum,
                buckets,
            })
        }
        MetricType::SUMMARY | MetricType::UNTYPED => {
            return None;
        }
    };
    let labels: Vec<Label> = metric
        .take_label()
        .into_iter()
        .map(|label| Label {
            name: label.name().to_string(),
            value: label.value().to_string(),
        })
        .collect();
    Some(Metric {
        labels,
        metric_value: Some(metric_value),
    })
}

fn convert_metric_family(
    mut metric_family: prometheus::proto::MetricFamily,
) -> Option<MetricFamily> {
    let name: String = metric_family.take_name();
    let metric_type = metric_family.get_field_type();
    let metrics: Vec<Metric> = metric_family
        .take_metric()
        .into_iter()
        .flat_map(|metric| convert_metric(metric_type, metric))
        .collect();
    if metrics.is_empty() {
        return None;
    }
    Some(MetricFamily { name, metrics })
}

fn safe_f64_to_u64(val: f64) -> Option<u64> {
    // This treats NaN as well.
    if !val.is_finite() || val.is_sign_negative() {
        return None;
    }
    Some(val as u64)
}

#[cfg(test)]
mod tests {
    use quickwit_cluster::{ChannelTransport, create_cluster_for_test};
    use serde_json::Value as JsonValue;

    use super::*;

    #[test]
    fn test_safe_f64_to_u64() {
        assert_eq!(safe_f64_to_u64(1.0), Some(1));
        assert_eq!(safe_f64_to_u64(0.0), Some(0));
        assert_eq!(safe_f64_to_u64(-1.0), None);
        assert_eq!(safe_f64_to_u64(f64::NAN), None);
        assert_eq!(safe_f64_to_u64(f64::INFINITY), None);
        assert_eq!(safe_f64_to_u64(1.1), Some(1));
        assert_eq!(safe_f64_to_u64(1.9), Some(1));
        assert_eq!(safe_f64_to_u64(2.0), Some(2));
    }

    #[tokio::test]
    async fn test_developer_api_server_get_debug_info() {
        let peer_seeds = Vec::new();
        let transport = ChannelTransport::default();
        let self_node_readiness = true;
        let cluster = create_cluster_for_test(
            peer_seeds,
            &["metastore", "control-plane", "indexer"],
            &transport,
            self_node_readiness,
        )
        .await
        .unwrap();

        let mut node_config = NodeConfig::for_test();
        node_config.metastore_uri =
            quickwit_common::uri::Uri::for_test("postgresql://username:password@db");
        let node_config = Arc::new(node_config);

        let developer_api_server = DeveloperApiServer {
            node_config,
            cluster,
            control_plane_mailbox_opt: None,
            ingest_router_opt: None,
            ingester_opt: None,
        };
        let request = GetDebugInfoRequest { roles: Vec::new() };
        let response = developer_api_server.get_debug_info(request).await.unwrap();
        let debug_info: JsonValue = serde_json::from_slice(&response.debug_info_json).unwrap();

        assert!(debug_info["build_info"].is_object());
        assert!(debug_info["env_info"].is_object());
        assert!(debug_info["runtime_info"].is_object());
        assert!(debug_info["node_config"].is_object());
        assert!(debug_info["cluster_membership_info"].is_object());

        assert_eq!(
            debug_info["node_config"]["metastore_uri"],
            "postgresql://username:***redacted***@db"
        );

        // TODO: Test control plane and ingester debug info.
    }
}
