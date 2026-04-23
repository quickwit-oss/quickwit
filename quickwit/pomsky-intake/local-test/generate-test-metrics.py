#!/usr/bin/env python3
"""Generate 100 synthetic Datadog metrics in Series V2 protobuf format.

Output is compatible with Vector's datadog_agent source at:
  - POST /api/v2/series      (MetricPayload protobuf for count, rate, gauge)
  - POST /api/beta/sketches   (SketchPayload protobuf for distribution)

All metric names are prefixed with 'ddbyoc.test.' to avoid collisions with real
metrics when metadata is submitted to Datadog staging. Each metric has a fixed
type (count, rate, gauge, or distribution) that is consistent across runs.
"""

import math
import os
import random
import time

import dd_metrics_pb2

# Test metric prefix to avoid collisions with real metric metadata in staging.
METRIC_PREFIX = "ddbyoc.test."

# Metric names mapped to their type. The type is fixed per metric name so that
# metadata submissions are consistent across runs. Types:
#   gauge  — point-in-time measurement (cpu %, memory bytes, queue depth, ...)
#   rate   — per-second throughput (bytes/s, iops, requests/s, ...)
#   count  — cumulative occurrences (hits, errors, invocations, ...)
#   distribution — latency / duration histograms (ms, seconds, ...)
METRICS = {
    # System
    "system.cpu.user": "gauge",
    "system.cpu.system": "gauge",
    "system.cpu.idle": "gauge",
    "system.cpu.iowait": "gauge",
    "system.mem.used": "gauge",
    "system.mem.free": "gauge",
    "system.mem.pct_usable": "gauge",
    "system.mem.cached": "gauge",
    "system.disk.in_use": "gauge",
    "system.disk.read_time": "distribution",
    "system.disk.write_time": "distribution",
    "system.io.util": "gauge",
    "system.io.r_s": "rate",
    "system.io.w_s": "rate",
    "system.load.1": "gauge",
    "system.load.5": "gauge",
    "system.load.15": "gauge",
    "system.net.bytes_rcvd": "rate",
    "system.net.bytes_sent": "rate",
    "system.net.packets_in.error": "count",
    "system.swap.used": "gauge",
    "system.swap.free": "gauge",
    # AWS
    "aws.ec2.cpuutilization": "gauge",
    "aws.ec2.network_in": "rate",
    "aws.ec2.network_out": "rate",
    "aws.ec2.disk_read_ops": "rate",
    "aws.ec2.disk_write_ops": "rate",
    "aws.ec2.status_check_failed": "count",
    "aws.rds.cpuutilization": "gauge",
    "aws.rds.disk_queue_depth": "gauge",
    "aws.rds.network_receive_throughput": "rate",
    "aws.rds.network_transmit_throughput": "rate",
    "aws.rds.free_storage_space": "gauge",
    "aws.rds.read_iops": "rate",
    "aws.rds.write_iops": "rate",
    "aws.applicationelb.request_count": "count",
    "aws.applicationelb.target_response_time": "distribution",
    "aws.applicationelb.healthy_host_count": "gauge",
    "aws.applicationelb.unhealthy_host_count": "gauge",
    "aws.lambda.duration": "distribution",
    "aws.lambda.errors": "count",
    "aws.lambda.invocations": "count",
    "aws.lambda.throttles": "count",
    "aws.lambda.concurrent_executions": "gauge",
    "aws.elb.latency": "distribution",
    "aws.elb.request_count": "count",
    "aws.elb.httpcode_elb_5xx": "count",
    # Kubernetes
    "kubernetes.cpu.usage.total": "gauge",
    "kubernetes.cpu.limits": "gauge",
    "kubernetes.cpu.requests": "gauge",
    "kubernetes.memory.usage": "gauge",
    "kubernetes.memory.limits": "gauge",
    "kubernetes.memory.requests": "gauge",
    "kubernetes.pods.running": "gauge",
    "kubernetes.containers.running": "gauge",
    "kubernetes.containers.restarts": "count",
    "kubernetes_state.deployment.replicas_desired": "gauge",
    "kubernetes_state.deployment.replicas_available": "gauge",
    "kubernetes_state.deployment.replicas_unavailable": "gauge",
    # Docker
    "docker.cpu.usage": "gauge",
    "docker.cpu.throttled": "gauge",
    "docker.mem.rss": "gauge",
    "docker.mem.limit": "gauge",
    "docker.net.bytes_rcvd": "rate",
    "docker.net.bytes_sent": "rate",
    # APM / Trace
    "trace.http.server.hits": "count",
    "trace.http.server.errors": "count",
    "trace.http.server.duration": "distribution",
    "trace.http.server.apdex": "gauge",
    "trace.servlet.request.hits": "count",
    "trace.servlet.request.errors": "count",
    "trace.servlet.request.duration": "distribution",
    "trace.grpc.server.hits": "count",
    "trace.grpc.server.errors": "count",
    "trace.grpc.server.duration": "distribution",
    # Application
    "http.requests.total": "count",
    "http.requests.errors": "count",
    "http.request.duration": "distribution",
    "http.response.size": "distribution",
    "http.connection.count": "gauge",
    "app.queue.depth": "gauge",
    "app.queue.latency": "distribution",
    "app.cache.hits": "count",
    "app.cache.misses": "count",
    "app.cache.evictions": "count",
    "db.query.count": "count",
    "db.query.duration": "distribution",
    "db.connection.pool.size": "gauge",
    "db.connection.pool.active": "gauge",
    "db.connection.pool.idle": "gauge",
    # Network
    "network.tcp.retransmits": "count",
    "network.tcp.connections": "gauge",
    "network.dns.lookup_time": "distribution",
    "network.http.response_time": "distribution",
    # Datadog internal
    "datadog.agent.running": "gauge",
    "datadog.agent.check_runs": "count",
    "datadog.trace_agent.receiver.spans_received": "rate",
    "datadog.trace_agent.receiver.traces_received": "rate",
}

METRIC_NAMES = list(METRICS.keys())

# Map string types to protobuf MetricType enum values.
METRIC_TYPE_TO_PROTO = {
    "count": dd_metrics_pb2.COUNT,
    "rate": dd_metrics_pb2.RATE,
    "gauge": dd_metrics_pb2.GAUGE,
}

SERVICES = [
    "payment-api",
    "web-frontend",
    "api-gateway",
    "user-service",
    "auth-service",
    "order-service",
    "inventory-service",
    "notification-service",
    "search-service",
    "recommendation-engine",
    "billing-service",
    "analytics-worker",
    "data-pipeline",
    "cache-proxy",
    "cdn-origin",
    "session-manager",
]

ENVS = ["production", "staging", "canary"]

HOSTS = [
    "i-01856994a381f021d",
    "i-00a86379f9e7e69b3",
    "i-0118270ea6f0fad6a",
    "i-042dbf0f44585ef50",
    "i-0a914021639e1033b",
    "i-0420bca6def36bed6",
    "i-05cd08f53af8854bf",
    "i-01d98f4b3deb1499d",
    "i-032a4769cbb3fb8ec",
    "i-03a46a1a2e29f39fc",
    "ip-10-128-33-22.ec2.internal-porygon",
    "i-0aa185cf38431ff91",
]

REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]
AZS = {
    "us-east-1": ["us-east-1a", "us-east-1b", "us-east-1c"],
    "us-west-2": ["us-west-2a", "us-west-2b", "us-west-2c"],
    "eu-west-1": ["eu-west-1a", "eu-west-1b", "eu-west-1c"],
    "ap-southeast-1": ["ap-southeast-1a", "ap-southeast-1b"],
}

INSTANCE_TYPES = ["m5.xlarge", "m5.2xlarge", "c5.xlarge", "c5.2xlarge", "r5.large", "t3.medium"]

KUBE_CLUSTERS = ["gizmo", "gadget", "falcon", "raptor"]
KUBE_NAMESPACES = ["default", "kube-system", "monitoring", "app", "data", "infra"]

EXTRA_TAG_POOL = [
    "version:1.2.3",
    "version:2.0.0",
    "version:3.1.4",
    "team:platform",
    "team:backend",
    "team:frontend",
    "team:data",
    "team:sre",
    "tier:web",
    "tier:api",
    "tier:worker",
    "tier:database",
    "tier:cache",
    "port:8080",
    "port:8443",
    "port:3000",
    "port:50051",
    "port:443",
    "runtime:go",
    "runtime:python",
    "runtime:java",
    "runtime:node",
    "container_name:app",
    "container_name:sidecar",
    "container_name:init",
    "image_tag:latest",
    "image_tag:v1.2.3",
    "image_tag:sha-abc1234",
    "cloud_provider:aws",
    "cloud_provider:gcp",
    "db_engine:postgres",
    "db_engine:mysql",
    "db_engine:redis",
    "db_engine:dynamodb",
    "dbname:users",
    "dbname:orders",
    "dbname:sessions",
    "dbname:analytics",
    "method:GET",
    "method:POST",
    "method:PUT",
    "method:DELETE",
    "status_code:200",
    "status_code:201",
    "status_code:400",
    "status_code:404",
    "status_code:500",
    "endpoint:/api/v1/users",
    "endpoint:/api/v1/orders",
    "endpoint:/api/v1/health",
    "endpoint:/api/v2/search",
    "endpoint:/api/v1/auth/login",
    "protocol:http",
    "protocol:https",
    "protocol:grpc",
    "disk_device:xvda",
    "disk_device:xvdb",
    "disk_device:nvme0n1",
    "network_interface:eth0",
    "network_interface:eth1",
    "autoscaling_group:web-asg",
    "autoscaling_group:api-asg",
    "autoscaling_group:worker-asg",
    "launch_template:lt-web-v3",
    "launch_template:lt-api-v2",
    "security_group:sg-web",
    "security_group:sg-internal",
    "vpc:vpc-prod-main",
    "vpc:vpc-prod-data",
    "subnet:subnet-private-a",
    "subnet:subnet-private-b",
    "subnet:subnet-public-a",
    "load_balancer:alb-web-prod",
    "load_balancer:alb-api-prod",
    "target_group:tg-web-prod",
    "target_group:tg-api-prod",
    "deployment:blue",
    "deployment:green",
    "canary:true",
    "canary:false",
    "feature_flag:new-checkout",
    "feature_flag:dark-launch",
    "shard:shard-01",
    "shard:shard-02",
    "shard:shard-03",
    "replica:primary",
    "replica:secondary",
    "replica:read-replica",
    "queue:orders",
    "queue:notifications",
    "queue:analytics",
    "topic:user-events",
    "topic:order-events",
    "topic:system-events",
    "consumer_group:cg-analytics",
    "consumer_group:cg-notifications",
    "cache_cluster:redis-prod-01",
    "cache_cluster:redis-prod-02",
    "dns_zone:prod.internal",
    "dns_zone:api.example.com",
    "ssl_cert:wildcard-prod",
    "ssl_cert:api-prod",
    "log_level:info",
    "log_level:warn",
    "log_level:error",
    "orchestrator:kubernetes",
    "orchestrator:ecs",
    "scheduler:default-scheduler",
    "priority_class:high",
    "priority_class:normal",
    "priority_class:low",
    "qos_class:guaranteed",
    "qos_class:burstable",
]


# ---------------------------------------------------------------------------
# DDSketch helpers — simplified version of the logarithmic index mapping used
# by the Datadog agent (relative accuracy 0.01, gamma ~1.02).
# ---------------------------------------------------------------------------
_DDSKETCH_GAMMA = 1.0 + 2.0 * 0.01 / (1.0 - 0.01)  # ≈1.020202...
_DDSKETCH_LOG_GAMMA = math.log(_DDSKETCH_GAMMA)


def _sketch_key(value):
    """Return the DDSketch bin key for a positive value."""
    if value <= 0:
        return 0
    return int(math.ceil(math.log(value) / _DDSKETCH_LOG_GAMMA))


def _values_to_dogsketch(values, ts):
    """Convert a list of sample values into a Dogsketch proto message."""
    if not values:
        return None

    bins = {}
    for v in values:
        k = _sketch_key(abs(v))
        bins[k] = bins.get(k, 0) + 1

    sorted_keys = sorted(bins.keys())
    ds = dd_metrics_pb2.SketchPayload.Sketch.Dogsketch()
    ds.ts = ts
    ds.cnt = len(values)
    ds.min = min(values)
    ds.max = max(values)
    ds.sum = sum(values)
    ds.avg = ds.sum / ds.cnt
    ds.k.extend(sorted_keys)
    ds.n.extend(bins[k] for k in sorted_keys)
    return ds


# ---------------------------------------------------------------------------
# Tag & value generation (unchanged from v1 script)
# ---------------------------------------------------------------------------


def generate_tags(service, env, host, region):
    """Generate a tag list with required tags plus 1-100 random extras."""
    az = random.choice(AZS[region])
    base_tags = [
        f"service:{service}",
        f"env:{env}",
        f"host:{host}",
        f"region:{region}",
        f"availability_zone:{az}",
        f"instance_type:{random.choice(INSTANCE_TYPES)}",
    ]

    # Add kubernetes tags ~60% of the time
    if random.random() < 0.6:
        cluster = random.choice(KUBE_CLUSTERS)
        ns = random.choice(KUBE_NAMESPACES)
        base_tags.extend(
            [
                f"kube_cluster_name:{cluster}",
                f"kube_namespace:{ns}",
                f"pod_name:{service}-{random.randint(1000, 9999):04x}",
            ]
        )

    # Add 1-100 additional tags (minus base tags already added)
    num_extra = random.randint(1, max(1, 100 - len(base_tags)))
    extras = random.sample(EXTRA_TAG_POOL, min(num_extra, len(EXTRA_TAG_POOL)))
    return base_tags + extras


def generate_value(metric_type):
    """Generate a realistic value appropriate for the metric type."""
    if metric_type == "gauge":
        return round(random.uniform(0, 10000), 2)
    elif metric_type == "rate":
        return round(random.uniform(0, 5000), 2)
    elif metric_type == "count":
        return round(random.uniform(0, 1000), 0)
    elif metric_type == "distribution":
        # Latency/duration in ms — skewed toward lower values
        return round(random.expovariate(1 / 150), 2)
    return round(random.uniform(0, 10000), 2)


# ---------------------------------------------------------------------------
# Payload builders
# ---------------------------------------------------------------------------


def generate_metrics(n=100):
    """Generate n metrics split into a MetricPayload and a SketchPayload."""
    base_ts = int(time.time())

    metric_payload = dd_metrics_pb2.MetricPayload()
    sketch_payload = dd_metrics_pb2.SketchPayload()

    series_count = 0
    sketch_count = 0
    type_counts = {}

    for _ in range(n):
        metric_name = random.choice(METRIC_NAMES)
        metric_type = METRICS[metric_name]
        service = random.choice(SERVICES)
        env = random.choice(ENVS)
        host = random.choice(HOSTS)
        region = random.choice(REGIONS)
        ts = base_ts + random.randint(0, 10)
        tags = generate_tags(service, env, host, region)

        type_counts[metric_type] = type_counts.get(metric_type, 0) + 1

        if metric_type == "distribution":
            # Distributions are sent as sketches.  Generate a batch of sample
            # values and convert them into a single Dogsketch entry.
            num_samples = random.randint(10, 200)
            values = [generate_value("distribution") for _ in range(num_samples)]

            sketch = sketch_payload.sketches.add()
            sketch.metric = METRIC_PREFIX + metric_name
            sketch.host = host
            sketch.tags.extend(tags)
            ds = _values_to_dogsketch(values, ts)
            if ds is not None:
                sketch.dogsketches.append(ds)
            sketch_count += 1
        else:
            # count, rate, gauge → MetricPayload
            value = generate_value(metric_type)

            ms = metric_payload.series.add()
            ms.metric = METRIC_PREFIX + metric_name
            ms.type = METRIC_TYPE_TO_PROTO[metric_type]
            ms.tags.extend(tags)

            pt = ms.points.add()
            pt.timestamp = ts
            pt.value = value

            # v2 resources encode the host
            res = ms.resources.add()
            res.type = "host"
            res.name = host

            # count and rate require an interval
            if metric_type in ("count", "rate"):
                ms.interval = 10

            series_count += 1

    return metric_payload, sketch_payload, series_count, sketch_count, type_counts


def main():
    metric_payload, sketch_payload, series_count, sketch_count, type_counts = generate_metrics(100)

    out_dir = os.path.join(os.path.dirname(__file__), "sandbox")
    os.makedirs(out_dir, exist_ok=True)

    # Write MetricPayload (count, rate, gauge)
    metrics_path = os.path.join(out_dir, "metrics.pb")
    with open(metrics_path, "wb") as f:
        f.write(metric_payload.SerializeToString())
    print(f"Wrote {series_count} series metrics to {metrics_path} ({os.path.getsize(metrics_path):,} bytes)")

    # Write SketchPayload (distributions)
    sketches_path = os.path.join(out_dir, "sketches.pb")
    with open(sketches_path, "wb") as f:
        f.write(sketch_payload.SerializeToString())
    print(f"Wrote {sketch_count} sketch metrics to {sketches_path} ({os.path.getsize(sketches_path):,} bytes)")

    # Print summary stats
    unique_metrics = set()
    for s in metric_payload.series:
        unique_metrics.add(s.metric)
    for s in sketch_payload.sketches:
        unique_metrics.add(s.metric)

    tag_counts = [len(s.tags) for s in metric_payload.series] + [len(s.tags) for s in sketch_payload.sketches]
    print(f"  Unique metric names: {len(unique_metrics)}")
    print(f"  By type: {', '.join(f'{t}={c}' for t, c in sorted(type_counts.items()))}")
    print(
        f"  Tags per metric: min={min(tag_counts)}, max={max(tag_counts)}, avg={sum(tag_counts) / len(tag_counts):.0f}"
    )


if __name__ == "__main__":
    main()
