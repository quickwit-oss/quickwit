#!/usr/bin/env python3
"""Generate 100 synthetic Datadog logs in JSON format.

Output is compatible with Vector's datadog_agent source at POST /api/v2/logs.
"""

import json
import os
import random
import time

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
    "i-083ae855fd27e8c9c",
    "i-01d8cb5156d771f3d",
    "i-0059ba9138ff16eec",
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

SOURCES = [
    "python",
    "java",
    "go",
    "nodejs",
    "nginx",
    "postgres",
    "redis",
    "kubernetes",
    "docker",
    "systemd",
    "syslog",
]

STATUSES = ["debug", "info", "notice", "warn", "error", "critical"]
STATUS_WEIGHTS = [5, 50, 10, 20, 12, 3]

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

# Realistic log messages keyed by status level
LOG_MESSAGES = {
    "debug": [
        "Entering request handler for /api/v1/users with method GET",
        "Cache lookup for key user:session:a83f2c completed in 0.3ms",
        "Database connection pool stats: active=12, idle=8, max=50",
        "Serializing response payload, size=4.2KB",
        "JWT token validated successfully for user_id=9281",
        "Loading configuration from environment variables",
        "Resolved DNS for postgres-primary.internal in 1.2ms",
        "Starting background job worker with concurrency=4",
        "Checking feature flag new-checkout for org_id=4412",
        "gRPC channel established to recommendation-engine:50051",
        "Attempting to acquire distributed lock order-processing-7291",
        "Request context deadline set to 30s",
        "Reading 256 bytes from socket connection fd=42",
        "Evaluating rate limit for client_id=api-key-8832, bucket=default",
        "Dequeued message from orders queue, offset=884231",
    ],
    "info": [
        "Server started and listening on port 8080",
        "Successfully processed payment of $142.50 for order_id=ORD-88412",
        "User user_id=7723 logged in from 203.0.113.42",
        "Deployed version v2.4.1 to production cluster gizmo",
        "Database migration 20240315_add_indexes completed in 3.2s",
        "Health check passed: all 5 upstream dependencies healthy",
        "Processed 1,247 events from Kafka topic user-events in 820ms",
        "Cache hit rate is 94.3% over the last 5 minutes",
        "Completed nightly data export, 2.3GB written to S3",
        "New WebSocket connection established from client 10.0.2.15",
        "Successfully rotated TLS certificate for api.example.com",
        "Auto-scaled web-asg from 4 to 6 instances based on CPU utilization",
        "Flushed 10,000 metrics to Datadog in 450ms",
        "Inventory sync completed: 842 products updated, 12 new, 3 removed",
        "Rate limiter configuration reloaded from config service",
        "Session manager pruned 1,203 expired sessions",
        "Search index rebuild completed for products index, 50,412 documents",
        "Outbound notification sent to webhook https://hooks.example.com/events",
        "Connection pool warmed up: 20 connections to postgres-primary ready",
        "Graceful shutdown initiated, draining 3 in-flight requests",
        "Request GET /api/v1/orders/88412 completed in 23ms with status 200",
        "Background job process-receipts completed successfully in 1.4s",
        "Loaded 842 feature flags from LaunchDarkly",
        "Circuit breaker for recommendation-engine reset to closed state",
        "Read replica postgres-read-2 promoted to primary",
    ],
    "notice": [
        "Slow query detected: SELECT * FROM orders WHERE status='pending' took 2.8s",
        "Request queue depth reached 150, approaching throttle threshold of 200",
        "TLS certificate for api.example.com expires in 14 days",
        "Memory usage at 78% of container limit (3.1GB / 4.0GB)",
        "Client 10.0.3.22 exceeded soft rate limit of 100 req/s, currently at 112 req/s",
        "Deprecated API endpoint /api/v1/legacy/users called by client_id=sdk-old-3391",
        "Database connection pool utilization at 85%, consider increasing max_connections",
        "Response time p99 for /api/v1/search increased to 1.8s from baseline 0.9s",
        "Disk utilization on /data volume at 72%, projected full in 18 days",
        "Retry attempt 2 of 3 for upstream call to billing-service",
    ],
    "warn": [
        "Request to payment gateway timed out after 5000ms, retrying",
        "Disk space on /var/log is 89% full, cleanup recommended",
        "Connection to Redis cache-cluster redis-prod-01 lost, reconnecting",
        "Response time for /api/v1/search exceeded SLO threshold of 500ms (actual: 823ms)",
        "Failed to send notification email to user_id=4412, SMTP server returned 421",
        "Memory usage exceeded 80% threshold on worker-prod-02",
        "Stale read detected from postgres read replica, lag is 4.2s",
        "API rate limit approaching for downstream service billing-service (92% of quota)",
        "Certificate for internal-ca.pem expires in 7 days",
        "Kafka consumer lag on topic order-events partition 3 is 12,400 messages",
        "Circuit breaker for recommendation-engine tripped, 5 consecutive failures",
        "Request payload size 4.8MB exceeds recommended limit of 1MB",
        "DNS resolution for analytics.internal took 2.1s, possible DNS issue",
        "Thread pool exhaustion warning: 48 of 50 threads active",
        "Skipping optional enrichment step due to upstream timeout",
    ],
    "error": [
        "Failed to process payment for order_id=ORD-77291: card declined by issuer",
        "Database query failed: connection refused to postgres-primary:5432",
        "Unhandled exception in request handler: NullPointerException at UserService.java:142",
        "Failed to write to Kafka topic order-events: broker not available",
        "S3 upload failed for file exports/daily-2024-03-15.csv: AccessDenied",
        "Authentication failed for user_id=8831: invalid refresh token",
        "gRPC call to inventory-service failed with UNAVAILABLE: connection reset",
        "Failed to deserialize message from queue notifications: invalid JSON at position 847",
        "Health check failed for upstream dependency billing-service: HTTP 503",
        "Request GET /api/v1/users/44120 failed with status 500 after 3 retries",
        "Out of memory: container killed by OOM killer, peak usage 4.1GB",
        "SSL handshake failed with client 203.0.113.88: certificate verify failed",
        "Deadlock detected in transaction processing, rolling back tx_id=TX-9912",
        "Failed to acquire distributed lock after 10s timeout: lock held by worker-prod-03",
        "Index corruption detected in search index products-v2, triggering rebuild",
    ],
    "critical": [
        "All database connections exhausted, max_connections=50 reached, new requests will fail",
        "Primary database postgres-primary is unreachable, failover initiated to postgres-standby",
        "Data loss detected: 247 events dropped from Kafka topic order-events due to broker failure",
        "Service payment-api is returning 100% error rate, all circuits open",
        "Cluster gizmo control plane unreachable, Kubernetes API server not responding",
        "Split brain detected in Redis cluster redis-prod, manual intervention required",
        "Root filesystem on web-prod-01 is 100% full, system is read-only",
        "Cascading failure detected: 4 of 6 upstream services returning errors",
    ],
}


def generate_ddtags(service, env, host, region):
    """Generate comma-separated ddtags with required tags plus 1-100 extras."""
    az = random.choice(AZS[region])
    base_tags = [
        f"service:{service}",
        f"env:{env}",
        f"host:{host}",
        f"region:{region}",
        f"availability_zone:{az}",
        f"instance_type:{random.choice(INSTANCE_TYPES)}",
    ]

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

    num_extra = random.randint(1, max(1, 100 - len(base_tags)))
    extras = random.sample(EXTRA_TAG_POOL, min(num_extra, len(EXTRA_TAG_POOL)))
    return ",".join(base_tags + extras)


def generate_logs(n=100):
    base_ts_ms = int(time.time() * 1000)
    logs = []

    for i in range(n):
        service = random.choice(SERVICES)
        env = random.choice(ENVS)
        host = random.choice(HOSTS)
        region = random.choice(REGIONS)
        status = random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
        source = random.choice(SOURCES)
        message = random.choice(LOG_MESSAGES[status])

        # Timestamps within a few seconds, in milliseconds
        ts_ms = base_ts_ms + random.randint(0, 10000)

        logs.append(
            {
                "message": message,
                "timestamp": ts_ms,
                "hostname": host,
                "status": status,
                "service": service,
                "ddsource": source,
                "ddtags": generate_ddtags(service, env, host, region),
            }
        )

    # Sort by timestamp for realism
    logs.sort(key=lambda l: l["timestamp"])
    return logs


def main():
    data = generate_logs(100)

    out_dir = os.path.join(os.path.dirname(__file__), "sandbox")
    os.makedirs(out_dir, exist_ok=True)

    out_path = os.path.join(out_dir, "logs.json")
    with open(out_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Wrote {len(data)} logs to {out_path}")

    from collections import Counter

    status_counts = Counter(l["status"] for l in data)
    tag_counts = [len(l["ddtags"].split(",")) for l in data]
    print(f"  Status distribution: {dict(sorted(status_counts.items()))}")
    print(f"  Tags per log: min={min(tag_counts)}, max={max(tag_counts)}, avg={sum(tag_counts) / len(tag_counts):.0f}")


if __name__ == "__main__":
    main()
