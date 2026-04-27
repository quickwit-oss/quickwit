#!/usr/bin/env python3
"""Generate synthetic Datadog trace chunks in protobuf format (V2 TracePayload).

Output is compatible with Vector's datadog_agent source at POST /api/v0.2/traces.
Each chunk represents a partial trace from a single service (5-15 spans sharing a
trace ID), simulating what a DD Agent would flush after its ~10s collection window.
Multiple chunks may share a trace ID, representing different services contributing
to the same distributed trace.
"""

import json
import os
import random
import time

import dd_trace_pb2

# --- Span operation templates ---
# Each entry: (span_name, resource, span_type, is_entry_point)
# The first item in each group can be a root span.
SPAN_TEMPLATES = {
    "web-frontend": [
        ("http.request", "GET /", "web", True),
        ("http.request", "GET /dashboard", "web", True),
        ("http.request", "POST /api/v1/orders", "web", True),
        ("http.request", "GET /api/v1/users/{id}", "web", True),
        ("template.render", "views/dashboard.html", "template", False),
        ("template.render", "views/order_confirm.html", "template", False),
        ("middleware.auth", "AuthenticationMiddleware", "web", False),
        ("middleware.cors", "CORSMiddleware", "web", False),
        ("session.load", "RedisSessionStore.get", "cache", False),
        ("static.serve", "GET /assets/main.css", "web", False),
    ],
    "api-gateway": [
        ("http.request", "POST /api/v1/checkout", "web", True),
        ("http.request", "GET /api/v1/products", "web", True),
        ("http.request", "GET /api/v2/search", "web", True),
        ("http.request", "PUT /api/v1/cart/{id}", "web", True),
        ("grpc.client", "OrderService.CreateOrder", "grpc", False),
        ("grpc.client", "UserService.GetUser", "grpc", False),
        ("grpc.client", "InventoryService.CheckStock", "grpc", False),
        ("rate_limit.check", "RateLimiter.allow", "custom", False),
        ("auth.validate_token", "JWTValidator.verify", "custom", False),
        ("request.transform", "RequestTransformer.apply", "custom", False),
    ],
    "payment-api": [
        ("grpc.server", "PaymentService.ProcessPayment", "grpc", True),
        ("grpc.server", "PaymentService.RefundPayment", "grpc", True),
        ("http.client", "POST https://api.stripe.com/v1/charges", "http", False),
        ("http.client", "POST https://api.stripe.com/v1/refunds", "http", False),
        ("db.query", "SELECT FROM payments WHERE id = ?", "sql", False),
        ("db.query", "INSERT INTO payments (amount, currency, status) VALUES (?, ?, ?)", "sql", False),
        ("db.query", "UPDATE payments SET status = ? WHERE id = ?", "sql", False),
        ("payment.validate", "PaymentValidator.validate", "custom", False),
        ("payment.fraud_check", "FraudDetector.score", "custom", False),
        ("kafka.produce", "payment-events", "queue", False),
    ],
    "order-service": [
        ("grpc.server", "OrderService.CreateOrder", "grpc", True),
        ("grpc.server", "OrderService.GetOrder", "grpc", True),
        ("grpc.server", "OrderService.ListOrders", "grpc", True),
        ("db.query", "INSERT INTO orders (user_id, total, status) VALUES (?, ?, ?)", "sql", False),
        ("db.query", "SELECT FROM orders WHERE user_id = ? ORDER BY created_at DESC", "sql", False),
        ("db.query", "UPDATE orders SET status = ? WHERE id = ?", "sql", False),
        ("grpc.client", "InventoryService.ReserveStock", "grpc", False),
        ("grpc.client", "PaymentService.ProcessPayment", "grpc", False),
        ("grpc.client", "NotificationService.SendEmail", "grpc", False),
        ("kafka.produce", "order-events", "queue", False),
        ("cache.get", "Redis.GET order:{id}", "cache", False),
        ("cache.set", "Redis.SET order:{id}", "cache", False),
    ],
    "user-service": [
        ("grpc.server", "UserService.GetUser", "grpc", True),
        ("grpc.server", "UserService.UpdateUser", "grpc", True),
        ("grpc.server", "UserService.ListUsers", "grpc", True),
        ("db.query", "SELECT FROM users WHERE id = ?", "sql", False),
        ("db.query", "UPDATE users SET email = ?, name = ? WHERE id = ?", "sql", False),
        ("db.query", "SELECT FROM users WHERE org_id = ? LIMIT ?", "sql", False),
        ("cache.get", "Redis.GET user:{id}", "cache", False),
        ("cache.set", "Redis.SET user:{id}", "cache", False),
        ("auth.check_permissions", "PermissionChecker.hasRole", "custom", False),
    ],
    "inventory-service": [
        ("grpc.server", "InventoryService.CheckStock", "grpc", True),
        ("grpc.server", "InventoryService.ReserveStock", "grpc", True),
        ("db.query", "SELECT quantity FROM inventory WHERE sku = ?", "sql", False),
        ("db.query", "UPDATE inventory SET quantity = quantity - ? WHERE sku = ?", "sql", False),
        ("db.query", "SELECT FROM inventory WHERE warehouse_id = ?", "sql", False),
        ("cache.get", "Redis.GET stock:{sku}", "cache", False),
        ("cache.set", "Redis.SET stock:{sku}", "cache", False),
        ("kafka.produce", "inventory-events", "queue", False),
    ],
    "notification-service": [
        ("grpc.server", "NotificationService.SendEmail", "grpc", True),
        ("grpc.server", "NotificationService.SendPush", "grpc", True),
        ("http.client", "POST https://api.sendgrid.com/v3/mail/send", "http", False),
        ("http.client", "POST https://fcm.googleapis.com/fcm/send", "http", False),
        ("db.query", "INSERT INTO notifications (user_id, type, status) VALUES (?, ?, ?)", "sql", False),
        ("db.query", "SELECT FROM notification_preferences WHERE user_id = ?", "sql", False),
        ("template.render", "emails/order_confirmation.html", "template", False),
        ("template.render", "emails/password_reset.html", "template", False),
    ],
    "search-service": [
        ("http.request", "GET /api/v2/search", "web", True),
        ("elasticsearch.query", "POST /products/_search", "elasticsearch", False),
        ("elasticsearch.query", "POST /products/_msearch", "elasticsearch", False),
        ("cache.get", "Redis.GET search:{hash}", "cache", False),
        ("cache.set", "Redis.SET search:{hash}", "cache", False),
        ("search.parse_query", "QueryParser.parse", "custom", False),
        ("search.rank_results", "Ranker.score", "custom", False),
    ],
}

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
    "i-07a91062bd7ecf277",
    "i-05ada25a89786f05f",
    "i-04cf9aca3b4050230",
]

REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]
AZS = {
    "us-east-1": ["us-east-1a", "us-east-1b", "us-east-1c"],
    "us-west-2": ["us-west-2a", "us-west-2b", "us-west-2c"],
    "eu-west-1": ["eu-west-1a", "eu-west-1b", "eu-west-1c"],
    "ap-southeast-1": ["ap-southeast-1a", "ap-southeast-1b"],
}

LANGUAGES = [
    ("go", "1.22.1", "v1.64.0"),
    ("python", "3.12.2", "v2.8.0"),
    ("java", "21.0.2", "v1.31.0"),
    ("nodejs", "20.11.1", "v5.8.0"),
]

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
    "runtime:go",
    "runtime:python",
    "runtime:java",
    "runtime:node",
    "container_name:app",
    "container_name:sidecar",
    "image_tag:latest",
    "image_tag:v1.2.3",
    "image_tag:sha-abc1234",
    "cloud_provider:aws",
    "db_engine:postgres",
    "db_engine:mysql",
    "db_engine:redis",
    "method:GET",
    "method:POST",
    "method:PUT",
    "method:DELETE",
    "status_code:200",
    "status_code:201",
    "status_code:400",
    "status_code:500",
    "endpoint:/api/v1/users",
    "endpoint:/api/v1/orders",
    "endpoint:/api/v1/health",
    "protocol:http",
    "protocol:https",
    "protocol:grpc",
    "autoscaling_group:web-asg",
    "autoscaling_group:api-asg",
    "vpc:vpc-prod-main",
    "vpc:vpc-prod-data",
    "deployment:blue",
    "deployment:green",
    "shard:shard-01",
    "shard:shard-02",
    "shard:shard-03",
    "queue:orders",
    "queue:notifications",
    "topic:user-events",
    "topic:order-events",
    "cache_cluster:redis-prod-01",
    "cache_cluster:redis-prod-02",
    "orchestrator:kubernetes",
    "kube_cluster_name:gizmo",
    "kube_cluster_name:falcon",
    "kube_namespace:app",
    "kube_namespace:default",
    "priority_class:high",
    "priority_class:normal",
]


def make_span_meta(service, env, region):
    """Build the meta (string tags) dict for a span."""
    az = random.choice(AZS[region])
    meta = {
        "env": env,
        "service": service,
        "region": region,
        "availability_zone": az,
    }
    # Add 1-100 extra tags
    num_extra = random.randint(1, min(96, len(EXTRA_TAG_POOL)))
    for tag in random.sample(EXTRA_TAG_POOL, num_extra):
        k, v = tag.split(":", 1)
        meta[k] = v
    return meta


def generate_span_id():
    return random.randint(1, (1 << 63) - 1)


def generate_trace_chunk(trace_id, base_time_ns, service_name, env, host, region, remote_parent_span_id=None):
    """Generate a partial trace chunk from a single service (5-15 spans).

    Simulates what the DD Agent flushes: spans from one service that share a
    trace ID. The local root span's parentID points to a span in another
    service (remote_parent_span_id) unless this is the top-level service.
    """
    templates = SPAN_TEMPLATES[service_name]
    num_spans = random.randint(5, 15)

    # Pick the local root span from entry points
    entry_points = [t for t in templates if t[3]]
    root_template = random.choice(entry_points)
    non_root_templates = [t for t in templates if not t[3]]

    # Local root span: covers this service's portion of the trace
    root_duration_ns = random.randint(10_000_000, 500_000_000)  # 10ms - 500ms
    root_start_ns = base_time_ns
    root_span_id = generate_span_id()

    # parentID is non-zero if this chunk is from a downstream service
    root_parent_id = remote_parent_span_id if remote_parent_span_id else 0

    spans_data = []
    spans_data.append(
        {
            "span_id": root_span_id,
            "parent_id": root_parent_id,
            "name": root_template[0],
            "resource": root_template[1],
            "type": root_template[2],
            "start_ns": root_start_ns,
            "duration_ns": root_duration_ns,
            "error": 0,
        }
    )

    # Build remaining spans as children of existing spans within this chunk
    for i in range(1, num_spans):
        parent = random.choice(spans_data)
        parent_start = parent["start_ns"]
        parent_end = parent["start_ns"] + parent["duration_ns"]

        available_window = parent["duration_ns"]
        if available_window < 1000:
            parent = spans_data[0]
            parent_start = parent["start_ns"]
            parent_end = parent["start_ns"] + parent["duration_ns"]
            available_window = parent["duration_ns"]

        child_offset = random.randint(1, max(1, available_window // 4))
        child_start = parent_start + child_offset

        remaining = parent_end - child_start
        if remaining < 1000:
            remaining = 1000
        child_duration = random.randint(max(1, remaining // 20), max(1, remaining - 1))

        template = random.choice(non_root_templates)

        # ~5% of spans have errors
        error_flag = 1 if random.random() < 0.05 else 0

        spans_data.append(
            {
                "span_id": generate_span_id(),
                "parent_id": parent["span_id"],
                "name": template[0],
                "resource": template[1],
                "type": template[2],
                "start_ns": child_start,
                "duration_ns": child_duration,
                "error": error_flag,
            }
        )

    # Convert to protobuf Span objects
    proto_spans = []
    for sd in spans_data:
        span = dd_trace_pb2.Span()
        span.service = service_name
        span.name = sd["name"]
        span.resource = sd["resource"]
        span.traceID = trace_id
        span.spanID = sd["span_id"]
        span.parentID = sd["parent_id"]
        span.start = sd["start_ns"]
        span.duration = sd["duration_ns"]
        span.error = sd["error"]
        span.type = sd["type"]

        meta = make_span_meta(service_name, env, region)
        meta["_dd.hostname"] = host
        if sd["error"]:
            meta["error.message"] = random.choice(
                [
                    "connection refused",
                    "timeout after 5000ms",
                    "404 not found",
                    "internal server error",
                    "permission denied",
                ]
            )
            meta["error.type"] = random.choice(
                [
                    "ConnectionError",
                    "TimeoutError",
                    "NotFoundError",
                    "InternalError",
                    "PermissionError",
                ]
            )
        for k, v in meta.items():
            span.meta[k] = v

        # Add standard metrics
        span.metrics["_sampling_priority_v1"] = 1.0
        span.metrics["_dd.measured"] = 1.0
        if sd["parent_id"] == root_parent_id:
            span.metrics["_dd.top_level"] = 1.0
            # Container tags are set on top-level spans by the DD Agent
            container_tags = (
                f"kube_namespace:ns-{service_name},"
                f"kube_deployment:{service_name},"
                f"pod_name:{service_name}-{random.randint(1000, 9999)},"
                f"container_name:{service_name},"
                f"image_tag:v{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 9)}"
            )
            span.meta["_dd.tags.container"] = container_tags

        proto_spans.append(span)

    return proto_spans, root_span_id


def generate_payload():
    """Generate a TracePayload with multiple partial trace chunks.

    Simulates realistic agent behavior: each TracerPayload represents one
    service's DD Agent flushing a chunk of spans. Multiple chunks may share
    a trace_id (distributed trace across services). Some traces have only
    one chunk (single-service trace).
    """
    base_time_ns = int(time.time()) * 1_000_000_000
    services = list(SPAN_TEMPLATES.keys())

    payload = dd_trace_pb2.TracePayload()
    payload.hostName = random.choice(HOSTS)
    payload.env = "production"
    payload.agentVersion = "7.52.0"
    payload.targetTPS = 50.0
    payload.errorTPS = 5.0

    summary = []

    # Generate 5 distributed traces, each with 2-4 service chunks
    for i in range(5):
        trace_id = random.randint(1, (1 << 63) - 1)
        env = random.choice(ENVS)
        region = random.choice(REGIONS)
        trace_base_ns = base_time_ns + random.randint(0, 10_000_000_000)

        # Pick 2-4 services that participate in this distributed trace
        num_services = random.randint(2, 4)
        trace_services = random.sample(services, num_services)

        # First service is the top-level (no remote parent)
        parent_span_id = None
        for svc_idx, service_name in enumerate(trace_services):
            host = random.choice(HOSTS)
            lang_name, lang_ver, tracer_ver = random.choice(LANGUAGES)

            # Each service's chunk starts a bit after the previous one
            chunk_offset_ns = svc_idx * random.randint(5_000_000, 50_000_000)
            chunk_base_ns = trace_base_ns + chunk_offset_ns

            spans, local_root_span_id = generate_trace_chunk(
                trace_id,
                chunk_base_ns,
                service_name,
                env,
                host,
                region,
                remote_parent_span_id=parent_span_id,
            )

            # Pick a random span from this chunk as the parent for the next
            # service's local root (simulates an outgoing RPC/HTTP call)
            parent_span_id = local_root_span_id

            tracer_payload = dd_trace_pb2.TracerPayload()
            tracer_payload.containerID = f"container-{random.randint(1000, 9999)}"
            tracer_payload.languageName = lang_name
            tracer_payload.languageVersion = lang_ver
            tracer_payload.tracerVersion = tracer_ver
            tracer_payload.runtimeID = f"{random.randint(100000, 999999):x}-{random.randint(100000, 999999):x}"
            tracer_payload.env = env
            tracer_payload.hostname = host
            tracer_payload.appVersion = random.choice(["v1.2.3", "v2.0.0", "v3.1.4"])

            chunk = dd_trace_pb2.TraceChunk()
            chunk.priority = 1
            chunk.origin = "rum"
            chunk.droppedTrace = False
            for span in spans:
                chunk.spans.append(span)

            tracer_payload.chunks.append(chunk)
            payload.tracerPayloads.append(tracer_payload)

            summary.append(
                {
                    "trace_id": trace_id,
                    "service": service_name,
                    "env": env,
                    "host": host,
                    "num_spans": len(spans),
                    "language": lang_name,
                    "is_root_chunk": svc_idx == 0,
                }
            )

    return payload, summary


def main():
    import sys

    sys.path.insert(0, os.path.dirname(__file__))

    payload, summary = generate_payload()
    serialized = payload.SerializeToString()

    out_dir = os.path.join(os.path.dirname(__file__), "sandbox")
    os.makedirs(out_dir, exist_ok=True)

    # Write binary protobuf
    proto_path = os.path.join(out_dir, "traces.pb")
    with open(proto_path, "wb") as f:
        f.write(serialized)

    # Write a human-readable JSON summary alongside it
    json_path = os.path.join(out_dir, "traces_summary.json")
    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2)

    trace_ids = {t["trace_id"] for t in summary}
    print(
        f"Wrote {len(summary)} trace chunks across {len(trace_ids)} distributed traces "
        f"({sum(t['num_spans'] for t in summary)} total spans) to {proto_path}"
    )
    print(f"Wrote human-readable summary to {json_path}")
    print(f"Binary size: {len(serialized):,} bytes")
    print()
    for tid in sorted(trace_ids):
        chunks = [t for t in summary if t["trace_id"] == tid]
        print(f"  Trace {tid}:")
        for c in chunks:
            root_marker = " (root)" if c["is_root_chunk"] else ""
            print(f"    {c['service']} on {c['host']}: {c['num_spans']} spans{root_marker}")


if __name__ == "__main__":
    main()
