# Quickwit telemetry relay

Quickwit nodes send anonymous usage data to `https://telemetry.quickwit.io/` (see [docs/telemetry.md](../../docs/telemetry.md)). This relay receives it and forwards it to Datadog Logs. It runs on AWS Lambda, behind CloudFront and an API Gateway HTTP API, and has no dependencies.

For each request, the relay:

1. Rejects it unless its body matches the payload format of `quickwit/quickwit-telemetry/src/payload.rs`, and keeps only the documented fields. Releases 0.6.0 and later all send this format.
2. Ignores the client IP address and request headers, except the country and city that CloudFront derives from the IP address.
3. Replaces `hashed_host_username` and `hashed_cluster_id`, unsalted MD5 hashes that can be reversed by guessing, with HMAC-SHA256 hashes keyed with `INSTALL_ID_SECRET`: `client.install_id` and `quickwit.cluster_id`.
4. Sends one log per event to the Datadog Logs intake API.

## Log format

```json
{
  "ddsource": "quickwit-telemetry",
  "service": "quickwit-telemetry",
  "message": "end_command",
  "schema_version": 2,
  "event": {
    "type": "end_command",
    "client_unixtime": 1760000060,
    "return_code": 0,
    "api_usage": { "elasticsearch_ingest": "100-999", "native_search": "1-9" },
    "cluster_summary": { "num_indexes": "2-10", "source_types": ["kafka"] }
  },
  "client": {
    "session_uuid": "0d6e4079-e2c8-4bd5-a1a1-a6bce5ad1c29",
    "install_id": "<64 hex characters>",
    "install_uuid": "8c4f2a5e-3b1d-4e7f-9a6c-2d5e8f1b3c7a",
    "os": "linux",
    "arch": "x86_64",
    "kubernetes": true,
    "cpus": "5-8",
    "memory": "8-16GiB"
  },
  "quickwit": {
    "version": "0.9.1",
    "services": ["indexer", "searcher"],
    "features": ["ingest_v2", "otlp", "s3_storage"],
    "build": { "commit": "4f20e4f", "target": "x86_64-unknown-linux-gnu", "release": true },
    "cluster_id": "<64 hex characters>"
  },
  "location": { "country": "US", "city": "Ashburn" },
  "num_dropped_events": 0
}
```

Fields that older releases don't send are left out. `event.cluster_summary` only comes from the node running the control plane.

## Settings

| Name | Description |
| --- | --- |
| `DD_LOGS_URL` | Datadog Logs intake URL, for example `https://http-intake.logs.datadoghq.com/api/v2/logs`. |
| `DD_API_KEY_PARAMETER` | SSM parameter holding the Datadog API key. |
| `INSTALL_ID_SECRET_PARAMETER` | SSM parameter holding the HMAC key. Changing it changes every install and cluster ID. |
| `ORIGIN_VERIFY_SECRET` | Value of the `X-Origin-Verify` header that CloudFront adds. Requests without it get HTTP 403. |

The Lambda entry point is `src/lambda.handler`, on Node.js 22.

## Test

```bash
npm test
```
