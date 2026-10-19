# Quickwit telemetry relay

Quickwit nodes send anonymous usage data to `https://telemetry.quickwit.io/` (see [docs/telemetry.md](../../docs/telemetry.md)). This relay receives it and forwards it to Datadog Logs. It runs as a Cloudflare Worker and has no dependencies.

For each request, the relay:

1. Rejects it unless it is a `POST /` whose body matches the payload format of `quickwit/quickwit-telemetry/src/payload.rs`, and copies only the documented fields.
2. Ignores the client IP address and request headers. Nothing from them is stored or forwarded.
3. Replaces `client_info.hashed_host_username`, an unsalted MD5 of `hostname:username`, with `client.install_id`, an HMAC-SHA256 keyed with `INSTALL_ID_SECRET`. Without the secret, the stored value can't be matched against guessed host or user names.
4. Sends one log per event to the Datadog Logs intake API.

Every release that still sends telemetry (0.6.0 to 0.8.x, and the releases that restored it) uses this payload format, so the relay accepts all of them.

## Log format

```json
{
  "ddsource": "quickwit-telemetry",
  "service": "quickwit-telemetry",
  "message": "end_command",
  "event": { "type": "end_command", "client_unixtime": 1760000060, "return_code": 0 },
  "client": {
    "session_uuid": "0d6e4079-e2c8-4bd5-a1a1-a6bce5ad1c29",
    "install_id": "<64 hex characters>",
    "os": "linux",
    "arch": "x86_64",
    "kubernetes": true
  },
  "quickwit": {
    "version": "0.9.1",
    "services": ["indexer", "searcher"],
    "features": ["otlp", "s3_storage"]
  },
  "num_dropped_events": 0
}
```

The log timestamp is the time Datadog receives the log. The client clock may be wrong, so its time is kept in `event.client_unixtime`.

## Settings

| Name | Kind | Description |
| --- | --- | --- |
| `DD_LOGS_URL` | variable | Datadog Logs intake URL, for example `https://http-intake.logs.datadoghq.com/api/v2/logs`. |
| `DD_API_KEY` | secret | Datadog API key used to send the logs. |
| `INSTALL_ID_SECRET` | secret | HMAC key for `install_id`. Changing it changes every install ID. |

The relay returns HTTP 500 and forwards nothing if a setting is missing.

## Test

```bash
npm test
```

## Run locally

Start the relay, then point a Quickwit node at it with `TELEMETRY_INGEST_API`:

```bash
DD_LOGS_URL=https://http-intake.logs.datadoghq.com/api/v2/logs \
DD_API_KEY=<api key> \
INSTALL_ID_SECRET=<secret> \
npm run dev
```

```bash
TELEMETRY_INGEST_API=http://127.0.0.1:8787/ quickwit run
```

## Deploy

```bash
npx wrangler secret put DD_API_KEY
npx wrangler secret put INSTALL_ID_SECRET
npx wrangler deploy
```

Uncomment the `routes` entry in `wrangler.toml` to serve the relay on `telemetry.quickwit.io`.

Not handled by the relay:

- Rate limiting. Use a Cloudflare rate limiting rule on the route.
- Platform logs. `wrangler.toml` turns Workers Logs off. Don't turn on Logpush or other request logs that record client IP addresses.
