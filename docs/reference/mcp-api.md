---
title: MCP API
sidebar_position: 21
---

Quickwit exposes a read-only Model Context Protocol (MCP) endpoint on its REST port:

```text
http://localhost:7280/api/v1/mcp
```

Configure an MCP client with this URL and the **Streamable HTTP** transport. The
endpoint is served by `quickwit-serve` wherever the REST API is available; no
separate process or node role is required.

The tools call the same handlers as Quickwit's
[Elasticsearch-compatible API](es_compatible_api.md). They use its query DSL,
response formats, defaults, and supported-feature restrictions, not the native
Quickwit search API. This does not add support for arbitrary Elasticsearch APIs.

## Tools

| Tool | Elasticsearch operation | Required arguments |
| --- | --- | --- |
| `list_indices` | `/{index}/_cat/indices` | None; `index` defaults to `*` |
| `get_field_capabilities` | `/{index}/_field_caps` | `index` |
| `search` | `/{index}/_search` | `index` |
| `count` | `/{index}/_count` | `index` |

The Elasticsearch paths in this table are relative to `/api/v1/_elastic`.

Arguments:

- **`index`**: an index name or comma-separated index patterns, for example
  `logs-*,traces`. Pass the decoded name, not a URL-encoded path.
- **`params`**: optional Elasticsearch URL query parameters. Every value must be
  a **string**, including numbers and booleans. Lists are comma-separated strings:
  `{"size":"10","_source_includes":"message,timestamp"}`. The accepted parameters
  depend on the operation. `list_indices` defaults `format` to `json`.
- **`body`**: optional Elasticsearch JSON request body for `search`, `count`, and
  `get_field_capabilities`. Defaults to `{}`. Field capabilities accepts an
  `index_filter` in its body; field patterns belong in `params.fields`.

Use `list_indices` and `get_field_capabilities` before constructing queries. For
searches, prefer narrow index patterns, explicit time-range filters, and small
result sizes. Use `body.size: 0` for aggregation-only searches. Search pagination
uses `from`/`size` or `search_after`; scroll is not exposed by these tools.

Successful calls return the Elasticsearch JSON response serialized into an MCP
text content block. Invalid tool arguments and Elasticsearch/backend errors
return a tool result with `isError: true` and the Elasticsearch error JSON. They
do not become successful empty search results. Unknown methods and unknown tool
names return JSON-RPC errors instead.

No ingestion, deletion, index administration, or arbitrary HTTP-request tool is
exposed.

## Protocol and transport

- Supported protocol version: **`2025-06-18`**. Initialization returns this version
  if the client's proposed version is not supported. The client must support the
  returned version to continue.
- Send one JSON-RPC message per HTTP POST. Batches are not supported.
- Set `Content-Type: application/json` and
  `Accept: application/json, text/event-stream`.
- After initialization, set `MCP-Protocol-Version: 2025-06-18` on every POST.
  Other versions, and a missing version after initialization, return HTTP 400.
- Responses use `application/json`; there are no SSE streams. Valid notifications
  and client responses receive HTTP 202 with an empty body. Sending a tool call
  without a request ID does **not** execute it.
- There are no sessions or `Mcp-Session-Id` headers. GET and DELETE return HTTP 405.
  The endpoint does not implement server-initiated requests, resources, prompts,
  progress notifications, or cancellation of running searches.
- Request bodies are limited to **1 MiB** and require `Content-Length`.
  Chunked request bodies without a length are not accepted. Search limits and
  partial-result behavior are inherited from the Elasticsearch-compatible API.
  There is no additional MCP response-size limit or truncation; callers should
  limit hits, source fields, and aggregation buckets.

## Example exchange

Initialize:

```bash
curl http://localhost:7280/api/v1/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
      "protocolVersion": "2025-06-18",
      "capabilities": {},
      "clientInfo": {"name": "example-client", "version": "1.0"}
    }
  }'
```

Send the initialized notification (HTTP 202, no response body):

```bash
curl http://localhost:7280/api/v1/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'MCP-Protocol-Version: 2025-06-18' \
  -d '{"jsonrpc":"2.0","method":"notifications/initialized"}'
```

Discover the tool schemas using `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`
with the same headers. To search an existing `logs-*` index:

```bash
curl http://localhost:7280/api/v1/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'MCP-Protocol-Version: 2025-06-18' \
  -d '{
    "jsonrpc": "2.0",
    "id": 3,
    "method": "tools/call",
    "params": {
      "name": "search",
      "arguments": {
        "index": "logs-*",
        "body": {"size": 5, "query": {"match_all": {}}}
      }
    }
  }'
```

## Security and browser clients

The endpoint inherits the REST listener's network exposure and TLS configuration.
It adds **no authentication, OAuth flow, or per-index authorization**. Anyone who
can reach it can use its tools to read the same data as the Elasticsearch API.
Keep it on a trusted network or behind an authenticated, access-controlled proxy;
do not expose it directly to the public Internet. Existing proxy path allowlists
must explicitly account for `/api/v1/mcp`.

When a request includes an `Origin` header, it must exactly match an explicit
entry in `rest.cors_allow_origins`. Missing `Origin` is permitted for non-browser
clients. An empty allowlist rejects all requests that include `Origin`. Neither
`*` nor `QW_ENABLE_CORS_DEBUG` bypasses this MCP origin check. For example:

```yaml
rest:
  cors_allow_origins:
    - https://your-mcp-client.example.com
```

CORS permits the `Content-Type` and `MCP-Protocol-Version` headers. Origin checks
help protect browser access against DNS rebinding; they are **not authentication**
and do not restrict non-browser clients.

Search results may contain untrusted document text. MCP clients should treat that
text as data, not as instructions or permission to invoke other tools.
