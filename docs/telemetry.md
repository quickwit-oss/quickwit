---
title: Telemetry
sidebar_position: 12
---

Quickwit collects anonymous usage data to learn which versions are running, on which platforms, and which features are in use. Quickwit is maintained by Datadog, Inc., which receives and stores this data.

## Disabling telemetry

Telemetry is on by default. To turn it off, set `QW_DISABLE_TELEMETRY` before starting Quickwit:

```bash
export QW_DISABLE_TELEMETRY=1
```

Any non-empty value other than `false`, `no` or `0` disables telemetry.

To check whether telemetry is on, run `quickwit --help`. While it is on, the output includes a `Telemetry: enabled` line:

```bash
quickwit --help
Sub-second search & analytics engine on cloud storage.
  Find more information at https://quickwit.io/docs

Telemetry: enabled
```

The line disappears when telemetry is off.

Only `quickwit run` sends telemetry. Other CLI commands never do.

## What is sent

A node sends at most one request per minute, with up to 10 events per request.

Events:

- `run_command`: the node started.
- `end_command`: the node stopped, with its exit code.
- `running`: the node is still up. Sent every 12 hours.
- `ui_index_page_load`: someone opened the Quickwit UI.

Each request also includes:

- The Quickwit version.
- The services the node runs, such as `indexer`, `searcher` or `metastore`.
- The features in use:
  - the metastore type, file-backed or PostgreSQL, if the node runs the metastore;
  - where the default index root URI points: S3, Azure Blob Storage, Google Cloud Storage or the local file system;
  - whether the OTLP endpoints and the Jaeger endpoint are enabled;
  - whether the searcher uses AWS Lambda for leaf searches or a local split cache;
  - whether the REST or gRPC server uses TLS.
- The OS and the CPU architecture.
- Whether the node runs on Kubernetes, based on whether `KUBERNETES_SERVICE_HOST` is set.
- A session ID, generated at random each time the node starts.
- An MD5 hash of the host name and the user name, used to count distinct installations. The relay replaces it with a keyed hash before storage. See below.
- The number of events dropped because too many were queued.

Quickwit never sends index names, documents, queries, configuration values or credentials.

## Where the data goes

Quickwit sends the data over HTTPS to `telemetry.quickwit.io`, a small relay operated by Datadog. Its source code is in [`distribution/telemetry-relay`](https://github.com/quickwit-oss/quickwit/tree/main/distribution/telemetry-relay). The relay:

- rejects any request that doesn't match the format above, and keeps only the fields listed on this page;
- doesn't store or forward your IP address;
- replaces the host and user name hash with a keyed hash computed with a secret that only the relay knows, so the stored value can't be matched against guessed host or user names;
- forwards the events to Datadog, where they are stored as logs.

Releases before 0.9.0 send the same data to the same address.
