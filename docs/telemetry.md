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

Any value disables telemetry, even an empty value or `false`.

To check whether telemetry is on, run `quickwit --help`. While it is on, the output includes a `Telemetry: enabled` line:

```bash
quickwit --help
Sub-second search & analytics engine on cloud storage.
  Find more information at https://quickwit.io/docs

Telemetry: enabled
```

The line disappears when telemetry is off. If the node can't set up HTTPS, for example because no CA certificates are installed, `quickwit run` logs a warning and sends nothing even though the line is shown.

Only `quickwit run` sends telemetry. Other CLI commands never do.

## What is sent

Each time a node starts, it sends a request right away. After that, it sends at most one request per minute, plus one last request with the pending events when it shuts down. Each request holds up to 10 events.

Events, each with the time it happened, in seconds since the Unix epoch:

- `install`: the node found no install ID in its data directory and created one. This happens with a new data directory, but also on the first start after upgrading from a release without install IDs, and on the first start after telemetry is turned on.
- `run_command`: the node started.
- `end_command`: the node stopped, with its exit code.
- `running`: the node is still up. Sent every 12 hours.

Each request also includes:

- The version of the request format.
- The Quickwit version, and how the binary was built: the commit, the target platform, and whether it was built from a release tag.
- The services the node runs, such as `indexer`, `searcher` or `metastore`.
- The features in use:
  - the metastore type, file-backed or PostgreSQL, if the node runs the metastore or a metastore read replica;
  - where the default index root URI points: S3, Azure Blob Storage, Google Cloud Storage or the local file system;
  - whether the S3 storage config targets an S3-compatible store, through a custom endpoint or a flavor such as MinIO, Garage, DigitalOcean Spaces or Google Cloud Storage;
  - whether the OTLP and Jaeger endpoints are enabled in the node configuration;
  - whether ingest API v1 and v2 are enabled;
  - whether the searcher uses AWS Lambda for leaf searches or a local split cache;
  - whether document clustering is configured;
  - whether the REST or gRPC server uses TLS.
- The OS and the CPU architecture.
- The number of CPUs and the amount of memory available to the node, as ranges such as `5-8` CPUs or `8-16GiB`.
- Whether the node runs on Kubernetes, based on whether `KUBERNETES_SERVICE_HOST` is set.
- A session ID, generated at random each time the node starts.
- An install ID, generated at random when the node starts with telemetry on and finds none in its data directory, then stored there in the `telemetry_install_id` file. It is never created while telemetry is off.
- An MD5 hash of the host name and the user name, used to count distinct installations from older releases, which have no install ID. The relay replaces it with a keyed hash before storage. See below.
- The number of events dropped because too many were queued.

`running` and `end_command` events also include how many requests each API received since the previous report, as ranges such as `10-99`: search and bulk ingestion on the Elasticsearch-compatible API, other Elasticsearch-compatible requests, search and ingestion on the native API, OTLP, and Jaeger. Searches from the Quickwit UI count as native searches. They also include how many times the node served the Quickwit UI page, in the same ranges. Browsers request it when someone opens or reloads the UI, and so does any other client that requests a UI URL, such as an uptime check.

Quickwit never sends index or field names, documents, queries, configuration values or credentials.

## Where the data goes

By default, Quickwit sends the data over HTTPS to `telemetry.quickwit.io`, which Datadog operates on AWS.

- The request goes through Amazon CloudFront, which looks up the country and city of your IP address and passes them on to the relay. CloudFront also passes on the IP address itself, in the `X-Forwarded-For` header that it adds to every request, but the relay ignores it.
- A small relay checks the request against the format above, rejects anything else, and keeps only the fields listed on this page.
- The relay replaces the host hash with a keyed hash, computed with a secret that only the relay knows, so the stored value can't be matched against guessed host names and user names.
- The relay forwards the events, with the country and city, to Datadog, where they are stored as logs. Your IP address is not stored.

For nodes running in the cloud, the country and city are usually those of the data center.

To send the data somewhere else instead, for example to a local HTTP server to see exactly what is sent, set `TELEMETRY_INGEST_API` to a URL before starting Quickwit. Nothing then goes to `telemetry.quickwit.io`, and the relay is bypassed: the receiver gets the requests as Quickwit sends them, unencrypted with an `http://` URL. This does not turn telemetry off.

Releases 0.6.0 to 0.8.x send an older version of this data to the same address: the events other than `install`, each with its time, without the API counts, plus the version, services, OS, architecture, Kubernetes flag, session ID, host hash, number of dropped events, and the metastore, OTLP and Jaeger features. Their AWS Lambda indexer and searcher send `run_command` with only the `aws_lambda` feature.

Releases before 0.6.0 also send data to the same address, from every CLI command: an event when a command ends, with its exit code, and an event when an index is created, ingested into, deleted or garbage collected, or when services start, with their names (in 0.1.x, with whether peer addresses were given). Each event has its time, and each request includes the number of dropped events, the version, OS, architecture, Kubernetes flag, session ID and host hash.

To turn telemetry off in releases before 0.9.0, set `QW_DISABLE_TELEMETRY` to any value, or `DISABLE_QUICKWIT_TELEMETRY` in 0.1.x.
