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

- `install`: the node started with a data directory that it had never used, so this counts new installations.
- `run_command`: the node started.
- `end_command`: the node stopped, with its exit code.
- `running`: the node is still up. Sent every 12 hours.
- `ui_index_page_load`: someone opened the Quickwit UI.

Each request also includes:

- The version of the request format.
- The Quickwit version, and how the binary was built: the commit, the target platform, and whether it was built from a release tag.
- The services the node runs, such as `indexer`, `searcher` or `metastore`.
- The features in use:
  - the metastore type, file-backed or PostgreSQL, if the node runs the metastore;
  - where the default index root URI points: S3, Azure Blob Storage, Google Cloud Storage or the local file system;
  - whether the S3 storage config targets an S3-compatible store, through a custom endpoint or a flavor such as MinIO, Garage, DigitalOcean Spaces or Google Cloud Storage;
  - whether the OTLP endpoints and the Jaeger endpoint are enabled;
  - whether ingest API v1 and v2 are enabled;
  - whether the searcher uses AWS Lambda for leaf searches or a local split cache;
  - whether document clustering is configured;
  - whether the REST or gRPC server uses TLS.
- The OS and the CPU architecture.
- The number of CPUs and the amount of memory available to the node, as ranges such as `5-8` CPUs or `8-16GiB`.
- Whether the node runs on Kubernetes, based on whether `KUBERNETES_SERVICE_HOST` is set.
- A session ID, generated at random each time the node starts.
- An install ID, generated at random the first time the node uses its data directory and stored there in the `telemetry_install_id` file. It is only created while telemetry is on.
- An MD5 hash of the host name and the user name, used to count distinct installations from older releases, which have no install ID. The relay replaces it with a keyed hash before storage. See below.
- An MD5 hash of the cluster ID and the metastore URI, without credentials, used to count clusters and the nodes in each. The relay replaces it with a keyed hash before storage.
- The number of events dropped because too many were queued.

`running` and `end_command` events also include:

- How many requests each API received since the previous report, as ranges such as `10-99`: search and bulk ingestion on the Elasticsearch-compatible API, other Elasticsearch-compatible requests, search and ingestion on the native API, OTLP, and Jaeger. Searches from the Quickwit UI count as native searches.
- On the node that runs the control plane only, so once per cluster: the number of indexes, as a range such as `2-10`, and the types of the sources created by users, such as `kafka` or `kinesis`.

Quickwit never sends index or field names, documents, queries, configuration values or credentials.

## Where the data goes

Quickwit sends the data over HTTPS to `telemetry.quickwit.io`, which Datadog operates on AWS. Its source code is in [`distribution/telemetry-relay`](https://github.com/quickwit-oss/quickwit/tree/main/distribution/telemetry-relay).

- The request goes through Amazon CloudFront, which looks up the country and city of your IP address and passes them on. The IP address itself goes no further.
- A small relay checks the request against the format above, rejects anything else, and keeps only the fields listed on this page.
- The relay replaces the host and cluster hashes with keyed hashes, computed with a secret that only the relay knows, so the stored values can't be matched against guessed host names, user names or cluster IDs.
- The relay forwards the events, with the country and city, to Datadog, where they are stored as logs. Your IP address is not stored.

For nodes running in the cloud, the country and city are usually those of the data center.

Releases 0.6.0 to 0.8.x send an older version of this data to the same address: the events other than `install`, without the API counts or cluster summary, plus the version, services, OS, architecture, Kubernetes flag, session ID, host hash, and the metastore, OTLP and Jaeger features.
