---
title: Source configuration
sidebar_position: 5
---

Quickwit can insert data into an index from one or multiple sources.
A source can be added after index creation using the [CLI command](../reference/cli.md#source) `quickwit source create`.
It can also be enabled or disabled with the `quickwit source enable/disable` subcommands.

A source is declared using an object called source config, which defines the source's settings. It consists of multiple parameters:

- source ID
- source type
- source parameters
- input_format
- maximum number of pipelines per indexer (optional)
- desired number of pipelines (optional)
- transform parameters (optional)

## Source ID

The source ID is a string that uniquely identifies the source within an index. It may only contain uppercase or lowercase ASCII letters, digits, hyphens (`-`), and underscores (`_`). Finally, it must start with a letter and contain at least 3 characters but no more than 255.

## Source type

The source type designates the kind of source being configured. Available source types are `ingest-api`, `kafka`, `kinesis`, `nats`, `pubsub`, and `pulsar`. The `file` type is also supported but only for local ingestion from [the CLI](/docs/reference/cli.md#tool-local-ingest).

## Source parameters

The source parameters indicate how to connect to a data store and are specific to the source type.

### File source

A file source reads data from files containing JSON objects separated by newlines (NDJSON). Gzip compression is supported provided that the file name ends with the `.gz` suffix.

#### Ingest a single file (CLI only)

To ingest a specific file, run the indexing directly in an adhoc CLI process with:

```bash
./quickwit tool local-ingest --index <index> --input-path <input-path>
```

Both local and object files are supported, provided that the environment is configured with the appropriate permissions. A tutorial is available [here](/docs/ingest-data/ingest-local-file.md).

#### Notification based file ingestion (beta)

Quickwit can automatically ingest all new files that are uploaded to an S3 bucket. This requires creating and configuring an [SQS notification queue](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ways-to-add-notification-config-to-bucket.html). A complete example can be found [in this tutorial](/docs/ingest-data/sqs-files.md).


The `notifications` parameter takes an array of notification settings. Currently one notifier can be configured per source and only the SQS notification `type` is supported.

Required fields for the SQS `notifications` parameter items:
- `type`: `sqs`
- `queue_url`: complete URL of the SQS queue (e.g `https://sqs.us-east-1.amazonaws.com/123456789012/queue-name`)
- `message_type`: format of the message payload, either
  - `s3_notification`: an [S3 event notification](https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventNotifications.html)
  - `raw_uri`: a message containing just the file object URI (e.g. `s3://mybucket/mykey`)
  - `deduplication_window_duration_sec`: maximum duration for which ingested files checkpoints are kept (default 3600)
  - `deduplication_window_max_messages`: maximum number of ingested file checkpoints kept (default 100k)
  - `deduplication_cleanup_interval_secs`: frequency at which outdated file checkpoints are cleaned up

*Adding a file source with SQS notifications to an index with the [CLI](../reference/cli.md#source)*

```bash
cat << EOF > source-config.yaml
version: 0.8
source_id: my-sqs-file-source
source_type: file
num_pipelines: 2
params:
  notifications:
    - type: sqs
      queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/queue-name
      message_type: s3_notification
EOF
./quickwit source create --index my-index --source-config source-config.yaml
```

:::note

- Quickwit does not automatically delete the source files after a successful ingestion. You can use [S3 object expiration](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html) to configure how long they should be retained in the bucket.
- Configure the notification to only forward events of type `s3:ObjectCreated:*`. Other events are acknowledged by the source without further processing and an warning is logged.
- We strongly recommend using a [dead letter queue](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html) to receive all messages that couldn't be processed by the file source. A `maxReceiveCount` of 5 is a good default value. Here are some common situations where the notification message ends up in the dead letter queue:
  - the notification message could not be parsed (e.g it is not a valid S3 notification)
  - the file was not found
  - the file is corrupted (e.g unexpected compression)
- AWS S3 notifications and AWS SQS provide "at least once" delivery guaranties. To avoid duplicates, the file source includes a mechanism that prevents the same file from being ingested twice. It works by storing checkpoints in the metastore that track the indexing progress for each file. You can decrease `deduplication_window_*` or increase `deduplication_cleanup_interval_secs` to reduce the load on the metastore.

:::

### Ingest API source

An ingest API source reads data from the [Ingest API](/docs/reference/rest-api.md#ingest-data-into-an-index). This source is automatically created at the index creation and cannot be deleted nor disabled.

### Kafka source

A Kafka source reads data from a Kafka stream. Each message in the stream must hold a JSON object.

A tutorial is available [here](/docs/ingest-data/kafka.md).

#### Kafka source parameters

The Kafka source consumes a `topic` using the client library [librdkafka](https://github.com/edenhill/librdkafka) and forwards the key-value pairs carried by the parameter `client_params` to the underlying librdkafka consumer. Common `client_params` options are bootstrap servers (`bootstrap.servers`), or security protocol (`security.protocol`). Please, refer to [Kafka](https://kafka.apache.org/documentation/#consumerconfigs) and [librdkafka](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) documentation pages for more advanced options.

| Property | Description | Default value |
| --- | --- | --- |
| `topic` | Name of the topic to consume. | required |
| `client_log_level` | librdkafka client log level. Possible values are: debug, info, warn, error. | `info` |
| `client_params` | librdkafka client configuration parameters. | `{}` |
| `enable_backfill_mode` | Backfill mode stops the source after reaching the end of the topic. | `false` |

**Kafka client parameters**

- `bootstrap.servers`
Comma-separated list of host and port pairs that are the addresses of a subset of the Kafka brokers in the Kafka cluster.

- `auto.offset.reset`
Defines the behavior of the source when consuming a partition for which there is no initial offset saved in the checkpoint. `earliest` consumes from the beginning of the partition, whereas `latest` (default) consumes from the end.

- `enable.auto.commit`
This setting is ignored because the Kafka source manages commit offsets internally using the [checkpoint API](../overview/concepts/indexing.md#checkpoint) and forces auto-commits to be disabled.

- `group.id`
Kafka-based distributed indexing relies on consumer groups. Unless overridden in the client parameters, the default group ID assigned to each consumer managed by the source is `quickwit-{index_uid}-{source_id}`.

- `max.poll.interval.ms`
Short max poll interval durations may cause a source to crash when back pressure from the indexer occurs. Therefore, Quickwit recommends using the default value of `300000` (5 minutes).

*Adding a Kafka source to an index with the [CLI](../reference/cli.md#source)*

```bash
cat << EOF > source-config.yaml
version: 0.8
source_id: my-kafka-source
source_type: kafka
num_pipelines: 2
params:
  topic: my-topic
  client_params:
    bootstrap.servers: localhost:9092
    security.protocol: SSL
EOF
./quickwit source create --index my-index --source-config source-config.yaml
```

### Kinesis source

A Kinesis source reads data from an [Amazon Kinesis](https://aws.amazon.com/kinesis/) stream. Each message in the stream must hold a JSON object.

A tutorial is available [here](/docs/ingest-data/kinesis.md).

**Kinesis source parameters**

The Kinesis source consumes a stream identified by a `stream_name` and a `region`.

| Property | Description | Default value |
| --- | --- | --- |
| `stream_name` | Name of the stream to consume. | required |
| `region` | The AWS region of the stream. Mutually exclusive with `endpoint`. | `us-east-1` |
| `endpoint` | Custom endpoint for use with AWS-compatible Kinesis service. Mutually exclusive with `region`. | optional |

If no region is specified, Quickwit will attempt to find one in multiple other locations and with the following order of precedence:

1. Environment variables (`AWS_REGION` then `AWS_DEFAULT_REGION`)

2. Config file, typically located at `~/.aws/config` or otherwise specified by the `AWS_CONFIG_FILE` environment variable if set and not empty.

3. Amazon EC2 instance metadata service determining the region of the currently running Amazon EC2 instance.

4. Default value: `us-east-1`

*Adding a Kinesis source to an index with the [CLI](../reference/cli.md#source)*

```bash
cat << EOF > source-config.yaml
version: 0.7
source_id: my-kinesis-source
source_type: kinesis
params:
  stream_name: my-stream
EOF
quickwit source create --index my-index --source-config source-config.yaml
```

### NATS source

A NATS source reads data from a [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) stream through a durable consumer. Each message carries one payload in the source's [input format](#input-format): a single JSON object (`json`, the default), a plain text document (`plain_text`), or an OTLP export request whose log records or spans are each indexed as a separate document (`otlp_*` formats). Payloads must not exceed 1 MiB.

A tutorial is available [here](/docs/ingest-data/nats.md).

The durable consumer is provisioned externally so its lifecycle, subject filters, deliver policy, and ack tuning belong to whoever provisioned it.

Delivery is **exactly-once on planned teardowns and at-least-once on crashes**. On a planned teardown (node shutdown, pipeline reassignment on a `num_pipelines` change), the pipeline is drained first: the source stops pulling, the in-flight messages are committed, published, and acknowledged before the pipeline stops, so nothing is indexed twice. Messages the pipeline had prefetched but not processed are negatively acknowledged so the remaining pipelines pick them up immediately. The drain runs under a time budget, the indexer's [`shutdown_drain_timeout`](node-config.md#indexer-configuration). A drain that cannot finish within it (e.g. the object storage or the metastore is unavailable) is abandoned and delivery degrades to at-least-once, as on a crash. After a crash, the unacknowledged messages are redelivered after `ack_wait` and indexed again, as duplicates.

The source never waits on unreachable NATS servers: acknowledgments and the negative acknowledgments sent at teardown are bounded by a 10 s timeout, unconfirmed acknowledgments stay pending and are retried at the next split publication, and a pipeline can still be stopped or reassigned during an outage.

#### Consumer invariants

These are properties of the consumer, not of the source. Only the ack policy is enforced when the source is created. The pull limits and `max_deliver` are checked when a pipeline starts and only produce a warning in the indexer logs. The rest are not checked at all, and getting them wrong looks like Quickwit being slow or duplicating rather than like a consumer misconfiguration.

**`ack_policy` must be `explicit`.** The source acknowledges each message individually once the split containing it is published, and waits for the server to confirm the acknowledgment — the confirmation is what tells the drain that the pipeline is empty. The consumer's ack floor is therefore the resume point, and it is the only progress state that matters. Any other policy is rejected when the source is created.

**`ack_wait` must exceed the end-to-end publish latency.** The timer starts at delivery and has to outlast three terms:

```
ack_wait > commit_timeout + split upload and publish + ack round trip
```

If `ack_wait` is shorter, NATS redelivers messages that are still being indexed; they are then indexed twice. 5 minutes with a 60 s `commit_timeout` leaves a wide margin.

It is also the *recovery* time from a lost delivery, so it should not be arbitrarily large either. If a message is delivered but never arrives (a dropped connection, a slow-consumer disconnect) nothing brings it back until the timer expires and the pipeline simply idles.

**`max_ack_pending` should be `-1`.** Nothing is acknowledged until a split is published, so a whole commit window is always ack-pending. This setting is a hard throughput cap rather than a safety valve:

```
achievable rate ≈ max_ack_pending / commit_timeout   documents per second
```

Measured at four pipelines with a 10 s commit timeout: `1000` gave 97 documents per second where the formula predicts 100, and `20000` gave 2,115 where it predicts 2,000. A bound that looks generous for an ordinary queue consumer throttles indexing. If the in-flight window has to be bounded, size it above `throughput × commit_timeout` rather than at a small absolute number, and note the trade: unlimited also makes the crash-replay window the whole delivered span above the ack floor rather than a bounded slice.

**`max_deliver` should stay unlimited (`-1`).** Redelivery is what recovers from a crash or a lost delivery. A finite value turns it into data loss: a message that exhausts it is never delivered again and is silently skipped.

**`max_batch`, `max_bytes` must not be below what the source pulls.** Each pull request asks for up to `QW_NATS_PULL_MAX_MESSAGES_PER_BATCH` messages and `QW_NATS_PULL_MAX_BYTES_PER_BATCH` bytes. A consumer limit below any of those makes the server reject every pull request and the pipeline idles. The source warns at start.

**The consumer must outlive the source.** Deleting it, or letting an `inactive_threshold` expire, while a source is bound to it does not stop the pipelines: the server answers their pull requests with "no responders", which is also what a JetStream outage produces, so the source retries indefinitely and warns. Recreate the consumer under the same name to resume, or disable the source.

**`deliver_policy`** is usually `all`, so a new consumer indexes the stream from the start.

**Give each consumer a single `filter_subject`.** A consumer configured with several filters through NATS 2.10's multi-filter `filter_subjects` appears to take a much slower path in the broker: on a stream whose subjects are interleaved, sixteen multi-filter consumers took 7.5 times longer than the same sixteen consumers with one filter each, with the broker saturated and the indexers idle.

#### What can be tuned on the Quickwit side

| Setting | Where | Effect |
| --- | --- | --- |
| `num_pipelines` | source config | Pipelines bound to the consumer. Scales cleanly on large messages: 52 → 147 MiB/s from one to four pipelines at 512 KiB. On small messages (1 KiB) the acknowledgment path caps the aggregate, and it is worth only about 1.15× from one to sixteen — there, parallelism has to come from more consumers. |
| [`commit_timeout_secs`](index-config.md#indexing-settings) | index config | Sets the size of the always-ack-pending window, so it interacts with `max_ack_pending` and with `ack_wait`. |
| `QW_NATS_PULL_MAX_BYTES_PER_BATCH` | env, default 10 MiB | Bounds the bytes the server may push per pull request. It must stay well under the server's per-connection `max_pending` (64 MiB by default), or the server declares a slow consumer and closes the connection; the messages are already counted as delivered, so the pipeline then idles for a full `ack_wait`. It must also stay above the server's `max_payload`, or larger messages are never delivered, and at or below the consumer's `max_bytes`; the source warns at start otherwise. 10 MiB and 20 MiB measure identically. |
| `QW_NATS_PULL_MAX_MESSAGES_PER_BATCH` | env, default 100,000 | Messages per pull request. Can be used along `QW_NATS_PULL_MAX_BYTES_PER_BATCH` to limit a batch if there is too much contention around acknowledgements. The client buffers eight such batches per subscription; messages beyond that are dropped client-side and come back after `ack_wait`. |

#### Scaling

Several pipelines can bind to the same consumer and NATS load-balances the messages across them, so scaling is a plain `num_pipelines` update and the control plane places the pipelines across the indexers of the cluster.

#### Acknowledgment cost

One confirmed acknowledgment per message is the source's dominant broker cost, and it is what decides whether the source or the indexer is the bottleneck.

- At **512 KiB** per message it is invisible. The NATS and Kafka sources measured within a percent of each other across the whole pipeline sweep.
- At **1 KiB** it is the ceiling. Sixteen per-tenant consumers reached 72 MiB/s while an equivalent Kafka source reached 150, and the broker spent 5.1 of the host's cores on acknowledgments against Quickwit's 4.7 on indexing — about ten times the broker CPU per document that a periodic offset commit costs. Priced on the broker alone, acknowledgment divides throughput by 6.5 at this size.

#### Monitoring

Being durable, the consumer is observable through NATS's own monitoring (`nats consumer info`, exporters): `num_pending` is the indexing lag and `num_ack_pending` the in-flight window, both available even while the pipelines are down. The source also reports `num_pending_acks` (messages indexed but whose split is not published yet) in its observable state.

When a message carries a W3C `traceparent` header and the indexers export their traces (see [distributed tracing](../distributed-tracing/plug-quickwit-to-jaeger.md)), the publisher's trace extends to Quickwit: a `process_nats_message` span covers the message's processing until it is acknowledged.

**NATS source parameters**

| Property | Description | Default value |
| --- | --- | --- |
| `uris` | List of NATS server URIs (e.g. `nats://localhost:4222`). | required |
| `stream` | Name of the JetStream stream to consume. | required |
| `consumer` | Name of the pre-provisioned durable consumer to bind to. | required |
| `tls` | TLS options: `ca_certificates_path` (PEM file whose root certificates are trusted instead of the system ones), and `client_certificate_path` + `client_key_path` (PEM files, set together) for mutual TLS. TLS itself is enabled by connecting to `tls://` URIs. The files are read when a connection is established: by the indexer nodes running the source, and by the node serving the source creation or update, which checks connectivity. | optional |
| `authentication` | Authentication parameters: either `user_password` (with `user` and `password`) or `token`. | optional |

*Adding a NATS source to an index with the [CLI](../reference/cli.md#source)*

```bash
cat << EOF > source-config.yaml
version: 0.8
source_id: my-nats-source
source_type: nats
num_pipelines: 2
params:
  uris:
    - nats://localhost:4222
  stream: my-stream
  consumer: my-consumer
EOF
./quickwit source create --index my-index --source-config source-config.yaml
```

### Pulsar source

A Puslar source reads data from one or several Pulsar topics. Each message in topic(s) must hold a JSON object.

A tutorial is available [here](/docs/ingest-data/pulsar.md).

**Pulsar source parameters**

The Pulsar source consumes `topics` using the client library [pulsar-rs](https://github.com/streamnative/pulsar-rs).

| Property | Description | Default value |
| --- | --- | --- |
| `topics` | List of topics to consume. | required |
| `address` | Pulsar URL (pulsar:// and pulsar+ssl://). | required |
| `consumer_name` | The consumer name to register with the pulsar source. | `quickwit` |

*Adding a Pulsar source to an index with the [CLI](../reference/cli.md#source)*

```bash
cat << EOF > source-config.yaml
version: 0.7
source_id: my-pulsar-source
source_type: pulsar
params:
  topics:
    - my-topic
  address: pulsar://localhost:6650
EOF
./quickwit source create --index my-index --source-config source-config.yaml
```

## Number of pipelines

The `num_pipelines` parameter is only available for distributed sources like Kafka, GCP PubSub, NATS, and Pulsar.

It defines the number of pipelines to run on a cluster for the source. The actual placement of these pipelines on the different indexer
will be decided by the control plane.

:::info

Note that distributing the indexing load of partitioned sources like Kafka is done by assigning the different partitions to different pipelines. As a result, it is important to ensure that the number of partitions is a multiple of `num_pipelines`.

Also, assuming you are only indexing a single Kafka source in your Quickwit cluster, you should set the number of pipelines to a multiple of the number of indexers. Finally, if your indexing throughput is high, you should provision between 2 and 4 vCPUs per pipeline.

For instance, assume you want to index a 60-partition topic, with each partition receiving a throughput of 10 MB/s. If you measured that Quickwit can index your data at a pace of 40MB/s per pipeline, a possible setting could be:
- 5 indexers with 8 vCPUs each
- 15 pipelines

Each indexer will then be in charge of 3 pipelines, and each pipeline will cover 4 partitions.
:::


## Transform parameters

For all source types but the `ingest-api`, ingested documents can be transformed before being indexed using [Vector Remap Language (VRL)](https://vector.dev/docs/reference/vrl/) scripts.

| Property | Description | Default value |
| --- | --- | --- |
| `script` | Source code of the VRL program executed to transform documents. | required |
| `timezone` | Timezone used in the VRL program for date and time manipulations. It must be a valid name in the [TZ database](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) | `UTC` |

```yaml
# Your source config here
# ...
transform:
  script: |
    .message = downcase(string!(.message))
    .timestamp = now()
    del(.username)
  timezone: local
```

## Input format

The `input_format` parameter specifies the expected data format of the source. The formats currently supported are:
- `json` (default)
- `otlp_logs_json`
- `otlp_logs_proto`
- `otlp_traces_json`
- `otlp_traces_proto`
- `plain_text`

*OTLP formats*

When ingesting OTLP data into an OTLP logs or traces index with a source other than the native OTEL endpoints, use this parameter to specify whether the exported logs or traces will be serialized in JSON or Protobuf. When possible, prefer the latter, which is a more compact encoding.

*Plaint text format*

Use this parameter for unstructured text data. Internally, Quickwit can only index JSON data. To allow the ingestion of plain text documents, Quickwit transform them on the fly into JSON objects of the following form: `{"plain_text": "<original plain text document>"}`. Then, they can be optionally transformed into more complex documents using a VRL script. (see [transform feature](#transform-parameters)).

The following is an example of how one could parse and transform a CSV dataset containing a list of users described by 3 attributes: first name, last name, and age.

```yaml
# Your source config here
# ...
input_format: plain_text
transform:
  script: |
    user = parse_csv!(.plain_text)
    .first_name = user[0]
    .last_name = user[1]
    .age = to_int!(user[2])
    del(.plain_text)
```

## Enabling/disabling a source from an index

A source can be enabled or disabled from an index using the [CLI command](../reference/cli.md) `quickwit source enable` or `quickwit source disable`:

```bash
quickwit source disable --index my-index --source my-source
```

A source is enabled by default. When disabling a source, the related indexing pipelines will be shut down on each relevant indexer and indexing for this source will be paused.

## Deleting a source from an index

A source can be removed from an index using the [CLI command](../reference/cli.md) `quickwit source delete`:

```bash
quickwit source delete --index my-index --source my-source
```

When deleting a source, the checkpoint associated with the source is also removed.
