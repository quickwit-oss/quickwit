---
title: NATS
description: A short tutorial describing how to set up Quickwit to ingest data from NATS JetStream in a few minutes
tags: [nats, integration]
icon_url: /img/tutorials/nats.svg
sidebar_position: 4
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

In this tutorial, we will describe how to set up Quickwit to ingest data from [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) in a few minutes. First, we will create an index and configure a NATS source. Then, we will create a JetStream stream and load some events from the [Stack Overflow dataset](https://www.kaggle.com/stackoverflow/stacksample) into it. Finally, we will execute some searches.

## Prerequisites

You will need the following to complete this tutorial:
- A local running [Quickwit instance](/docs/get-started/installation.md)
- A local running NATS server (2.10+) with JetStream enabled
- The [NATS CLI](https://github.com/nats-io/natscli)

### Quickwit setup

[Download](/docs/get-started/installation.md) Quickwit and start a server. Then open a new terminal to execute CLI commands with the same binary.

```bash
./quickwit run
```

Test that the cluster is running:

```bash
./quickwit index list
```

### NATS setup

<Tabs>

<TabItem value="Local" label="Local">

Download the [NATS server](https://docs.nats.io/running-a-nats-service/introduction/installation) and start it with JetStream enabled:

```bash
nats-server --jetstream
```

</TabItem>

<TabItem value="Docker" label="Docker">

```bash
docker run -it -p 4222:4222 nats:2.10 --jetstream
```

See the details on the [official documentation](https://docs.nats.io/running-a-nats-service/nats_docker).

</TabItem>

</Tabs>

## Prepare Quickwit

First, let's create a new index. Here is the index config and doc mapping corresponding to the schema of Stack Overflow posts:

```yaml title="index-config.yaml"
#
# Index config file for Stack Overflow dataset.
#
version: 0.7

index_id: stackoverflow

doc_mapping:
  field_mappings:
    - name: user
      type: text
      fast: true
      tokenizer: raw
    - name: tags
      type: array<text>
      fast: true
      tokenizer: raw
    - name: type
      type: text
      fast: true
      tokenizer: raw
    - name: title
      type: text
      tokenizer: default
      record: position
      stored: true
    - name: body
      type: text
      tokenizer: default
      record: position
      stored: true
    - name: questionId
      type: u64
    - name: answerId
      type: u64
    - name: acceptedAnswerId
      type: u64
    - name: creationDate
      type: datetime
      fast: true
      input_formats:
        - rfc3339
      fast_precision: seconds
  timestamp_field: creationDate

search_settings:
  default_search_fields: [title, body]

indexing_settings:
  commit_timeout_secs: 10
```

Execute these Bash commands to download the index config and create the `stackoverflow` index.

```bash
# Download stackoverflow index config.
wget -O stackoverflow.yaml https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/stackoverflow/index-config.yaml

# Create index.
./quickwit index create --index-config stackoverflow.yaml
```

## Create a JetStream stream and a durable consumer

The NATS source consumes a JetStream stream through a durable consumer, so messages must be published on subjects captured by a stream, and the consumer must be provisioned before creating the source — Quickwit only ever fetches it, and never creates, updates, nor deletes it. Let's create both:

```bash
nats stream add stackoverflow --subjects "stackoverflow.posts" --defaults
cat > quickwit-consumer.json <<EOF
{
  "durable_name": "quickwit-consumer",
  "deliver_policy": "all",
  "ack_policy": "explicit",
  "ack_wait": 300000000000,
  "max_ack_pending": -1
}
EOF
nats consumer add stackoverflow quickwit-consumer --config quickwit-consumer.json
```

The consumer is described in a JSON file because the `nats` CLI cannot express an unbounded `max_ack_pending` through its `--max-pending` flag. Durations in the file are in nanoseconds, so `300000000000` is the 5 minute `ack_wait`.

:::info

The consumer must use the explicit ack policy: the source acknowledges each message once it is durably indexed, and the consumer's ack floor is the resume point. The subject filters, the deliver policy, and the ack tuning are properties of the consumer, not of the Quickwit source.

Two of those settings decide whether indexing works at all, and neither is checked when the source is created:

- `ack_wait` must outlast the whole path from delivery to acknowledgment — the commit timeout, plus the split upload and publish, plus the acknowledgment round trip. Shorter than that and NATS redelivers messages that are still being indexed, which duplicates them. `5m` against this tutorial's 10 s commit timeout is a wide margin. It is also the recovery time from a lost delivery, so it should not be arbitrarily large either.
- `max_ack_pending: -1` leaves the in-flight window unbounded. The source acknowledges only once a split is published, so a whole commit window is always ack-pending and a bounded value is a hard throughput cap of roughly `max_ack_pending / commit_timeout` documents per second.

See [Consumer invariants](../configuration/source-config.md#consumer-invariants) for the details, and [Scaling](../configuration/source-config.md#scaling) for how to grow past one pipeline.

:::

## Create the NATS source

A NATS source just needs to define the server URIs, the stream, and the durable consumer to bind to.

```yaml title="nats-source.yaml"
#
# NATS source config file.
#
version: 0.8
source_id: nats-source
source_type: nats
params:
  uris:
    - nats://localhost:4222
  stream: stackoverflow
  consumer: quickwit-consumer
```

Run these commands to download the source config file and create the source.

```bash
# Download NATS source config.
wget -O stackoverflow-nats-source.yaml https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/stackoverflow/nats-source.yaml

# Create source.
./quickwit source create --index stackoverflow --source-config stackoverflow-nats-source.yaml
```

As soon as the NATS source is created, the Quickwit control plane will ask an indexer to start a new indexing pipeline. You will see logs like below on the indexer:

```bash
INFO spawn_pipeline{index=stackoverflow gen=0}: quickwit_indexing::source::nats_source: starting NATS source index_id=stackoverflow source_id=nats-source stream=stackoverflow consumer_name=quickwit-consumer
```

The consumer's indexing lag and ack floor are visible at any time — even while the pipeline is down — with `nats consumer info stackoverflow quickwit-consumer`.

## Populate the stream

To populate the stream, we will use a python script:

```python title=send_messages_to_nats.py
import asyncio

import nats


async def main():
    client = await nats.connect("nats://localhost:4222")
    jetstream = client.jetstream()

    with open("stackoverflow.posts.transformed-10000.json", encoding="utf8") as file:
        for i, line in enumerate(file):
            await jetstream.publish("stackoverflow.posts", line.strip().encode("utf-8"))
            if i % 1000 == 0:
                print(f"{i}/10000 messages sent.")

    await client.close()


asyncio.run(main())
```

Install the [python client](https://github.com/nats-io/nats.py) locally and run the script:

```bash
# Download the first 10_000 Stackoverflow posts articles.
curl -O https://quickwit-datasets-public.s3.amazonaws.com/stackoverflow.posts.transformed-10000.json

# Install nats python client.
pip3 install nats-py
wget https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/stackoverflow/send_messages_to_nats.py
python3 send_messages_to_nats.py
```

## Time to search!

You can run this command to inspect the properties of the index and check the current number of published splits and documents:

```bash
# Display some general information about the index.
./quickwit index describe --index stackoverflow
```

You will notably see the number of published documents.

You are now ready to execute some queries.

```bash
curl 'http://localhost:7280/api/v1/stackoverflow/search?query=search+AND+engine'
```

If your Quickwit server is local, you can access the results through the Quickwit UI on [localhost:7280](http://localhost:7280/ui/search?query=&index_id=stackoverflow&max_hits=10).

## Tear down resources (optional)

Let's delete the files and resources created for the purpose of this tutorial.

```bash
# Delete quickwit index.
./quickwit index delete --index stackoverflow --yes
# Delete NATS stream, and with it the consumer it holds.
nats stream rm -f stackoverflow
```

Note that deleting a Quickwit source does **not** delete its consumer: the consumer is provisioned outside of Quickwit and outlives the source. Here the stream is deleted too, which removes it. Deleting only the source would leave the consumer behind, still holding its position.

This concludes the tutorial. If you have any questions regarding Quickwit or encounter any issues, don't hesitate to ask a [question](https://github.com/quickwit-oss/quickwit/discussions) or open an [issue](https://github.com/quickwit-oss/quickwit/issues) on [GitHub](https://github.com/quickwit-oss/quickwit) or contact us directly on [Discord](https://discord.com/invite/MT27AG5EVE).
