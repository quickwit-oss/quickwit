---
title: Send logs using Fluentbit
sidebar_label: Using Fluentbit
description: A simple tutorial to send logs from Fluentbit to Quickwit in a few minutes.
icon_url: /img/tutorials/fluentbit-logo.png
tags: [logs, ingestion]
sidebar_position: 4
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

[Fluent Bit](https://fluentbit.io/) is an open-source logging and metrics processor and forwarder to multiple destinations.

In this guide, we will show you how to connect it to Quickwit.

## Prerequisites

- [Install Quickwit](/docs/get-started/installation.md)
- Start a Quickwit instance with `./quickwit run`
- [Install Fluentbit](https://docs.fluentbit.io/manual/installation/getting-started-with-fluent-bit)


## Create a simple index for Fluentbit logs

Let's create a schemaless index with only one field `timestamp`. The mode `dynamic` indicates that Quickwit will index all fields even if they are not defined in the doc mapping.

```yaml title="index-config.yaml"

version: 0.5

index_id: fluentbit-logs

doc_mapping:
  mode: dynamic
  field_mappings:
    - name: timestamp
      type: datetime
      input_formats:
        - unix_timestamp
      output_format: unix_timestamp_secs
      fast: true
  timestamp_field: timestamp

indexing_settings:
  commit_timeout_secs: 10
```

```bash
curl -o fluentbit-logs.yaml https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/fluentbit-logs/index-config.yaml
```

And then create the index with `cURL` or the `CLI`:

<Tabs>

<TabItem value="curl" label="cURL">

```bash
curl -XPOST http://localhost:7280/api/v1/indexes -H "content-type: application/yaml" --data-binary @fluentbit-logs.yaml
```

</TabItem>

<TabItem value="cli" label="CLI">

```bash
./quickwit index create --index-config fluentbit-logs.yaml
```

</TabItem>

</Tabs>


## Setup Fluentbit

Fluentbit configuration file is made of inputs and outputs. For this tutorial, we will use a dummy configuration:

``` title=fluent-bit.conf
[INPUT]
  Name   dummy
  Tag    dummy.log

[OUTPUT]
  Name http
  Match *
  URI   /api/v1/fluentbit-logs/ingest
  Host  localhost
  Port  7280
  tls   Off
  Format json_lines
  Json_date_key    timestamp
  Json_date_format epoch
```

Fluentbit will send `dummy` logs to Quickwit endpoint `/api/v1/fluentbit-logs/ingest`.

Let's start Fluentbit.

```bash
fluent-bit -c fluent-bit.conf
```

## Search logs

Quickwit is now ingesting logs coming from Fluentbit and you can search them either with `curl` or by using the UI:
- `curl "http://127.0.0.1:7280/api/v1/fluentbit-logs/search?query=severity:DEBUG"`
- Open your browser at `http://127.0.0.1:7280/ui/search?query=severity:DEBUG&index_id=fluentbit-logs&max_hits=10`.


## Further improvements

You will soon be able to do aggregations on dynamic fields (planned for 0.6).
