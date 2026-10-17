---
title: Send logs from Vector
sidebar_label: Using Vector
description: A simple tutorial to send logs from Vector to Quickwit in a few minutes.
icon_url: /img/tutorials/vector-logo.png
tags: [logs, ingestion]
sidebar_position: 3
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

[Vector](https://vector.dev/) is an amazing piece of software (in Rust obviously) and brings a new fresh wind in the observability space,
it is well-known for collecting logs from every part of your infrastructure, transforming and aggregating them, and finally forwarding them to a sink.

In this guide, we will show you how to connect it to Quickwit.

## Start Quickwit server

<Tabs>

<TabItem value="cli" label="CLI">

```bash
# Create Quickwit data dir.
mkdir qwdata
./quickwit run
```

</TabItem>

<TabItem value="docker" label="Docker">

```bash
# Create Quickwit data dir.
mkdir qwdata
docker run --rm -v $(pwd)/qwdata:/quickwit/qwdata -p 7280:7280 quickwit/quickwit run
```

</TabItem>

</Tabs>

## Taking advantage of Quickwit's native support for logs

Let's embrace the OpenTelemetry standard and take advantage of Quickwit features. With the native support for OpenTelemetry standards, Quickwit already comes with an index called `otel-logs_v0_7` that is compatible with the OpenTelemetry [logs data model](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/logs/data-model.md). This means we can start pushing log data without any prior usual index setup.

The OpenTelemetry index configuration can be found in the [quickwit-opentelemetry/src/otlp/logs.rs](https://github.com/quickwit-oss/quickwit/blob/main/quickwit/quickwit-opentelemetry/src/otlp/logs.rs) source file.

## Setup Vector

Our sink here will be Quickwit ingest API `http://127.0.0.1:7280/api/v1/otel-logs-v0_7/ingest`.
To keep it simple in this tutorial, we will use a log source called `demo_logs` that generates logs in a given format. Let's choose the common `syslog` format
(Vector does not generate logs in the OpenTelemetry format directly!) and use the transform feature to map the `syslog` format into the OpenTelemetry format.


```toml title=vector.toml
[sources.generate_syslog]
type = "demo_logs"
format = "syslog"
count = 100000
interval = 0.001

[transforms.remap_syslog]
inputs = [ "generate_syslog"]
type = "remap"
source = '''
  structured = parse_syslog!(.message)
  .timestamp_nanos = to_unix_timestamp!(structured.timestamp, unit: "nanoseconds")
  .body = structured
  .service_name = structured.appname
  .resource_attributes.source_type = .source_type
  .resource_attributes.host.hostname = structured.hostname
  .resource_attributes.service.name = structured.appname
  .attributes.syslog.procid = structured.procid
  .attributes.syslog.facility = structured.facility
  .attributes.syslog.version = structured.version
  .severity_text = if includes(["emerg", "err", "crit", "alert"], structured.severity) {
    "ERROR"
  } else if structured.severity == "warning" {
    "WARN"
  } else if structured.severity == "debug" {
    "DEBUG"
  } else if includes(["info", "notice"], structured.severity) {
    "INFO"
  } else {
   structured.severity
  }
  .scope_name = structured.msgid
  del(.message)
  del(.host)
  del(.timestamp)
  del(.service)
  del(.source_type)
'''

# useful to see the logs in the terminal
# [sinks.emit_syslog]
# inputs = ["remap_syslog"]
# type = "console"
# encoding.codec = "json"

[sinks.quickwit_logs]
type = "http"
method = "post"
inputs = ["remap_syslog"]
encoding.codec = "json"
framing.method = "newline_delimited"
uri = "http://127.0.0.1:7280/api/v1/otel-logs-v0_7/ingest"
```
Download the above Vector config file.

```bash
curl -o vector.toml https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/vector-otel-logs/vector.toml
```

Now let's start Vector so that we can start sending logs to Quickwit.

```bash
docker run -v $(pwd)/vector.toml:/etc/vector/vector.toml:ro -p 8383:8383 --net=host timberio/vector:0.25.0-distroless-libc
```

## Search logs

Quickwit is now ingesting logs coming from Vector and you can search them either with `curl` or by using the UI:
- `curl -XGET http://127.0.0.1:7280/api/v1/otel-logs-v0_7/search?query=severity_text:ERROR`
- Open your browser at `http://127.0.0.1:7280/ui/search?query=severity_text:ERROR&index_id=otel-logs-v0_7&max_hits=10` and play with it!

## Compute aggregation on severity_text

For aggregations, we can't use yet Quickwit UI but we can use cURL.

Let's craft a nice aggregation query to count how many `INFO`, `DEBUG`, `WARN`, and `ERROR` per minute (all datetime are stored in microseconds thus the interval of 60_000_000 microseconds) we have:

```json title=aggregation-query.json
{
  "query": "*",
  "max_hits": 0,
  "aggs": {
    "count_per_minute": {
      "histogram": {
          "field": "timestamp_nanos",
          "interval": 60000000
      },
      "aggs": {
        "severity_text_count": {
          "terms": {
            "field": "severity_text"
          }
        }
      }
    }
  }
}
```

```bash
curl -XPOST -H "Content-Type: application/json" http://127.0.0.1:7280/api/v1/otel-logs-v0_7/search --data @aggregation-query.json
```

## Going further

Now you can also deploy Grafana and connect to Quickwit as data source for query, dashboard, alerts and more!
