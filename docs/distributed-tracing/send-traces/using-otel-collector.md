---
title: Using OTEL Collector
description: Using OTEL Collector
tags: [otel, collector, traces]
sidebar_position: 1
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

If you already have your own OpenTelemetry Collector and want to export your traces to Quickwit, you need a new OLTP gRPC exporter in your config.yaml:

<Tabs>

<TabItem value="macOS_windows" label="macOS/Windows">

```yaml title="otel-collector-config.yaml"
receivers:
  otlp:
    protocols:
      grpc:
      http:

processors:
  batch:

exporters:
  otlp/quickwit:
    endpoint: host.docker.internal:7281
    tls:
      insecure: true

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlp/quickwit]
```

</TabItem>

<TabItem value="linux" label="Linux">

```yaml title="otel-collector-config.yaml"
receivers:
  otlp:
    protocols:
      grpc:
      http:

processors:
  batch:

exporters:
  otlp/quickwit:
    endpoint: 127.0.0.1:7281
    tls:
      insecure: true

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlp/quickwit]
```

</TabItem>

</Tabs>


## Test your OTEL configuration

1. [Install](../../get-started/installation.md) and start a Quickwit server:
   
```bash
./quickwit run
```

2. Start a collector with the previous config:

<Tabs>

<TabItem value="macOS_windows" label="macOS/Windows">

```bash
docker run -v ${PWD}/otel-collector-config.yaml:/etc/otelcol/config.yaml -p 4317:4317 -p 4318:4318 -p 7281:7281 otel/opentelemetry-collector
```

</TabItem>

<TabItem value="linux" label="Linux">

```bash
docker run -v ${PWD}/otel-collector-config.yaml:/etc/otelcol/config.yaml --network=host -p 4317:4317 -p 4318:4318 -p 7281:7281 otel/opentelemetry-collector
```

</TabItem>

</Tabs>

3. Send a trace to your collector with cURL:

```bash
curl -XPOST "http://localhost:4318/v1/traces" -H "Content-Type: application/json" \
--data-binary @- << EOF
{
 "resource_spans": [
   {
     "resource": {
       "attributes": [
         {
           "key": "service.name",
           "value": {
             "string_value": "test-with-curl"
           }
         }
       ]
     },
     "scope_spans": [
       {
         "scope": {
           "name": "manual-test"
         },
         "spans": [
           {
             "time_unix_nano": "1678974011000000000",
             "start_time_unix_nano": "1678974011000000000",
             "end_time_unix_nano": "1678974021000000000",
             "trace_id": "3c191d03fa8be0653c191d03fa8be065",
             "span_id": "3c191d03fa8be065",
             "kind": 2,
             "events": [],
             "status": {
               "code": 1
             }
           }
         ]
       }
     ]
   }
 ]
}
EOF
```

You should see a log on the Quickwit server similar to the following:

```bash
2023-03-16T13:44:09.369Z  INFO quickwit_indexing::actors::indexer: new-split split_id="01GVNAKT5TQW0T2QGA245XCMTJ" partition_id=6444214793425557444
```

This means that Quickwit has received the trace and created a new split. Wait for the split to be published before searching for traces.

## Next step

Follow our tutorial on [how to send traces from your python app](using-otel-sdk-python.md).
