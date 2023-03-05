---
title: Instrument your Python application and send traces to Quickwit
description: A simple tutorial to send traces to Quickwit from a Python Flask app.
icon_url: /img/tutorials/python-logo.png
tags: [python, traces, ingestion]
sidebar_position: 2
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

In this tutorial, we will show you how to instrument a Python [Flask](https://flask.palletsprojects.com/en/2.2.x/) app with OpenTelemetry and send traces to Quickwit. This tutorial is based on the [Python OpenTelemetry](https://opentelemetry.io/docs/instrumentation/python/getting-started/) documentation.

## Prerequisites

- Python3 in a virtual environment (`python3 -m venv . && source ./bin/activate`)
- A running [Quickwit instance](/docs/get-started/installation.md) with OTLP service enabled: `QW_ENABLE_OTLP_ENDPOINT=true ./quickwit run`
- A running Jaeger UI instance:

```bash
docker run --rm --name jaeger-qw \
    -e SPAN_STORAGE_TYPE=grpc-plugin \
    -e GRPC_STORAGE_SERVER=host.docker.internal:7281 \
    -p 16686:16686 \
    jaegertracing/jaeger-query:latest
```

## Create a Flask app

We will implement a flask application that doing three things: fetching an IP address, parsing it, and displaying it. Our golang app will be instrumenting each step with OpenTelemetry and send traces to Quickwit.

Let's intall the dependencies:

```bash
pip install flask
pip install opentelemetry-distro
```

The opentelemetry-distro package installs the API, SDK, and the opentelemetry-bootstrap and opentelemetry-instrument tools that you’ll use.

Here is the code of our app:

```python title=app.py
import ipaddress
import random
import time
import requests

from random import randint
from flask import Flask, request

app = Flask(__name__)

@app.route("/process-ip")
def process_ip():
    body = fetch()
    ip = parse(body)
    display(ip)
    return ip

def fetch():
    resp = requests.get('https://httpbin.org/ip')
    body = resp.json()
    return body

def parse(body):
    # Sleep for a random amount of time to make the span more visible.
    secs = random.randint(1, 100) / 1000
    time.sleep(secs)

    return body["origin"]

def display(ip):
    # Sleep for a random amount of time to make the span more visible.
    secs = random.randint(1, 100) / 1000
    time.sleep(secs)

    message = f"Your IP address is `{ip}`."
    print(message)

if __name__ == "__main__":
    app.run(port=5000)
```

## Auto-instrumentation

OpenTelemetry provides a tool called `opentelemetry-bootstrap` that automatically instruments your Python application.

```bash
opentelemetry-bootstrap -a install
```

Now we are ready to run the app (we don't need metrics so we disable them):

```bash
OTEL_METRICS_EXPORTER=none opentelemetry-instrument \
    --traces_exporter console \
    python app.py
```

By hitting [http://localhost:5000/process-ip](http://localhost:5000/process-ip) you should see the corresponding trace in the console.

## Manual instrumentation

For the sake of the tutorial, we will now add a manual intrumentation by creating manually spans.

```python title=intrumented_app.py
import ipaddress
import random
import time
import requests

from random import randint
from flask import Flask, request

from opentelemetry import trace

app = Flask(__name__)

@app.route("/process-ip")
@tracer.start_as_current_span("process_ip")
def process_ip():
    body = fetch()
    ip = parse(body)
    display(ip)
    return ip

@tracer.start_as_current_span("fetch")
def fetch():
    resp = requests.get('https://httpbin.org/ip')
    body = resp.json()

    headers = resp.headers
    current_span = trace.get_current_span()
    current_span.set_attribute("status_code", resp.status_code)
    current_span.set_attribute("content_type", headers["Content-Type"])
    current_span.set_attribute("content_length", headers["Content-Length"])

    return body

@tracer.start_as_current_span("parse")
def parse(body):
    # Sleep for a random amount of time to make the span more visible.
    secs = random.randint(1, 100) / 1000
    time.sleep(secs)

    return body["origin"]

@tracer.start_as_current_span("display")
def display(ip):
    # Sleep for a random amount of time to make the span more visible.
    secs = random.randint(1, 100) / 1000
    time.sleep(secs)

    message = f"Your IP address is `{ip}`."
    print(message)

    current_span = trace.get_current_span()
    current_span.add_event(message)

if __name__ == "__main__":
    app.run(port=5000)
```

Start the instrumented app:

```bash
OTEL_METRICS_EXPORTER=none opentelemetry-instrument \
    --traces_exporter console \
    python instrumented_app.py
```

If you hit again [http://localhost:5000/process-ip](http://localhost:5000/process-ip), you should see new spans with name `fetch`, `parse`, and `display` and with the corresponding custom attributes.


## Sending traces directly to Quickwit

To send traces directly to Quickwit, we need to use the OTLP exporter. We will also add a service name to identify the service that sends the traces in Quickwit and Jaeger UI.

```bash
OTEL_METRICS_EXPORTER=none \ # We don't need metrics
OTEL_SERVICE_NAME=flask \
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:7281 \
opentelemetry-instrument python instrumented_app.py
```

Now, if you hit [http://localhost:5000/process-ip](http://localhost:5000/process-ip), traces will be send to Quickwit, you just need to wait around 30 seconds before they are indexed. It's time for a coffee break!

30 seconds has passed, let's query the traces from our service:

```bash
curl -XPOST http://localhost:7280/api/v1/otel-trace-v0/search -H 'Content-Type: application/json' -d '{
    "query": "resource_attributes.service.name:flask"
}'
```

You can now open Jaeger UI [localhost:16686](http://localhost:16686/) and play with, you have now a Quickwit working backend.

![Flask trace analysis in Jaeger UI](../assets/images/jaeger-ui-python-app-trace-analysis.png)

![Flask traces in Jaeger UI](../assets/images/jaeger-ui-python-app-traces.png)

## Sending traces to an OpenTelemetry collector

Coming soon.

## Further improvements

You will soon be able to do aggregations on dynamic fields (planned for 0.6).
