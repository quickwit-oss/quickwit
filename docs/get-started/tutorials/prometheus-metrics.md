---
title: Metrics with Grafana and Prometheus
description: A simple tutorial to display Quickwit metrics with Grafana.
icon_url: /img/tutorials/quickwit-logo.png
tags: [grafana, prometheus, integration]
sidebar_position: 2
---

In this tutorial, you will learn how to set up Grafana to display Quickwit metrics using Prometheus. Grafana will visualize the metrics collected from Quickwit, allowing you to monitor its performance effectively.

## Step 1: Create a Docker Compose File

First, create a `docker-compose.yml` file in your project directory. This file will configure and run Quickwit, Prometheus, and Grafana as Docker services.

Here’s the complete Docker Compose configuration:

```yaml
services:
  quickwit:
    image: quickwit/quickwit
    environment:
      QW_ENABLE_OPENTELEMETRY_OTLP_EXPORTER: "true"
      OTEL_EXPORTER_OTLP_ENDPOINT: "http://localhost:7281"
    ports:
      - 7280:7280
    command: ["run"]

  grafana:
    image: grafana/grafana-oss
    container_name: grafana
    ports:
      - "${MAP_HOST_GRAFANA:-127.0.0.1}:3000:3000"
    environment:
      GF_INSTALL_PLUGINS: https://github.com/quickwit-oss/quickwit-datasource/releases/download/v0.4.6/quickwit-quickwit-datasource-0.4.6.zip;quickwit-quickwit-datasource
      GF_AUTH_DISABLE_LOGIN_FORM: "true"
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Admin

  prometheus:
    image: prom/prometheus:latest
    container_name: prometheus
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml  # Ensure prometheus.yml exists in the same directory
    ports:
      - 9090:9090
```

### Explanation of Services

- **Quickwit**: Runs the Quickwit service on port `7280`.
- **Grafana**: Queries and displays data from Prometheus.
- **Prometheus**: Collects metrics from Quickwit using the `/metrics` endpoint.

## Step 2: Configure Prometheus

Prometheus needs a configuration file to define how it scrapes metrics from Quickwit. Create a file named `prometheus.yml` in the same directory as your Docker Compose file with the following content:

```yaml
global:
  scrape_interval: 1s
  scrape_timeout: 1s

scrape_configs:
  - job_name: quickwit
    metrics_path: /metrics
    static_configs:
      - targets:
          - quickwit:7280
```

## Step 3: Start the Services

Run the following command in your terminal to start all services defined in the Docker Compose file:

```bash
docker compose up
```

This will launch Quickwit, Prometheus, and Grafana services.

## Step 4: Configure Grafana to Use Prometheus

1. Open Grafana in your browser at `http://localhost:3000`.
2. Navigate to **Configuration** > **Data Sources**.
3. Click **Add Data Source**, select **Prometheus**, and set the URL to `http://prometheus:9090`.
4. Click **Save & Test** to verify the connection.

## Step 5: Create or Use Pre-Configured Dashboards

Now that Grafana is set up with Prometheus as a data source, you can create custom dashboards or use Quickwit's pre-configured dashboards:

1. Go to the **Dashboards** section in Grafana.
2. Import or create a new dashboard to visualize metrics.
3. Alternatively, use one of Quickwit’s [pre-configured dashboards](../../operating/monitoring).

