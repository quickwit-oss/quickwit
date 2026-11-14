---
title: Multi-node cluster with Docker Compose
description: Start a three-node Quickwit cluster locally with Docker Compose, PostgreSQL, and an S3-compatible store.
tags: [docker, cluster, self-hosted]
sidebar_position: 7
---

In this guide, we will start a three-node Quickwit cluster with Docker Compose. The cluster uses:

- **PostgreSQL** for the metastore,
- **LocalStack** as a local S3-compatible store for splits,
- three Quickwit nodes: `quickwit-1` runs every service, and `quickwit-2` and `quickwit-3` run only the searcher.

The same recipe works with a real PostgreSQL instance and a real S3 or MinIO bucket; LocalStack is only here to keep the tutorial self-contained.

## Prerequisites

You need [Docker](https://docs.docker.com/engine/install/) with the Compose plugin.

## Docker Compose file

Save the following configuration as `docker-compose.yaml`:

```yaml title="docker-compose.yaml"
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: quickwit
      POSTGRES_PASSWORD: quickwit
      POSTGRES_DB: quickwit
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U quickwit"]
      interval: 2s
      timeout: 5s
      retries: 30

  localstack:
    image: localstack/localstack:3.5.0
    environment:
      SERVICES: s3
      AWS_DEFAULT_REGION: us-east-1
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:4566/_localstack/health"]
      interval: 2s
      timeout: 5s
      retries: 30

  createbucket:
    image: amazon/aws-cli:latest
    depends_on:
      localstack:
        condition: service_healthy
    environment:
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      AWS_DEFAULT_REGION: us-east-1
    entrypoint: /bin/sh -c "aws --endpoint-url=http://localstack:4566 s3 mb s3://quickwit"

  quickwit-1:
    image: quickwit/quickwit:0.9.0
    command: run
    depends_on:
      postgres:
        condition: service_healthy
      createbucket:
        condition: service_completed_successfully
    environment: &quickwit-env
      QW_NODE_ID: quickwit-1
      QW_LISTEN_ADDRESS: 0.0.0.0
      QW_ENABLED_SERVICES: metastore,control_plane,indexer,janitor,searcher
      QW_PEER_SEEDS: quickwit-1:7280,quickwit-2:7280,quickwit-3:7280
      QW_METASTORE_URI: postgres://quickwit:quickwit@postgres:5432/quickwit
      QW_DEFAULT_INDEX_ROOT_URI: s3://quickwit/indexes
      QW_S3_ENDPOINT: http://localstack:4566
      QW_S3_FORCE_PATH_STYLE_ACCESS: "true"
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      AWS_REGION: us-east-1
    ports:
      - "7280:7280"

  quickwit-2:
    image: quickwit/quickwit:0.9.0
    command: run
    depends_on:
      - quickwit-1
    environment:
      <<: *quickwit-env
      QW_NODE_ID: quickwit-2
      QW_ENABLED_SERVICES: searcher
    ports:
      - "7281:7280"

  quickwit-3:
    image: quickwit/quickwit:0.9.0
    command: run
    depends_on:
      - quickwit-1
    environment:
      <<: *quickwit-env
      QW_NODE_ID: quickwit-3
      QW_ENABLED_SERVICES: searcher
    ports:
      - "7282:7280"
```

`QW_PEER_SEEDS` lists the REST addresses of all three nodes so they can discover each other. `QW_LISTEN_ADDRESS` must be `0.0.0.0` in cluster mode; otherwise nodes only accept traffic on localhost and never join the cluster.

## Start the cluster

```bash
docker compose up -d
```

Wait a few seconds, then check the cluster:

```bash
curl -s "http://localhost:7280/api/v1/cluster?format=pretty_json"
```

The response should list `quickwit-1`, `quickwit-2`, and `quickwit-3` in `ready_nodes`. Each node also serves the REST API and the UI on ports 7280, 7281, and 7282 respectively.

## Create an index and add documents

Save this index configuration as `demo-index.yaml`:

```yaml title="demo-index.yaml"
version: 0.8
index_id: demo
doc_mapping:
  field_mappings:
    - name: message
      type: text
      tokenizer: default
    - name: timestamp
      type: datetime
      input_formats:
        - unix_timestamp
      output_format: unix_timestamp_secs
      fast: true
  timestamp_field: timestamp
search_settings:
  default_search_fields: [message]
```

Create the index:

```bash
curl -XPOST http://localhost:7280/api/v1/indexes \
  -H 'content-type: application/yaml' \
  --data-binary @demo-index.yaml
```

Index a few documents:

```bash
cat > demo-docs.json <<'EOF'
{"message":"hello quickwit cluster","timestamp":1700000000}
{"message":"distributed search is working","timestamp":1700000001}
{"message":"third document","timestamp":1700000002}
EOF

curl -XPOST "http://localhost:7280/api/v1/demo/ingest?commit=force" \
  --data-binary @demo-docs.json
```

## Search from any node

The index metadata lives in PostgreSQL and the splits live in the S3 bucket, so every searcher can serve the query. Query any node:

```bash
curl -s "http://localhost:7280/api/v1/demo/search?query=distributed"
curl -s "http://localhost:7281/api/v1/demo/search?query=distributed"
curl -s "http://localhost:7282/api/v1/demo/search?query=distributed"
```

All three requests return the same hit.

## Clean up

Stop the cluster and remove the PostgreSQL and LocalStack volumes:

```bash
docker compose down -v
```

## Going further

- Replace LocalStack with a real S3 bucket or MinIO by changing `QW_S3_ENDPOINT` and the AWS credentials.
- Replace the tutorial PostgreSQL credentials with your own; see the [metastore configuration](../../configuration/metastore-config) for the accepted URI formats.
- To run different services on different nodes, change `QW_ENABLED_SERVICES`. See [deployment modes](../../deployment/deployment-modes) for the available services and the cluster topology they enable.
