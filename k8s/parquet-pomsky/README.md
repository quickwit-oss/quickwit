# Parquet-Pomsky Deployment

Parquet-Pomsky is a separate Quickwit deployment in the `quickhouse-incubator` namespace for parquet ingestion in `gizmo.us1.staging.dog`, isolated from the main pomsky deployment (for now).

We have a separate orgstore cluster for the metastore, `orgstore-parquet-cloudprem-metastore`. We're using a separate S3 bucket as well, `parquet-pomsky-staging`.

## Build and Push Image

Build and push the pomsky image with the `parquet` tag:

```bash
CI_JOB_TOKEN=$(ddtool auth gitlab token) ./scripts/image-tool.sh --push \
  --tag parquet
```

## Deploy to Staging

```bash
bzl run //k8s/parquet-pomsky:staging --define image_digest=$(crane digest registry.ddbuild.io/pomsky:parquet)
```
