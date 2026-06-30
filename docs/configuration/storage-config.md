---
title: Storage configuration
sidebar_position: 2
---

## Supported Storage Providers

Quickwit currently supports four types of storage providers:
- Amazon S3 and S3-compatible (Garage, MinIO, ...)
- Azure Blob Storage
- Local file storage*
- Google Cloud Storage (native API)

## Storage URIs

Storage URIs refer to different storage providers identified by a URI "protocol" or "scheme". Quickwit supports the following storage URI protocols:
- `s3://` for Amazon S3 and S3-compatible (per-bucket overrides via `storage.s3.profiles`, see [Per-bucket S3 profiles](#per-bucket-s3-profiles))
- `azure://` for Azure Blob Storage
- `file://` for local file systems
- `gs://` for Google Cloud Storage

In general, you can use a storage URI or a file path anywhere you would intuitively expect a file path. For instance:
- when setting the `index_uri` of an index to specify the storage provider and location;
- when setting the `metastore_uri` in a node config to set up a file-backed metastore;
- when passing a file path as a command line argument.

### Local file storage URIs

Quickwit interprets regular file paths as local file system URIs. Relative file paths are allowed and are resolved relatively to the current working directory (CWD). `~` can be used as a shortcut to refer to the user’s home directory. The following are valid local file system URIs:

```markdown
- /var/quickwit
- file:///var/quickwit
- /home/quickwit/data
- ~/data
- ./quickwit
```

:::caution
When using the `file://` protocol, a third `/` is necessary to express an absolute path. For instance, the following URI `file://home/quickwit/` is interpreted as `./home/quickwit`
:::

## Storage configuration

This section contains one configuration subsection per storage provider. If a storage configuration parameter is not explicitly set, Quickwit relies on the default values provided by the storage provider SDKs ([Azure SDK for Rust](https://github.com/Azure/azure-sdk-for-rust), [AWS SDK for Rust](https://github.com/awslabs/aws-sdk-rust)).

### S3 storage configuration

| Property | Description | Default value |
| --- | --- | --- |
| `flavor` | The optional storage flavor to use. Available flavors are `digital_ocean`, `garage`, `gcs`, and `minio`. | |
| `access_key_id` | The AWS access key ID. | |
| `secret_access_key` | The AWS secret access key. | |
| `region` | The AWS region to send requests to. | `us-east-1` (SDK default) |
| `endpoint` | Custom endpoint for use with S3-compatible providers. | SDK default |
| `force_path_style_access` | Disables [virtual-hosted–style](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html) requests. Required by some S3-compatible providers (Ceph, MinIO). | `false` |
| `disable_multi_object_delete` | Disables [Multi-Object Delete](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html) requests. Required by some S3-compatible providers (GCS). | `false` |
| `disable_multipart_upload` | Disables [multipart upload](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html) of objects. Required by some S3-compatible providers (GCS). | `false` |
| `checksum_algorithm` | Upload [checksum](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html) algorithm. Allowed values: `crc32c` (computed and validated by the AWS SDK), `md5` (sent client-side via `Content-MD5`; useful for S3-compatible providers that predate `x-amz-checksum-*`), or `disabled`.  | `crc32c` |
| `disable_checksums` | **Deprecated.** Previously a boolean that disabled all request/response checksums. Equivalent to setting `checksum_algorithm: disabled`. | `false` |

:::warning
Hardcoding credentials into configuration files is not secure and strongly discouraged. Prefer the alternative authentication methods that your storage backend may provide.
:::

#### Environment variables

| Env variable | Description |
| --- | --- |
| `QW_S3_ENDPOINT` | Custom S3 endpoint. |
| `QW_S3_MAX_CONCURRENCY` | Limit the number of concurrent requests to S3 |

#### Storage flavors

Storage flavors ensure that Quickwit works correctly with storage providers that deviate from the S3 API by automatically configuring the appropriate settings. The available flavors are:
- `digital_ocean`
- `garage`
- `gcs`
- `minio`

*Digital Ocean*

The Digital Ocean flavor (`digital_ocean`) forces path-style access and turns off multi-object delete requests.

*Garage flavor*

The Garage flavor (`garage`) overrides the `region` parameter to `garage` and forces path-style access.

*Google Cloud Storage*

The Google Cloud Storage flavor (`gcs`) turns off multi-object delete requests, multipart uploads, and disables checksums.

*MinIO flavor*

The MinIO flavor (`minio`) overrides the `region` parameter to `minio` and forces path-style access.

Example of a storage configuration for Google Cloud Storage in YAML format:

```yaml
storage:
  s3:
    flavor: gcs
    region: us-east1
    endpoint: https://storage.googleapis.com
```

#### Per-bucket S3 profiles

In addition to the primary `s3:` block, you can declare per-bucket overrides under `storage.s3.profiles`. The map key is the bucket name; when an `s3://<bucket>/...` URI is resolved, an exact match supplies that bucket's own endpoint, credentials, region, and flags. Any bucket not listed falls back to the fields on the primary `s3:` block. URIs stay canonical `s3://` — routing is by bucket name, so nothing extra is persisted in the index metadata.

Each profile accepts the same fields as the primary `s3:` block, *except* `profiles` itself (no recursion). If `access_key_id` / `secret_access_key` are omitted on a profile, the global AWS SDK credential chain is used (env vars, instance metadata, etc.).

Profiles are self-contained: the process-wide `QW_S3_ENDPOINT` and `QW_S3_FORCE_PATH_STYLE_ACCESS` overrides apply to the primary `s3:` backend only. A profile always uses its own `endpoint` and `force_path_style_access` values.

> Because routing keys on bucket name, a given bucket name maps to exactly one backend. If you need the *same* bucket name on two different endpoints, give the buckets distinct names.

```yaml
storage:
  s3:
    # Primary backend — used for any bucket not listed under `profiles`.
    endpoint: https://s3.us-east-1.amazonaws.com
    region: us-east-1
    profiles:
      # Buckets named `logs-bucket-eu` resolve to this endpoint.
      logs-bucket-eu:
        endpoint: https://s3.eu-west-3.amazonaws.com
        region: eu-west-3
        access_key_id: ${SECONDARY_S3_ACCESS_KEY_ID}
        secret_access_key: ${SECONDARY_S3_SECRET_ACCESS_KEY}
      # Buckets named `seaweed-logs` resolve here. Falls back to the global AWS
      # SDK credentials when keys are omitted.
      seaweed-logs:
        endpoint: http://seaweedfs-s3:8333
        region: us-east-1
        force_path_style_access: true
```

An index simply points at the bucket by its canonical URI; the matching profile is applied automatically:

```yaml
index_id: logs-eu
index_uri: s3://logs-bucket-eu/logs-eu
```

### Azure storage configuration

| Property | Description | Default value |
| --- | --- | --- |
| `account` | The Azure storage account name. | |
| `access_key` | The Azure storage account access key. | |

#### Environment variables

| Env variable | Description |
| --- | --- |
| `QW_AZURE_STORAGE_ACCOUNT` | Azure Blob Storage account name. |
| `QW_AZURE_STORAGE_ACCESS_KEY` | Azure Blob Storage account access key. |

Example of a storage configuration for Azure in YAML format:

```yaml
storage:
  azure:
    account: your-azure-account-name
    access_key: your-azure-access-key
```

## Storage configuration examples for various object storage providers

### Garage

[Garage](https://garagehq.deuxfleurs.fr/) is an open-source distributed object storage service tailored for self-hosting.

```yaml
storage:
  s3:
    flavor: garage
    endpoint: http://127.0.0.1:3900
```

### MinIO

[MinIO](https://min.io/) is a high-performance object storage.

```yaml
storage:
  s3:
    flavor: minio
    endpoint: http://127.0.0.1:9000
```

Note: `default_index_root_uri` or index URIs do not include the endpoint, you should set it as a typical S3 path such as `s3://indexes`.
