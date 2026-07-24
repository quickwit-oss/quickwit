---
title: Storage configuration
sidebar_position: 2
---

## Supported Storage Providers

Quickwit currently supports five types of storage providers:
- Amazon S3 and S3-compatible (Garage, MinIO, ...)
- Azure Blob Storage
- Local file storage*
- Google Cloud Storage (native API)
- IPFS (InterPlanetary File System, via the RPC API of a node such as [Kubo](https://github.com/ipfs/kubo))

## Storage URIs

Storage URIs refer to different storage providers identified by a URI "protocol" or "scheme". Quickwit supports the following storage URI protocols:
- `s3://` for Amazon S3 and S3-compatible
- `azure://` for Azure Blob Storage
- `file://` for local file systems
- `gs://` for Google Cloud Storage
- `ipfs://` for IPFS (content-addressed storage)

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

### IPFS storage configuration

Quickwit stores splits in the MFS (Mutable File System) of an IPFS node through its RPC API. MFS presents content-addressed, chunked DAGs as regular files supporting ranged reads, which matches the byte-range access pattern Quickwit uses to open splits. Every split written this way has a CID and is addressable, pinnable, and exportable (e.g. as a CAR file) from the wider IPFS network.

An `ipfs://` URI maps to an MFS directory on the configured node: `ipfs://quickwit-indexes/my-index` reads and writes under the MFS path `/quickwit-indexes/my-index`.

| Property | Description | Default value |
| --- | --- | --- |
| `api_endpoint` | Base URL of the IPFS node RPC API. | `http://127.0.0.1:5001` |
| `chunker` | Chunker applied when writing splits. Larger fixed-size chunks reduce the number of blocks a ranged read spans. | `size-1048576` |
| `raw_leaves` | Use raw leaf blocks (no UnixFS envelope around data blocks). | `true` |
| `request_timeout_secs` | Request timeout in seconds for individual RPC calls. | `30` |

#### Environment variables

| Env variable | Description |
| --- | --- |
| `QW_IPFS_API_ENDPOINT` | Base URL of the IPFS node RPC API. |

Example of a storage configuration for IPFS in YAML format:

```yaml
storage:
  ipfs:
    api_endpoint: http://127.0.0.1:5001
```

:::note
The IPFS backend covers index data (splits). Keep the metastore on PostgreSQL or a file-backed URI: metastore state is small, mutable, and frequently rewritten, which is a poor fit for content addressing.
:::

:::note
Deleting a split unlinks it from MFS but only reclaims disk space once the IPFS node runs garbage collection. Kubo does not GC by default: run the daemon with `--enable-gc` or schedule `ipfs repo gc`, especially on indexes with frequent merges.
:::

:::caution
The IPFS storage backend requires a Quickwit binary compiled with the `ipfs` feature enabled (`--features quickwit-storage/ipfs`). Release binaries built from the default release feature set include it.
:::

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
