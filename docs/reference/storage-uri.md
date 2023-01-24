---
title: Storage URI
sidebar_position: 5
---

In Quickwit, Storage URIs refer to different kinds of storage.

Generally speaking, you can use a storage URI or a regular file path wherever you would have expected a file path.

For instance:

- when configuring the index storage. (Passed as the `index_uri` in the index command line.)
- when configuring a file-backed metastore. (`metastore_uri` in the QuickwitConfig).
- when passing a config file in the command line. (you can store your `quickwit.yaml` on Amazon S3 if you want)

Right now, only two types of storage are supported.

## Local file system

One can refer to the file system storage by using a file path directly, or a URI with the `file://` protocol. Relative file paths are allowed and are resolved relatively to the current working directory (CWD). `~` can be used as a shortcut to refer to the current user directory.

The following are valid local file system URIs

```markdown
- /var/quickwit
- file:///var/quickwit
- /home/quickwit/data
- ~/data
- ./quickwit
```

:::caution
When using the `file://` protocol, a third `/` is necessary to express an absolute path.

For instance, the following URI `file://home/quickwit/` is interpreted as `./home/quickwit`

:::

## Amazon S3

It is also possible to refer to Amazon S3 using a S3 URI. S3 URIs must have to follow the following format:

```markdown
s3://<bucket name>/<key>
```

For instance

```markdown
s3://quickwit-prod/quickwit-indexes
```

The credentials, as well as the region or the custom endpoint, have to be configured separately, using the methods described below.

### S3 credentials

Quickwit will detect the S3 credentials using the first successful method in this list (order matters)

- check for environment variables (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`)
- check for the configuration in the `~/.aws/credentials` filepath.
- check for the [Amazon ECS environment](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html)
- check the [EC2 instance metadata API](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html)

### Region

The region or custom endpoint will be detected using the first successful method in this list (order matters)

- `AWS_DEFAULT_REGION` environment variable
- `AWS_REGION` environment variable
- Amazon’s instance metadata API [https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html)

## S3-compatible Object Storage

Quickwit can target other S3-compatible storage.
This is done by setting an endpoint url in the `QW_S3_ENDPOINT` environment variable.

Depending on the object storage, you will also need to set the region.

Example:

```bash
export QW_S3_ENDPOINT=http://localhost:9000/
```

### Google Cloud Storage

Example for Google Cloud Storage:

```bash
export QW_S3_ENDPOINT=https://storage.googleapis.com
```

See our [Google Cloud Storage Setup Guide](../guides/storage-setup/gcs-setup.md) for the detailed steps to configure Quickwit with Google Cloud Storage.


### Scaleway object storage

Example:

```bash
export QW_S3_ENDPOINT=https://s3.{your-region}.scw.cloud
export AWS_REGION={your-region}
```

See our [Scaleway Setup Guide](../guides/storage-setup/scaleway-setup.md) for the detailed steps to configure Quickwit with Scaleway object storage.

### Garage

[Garage](https://garagehq.deuxfleurs.fr/) is an Open-Souce lightweight and efficient object storage.

To use it with Quickwit, you will need to setup the region, as mentioned in [Garage documentation](https://garagehq.deuxfleurs.fr/documentation/connect/), it's often set just to `garage`.

Example for a local garage server:

```bash
export QW_S3_ENDPOINT=http://127.0.0.1:3900
export AWS_REGION=garage
```

### MinIO, Ceph and more!

We support other S3-compatible storages and are [welcoming PRs](http://github.com/quickwit-oss/quickwit) to enrich the documentation with new storage backends.

## Azure blob storage

Quickwit supports Azure URIs formatted as `azure://{storage-account}/{container}/{prefix}` where:
- `storage-account` is your Azure storage account name. 
- `container` is the container name (or bucket in S3 parlance).
- `prefix` is optional and can be any prefix.

See our [Azure Setup Guide](../guides/storage-setup/azure-setup.md) for the detailed steps to configure Quickwit with Azure.

