---
title: Storage URI
position: 6
---

In Quickwit, Storage URIs refer to different kinds of storage.

Generally speaking, you can use a storage URI or a regular file path wherever you would have expected a file path.

 
For instance

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
- check for the configuration in the  `~/.aws/credentials` filepath.
- check for the [Amazon ECS environment](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html)
- check the [EC2 instance metadata API](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html)

### Region

The region will be detected using the first successful method in this list (order matters)

- `AWS_DEFAULT_REGION` environment variable
- `AWS_REGION` environment variable
- Amazon’s instance metadata API [https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html)

:::caution
Custom endpoints are not supported yet.

:::
