
---
title: Tutorial - Set up your environment search on AWS S3
sidebar_position: 3
---

# Setup your environment
To let Quickwit store your indexes on AWS S3, you need to define three environment variables:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION` or `AWS_DEFAULT_REGION`. If variables are malformed, it will fallback to us-east-1.

You can also have them defined in a `~/.aws/credentials` and `~/.aws/config` files.


# Common errors
If you put the wrong credentials, you will see this error message with `Unauthorized` in your terminal:
```
Command failed: Another error occured. `Metastore error`. Cause: `StorageError(kind=Unauthorized, source=Failed to fetch object: s3://quickwit-dev/my-hdfs/quickwit.json)`
```

If you put the wrong region, you will see this one:
```
Command failed: Another error occured. `Metastore error`. Cause: `StorageError(kind=InternalError, source=Failed to fetch object: s3://your-bucket/your-index/quickwit.json)`.
```




