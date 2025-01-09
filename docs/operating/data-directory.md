---
title: Data directory
sidebar_position: 1
---

Quickwit operates on a local directory to store temporary files. By default, this working directory is named `qwdata` and placed right next to the Quickwit binary.

Let's have a look at how Quickwit organizes the data directory.

## Data directory layout

When operating Quickwit, you will end up with the following tree:

```bash
qwdata
├── cache
│   └── splits
|       ├── 03BSWV41QNGY5MZV9JYAE1DCGA.split
│       └── 01GSWV41QNGY5MZV9JYAE1DCB7.split
├── delete_task_service
│   └── wikipedia%01H13SVKDS03P%TpCfrA
├── indexing
│   ├── wikipedia%01H13SVKDS03P%_ingest-api-source%RbaOAI
│   └── wikipedia%01H13SVKDS03P%kafka-source%cNqQtI
├── wal
│   ├── wal-00000000000000000056
│   └── wal-00000000000000000057
└── queues
    ├── partition_id
    ├── wal-00000000000000000028
    └── wal-00000000000000000029
```

### `/queues` and `/wal` directories
 
These directories are created only if the ingest API service is running on your node. They contains write ahead log files of the ingest API to guard against data loss. The `/queues` directory is used by the legacy version of the ingest (sometimes referred to as ingest V1). It is meant to be phased out in upcoming versions of Quickwit. Learn more about ingest API versions [here](../ingest-data/ingest-api.md#ingest-api-versions).

The log file is truncated when Quickwit commits a split (piece of index), which means that the split is stored on the storage and its metadata are in the metastore.

You can configure `max_queue_memory_usage` and `max_queue_disk_usage` in the [node config file](../configuration/node-config.md#ingest-api-configuration) to limit the max disk usage.

### `/indexing` directory

This directory holds the local indexing directory of each indexing source of each index managed by Quickwit. In the above tree, you can see two directories corresponding to the `wikipedia` index, which means that index is currently handling two sources.


### `/delete_task_service` directory

This directory is used by the Janitor service to apply deletes on indexes. During this process, splits are downloaded, a new split is created while applying deletes and uploaded to the target storage. This directory gets created only on nodes running the Janitor service.

### `/cache` directory

This directory is used for caching splits that will undergo a merge operation to save disk IOPS. Splits are evicted if they are older than two days. If cache limits are reached, oldest splits are evicted.

You can [configure](../configuration/node-config#indexer-configuration) the number of splits the cache can hold with `split_store_max_num_splits` and limit the overall size in bytes of splits with `split_store_max_num_bytes`.


## Setting the right splits cache limits

Caching splits saves disk IOPS when Quickwit needs to merge splits.

Setting the right limits will depend on your [merge policy](../configuration/index-config.md#merge-policies) and the number of partitions you are using. The default splits cache limits should fit most use cases.

### Splits cache with the default configuration

For a given index, Quickwit commits one split every minute and uses the "Stable log" [merge policy](../configuration/index-config.md#merge-policies). This merge policy by default merges splits by group of 10, 11, or 12 until splits have more than 10 millions of documents. A split will typically undergo 3 or 4 merges and after will be considered as mature and evicted from the cache.

The following table shows how many splits will be created after a given amount of time assuming a 20MB/s ingestion rate with a compression ratio of 0.5:

| Time (minutes) | Number of splits                       | Splits size (GB) |
| -------------- | -------------------------------------- | ----------- |
| 1              | 1                                      | 0.6 GB      |
| 2              | 2                                      | 1.2 GB      |
| 10             | 10                                     | 6 GB        |
| 11             | 1 + 1 (merged once)                    | 6.6 GB      |
| 21             | 1 + 2 (merged once)                    | 12.6 GB     |
| 91             | 1 + 9 (merged once)                    | 54.6 GB     |
| 101            | 1 + 1 (merged twice)                   | 60.6 GB     |
| 111            | 2 + 1 (merged once) + 1 (merged twice) | 66.6 GB     |
| 201            | 1 + 0 (merged once) + 2 (merged twice) | 120.6 GB    |
| ..             | ...                                    |             |

In this case, the default cache limits of 1000 splits and 100GB are good enough to avoid downloading splits from the storage for the first two merges. This is perfectly fine for a production use case. You may want to increase the splits cache size to avoid any split download.

You can monitor the download rate with our [indexers dashboard](monitoring.md).

### Splits cache with partitioning

When using [partitions](../overview/concepts/querying.md#partitioning), Quickwit will create one split per partition and the number of splits can add up very quickly.

Let's take a concrete example with the following assumptions:
- A [commit timeout](../configuration/index-config.md#indexing-settings) of 1 minute.
- A partitioning that has 100 partitions. Quickwit will create 100 splits per minute assuming a document of each partition is ingested in one minute.
- A merge policy that merges splits of same partition as soon as there is 10 splits.

The following table shows how many splits will be created after a given amount of time:

| Time (minutes) | Number of splits |
| ------------ | ---------------- |
| 1            | 100              |
| 2            | 200              |
| 10           | 1000             |
| 11           | 100 + 100 (merged once) |
| 21           | 100 + 200 (merged once) |
| 91           | 100 + 900 (merged once) |
| 100          | 1000 + 900 (merged once) |
| 101          | 100 + 0 (merge once) + 100 (merged twice) |
| 200          | 1000 + 900 (merged once) + 100 (merged twice) |
| 201          | 100 + 0 (merged once) + 200 (merged twice) |

With these assumptions, you have to set `split_store_max_num_splits` to at least 1000 to avoid downloading splits from the storage for the first merge operation. And as merging can take a bit of time, you should set `split_store_max_num_splits` to a value that can hold all the splits that are not yet merged plus the incoming splits, a value of 1100 splits should be enough. If you want to store split until the second merge, a limit of 2500 splits should be good enough.

## Troubleshooting with a huge number of local splits

When starting, Quickwit is scanning all the splits in the cache directory to know which split is present locally, this can take a few minutes if you have tens of thousands splits. On Kubernetes, as your pod can be restarted if it takes too long to start, you may want to clean up the data directory or increase the liveliness probe timeout.
Also please report such a behavior on [GitHub](https://github.com/quickwit-oss/quickwit) as we can certainly optimize this start phase.
