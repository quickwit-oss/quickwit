---
title: Data directory
sidebar_position: 2
---

Quickwit operates on a local directory to store temporary files. By default, this working directory is named `qwdata` and placed right next to the Quickwit binary. When operating Quickwit, you will end up with the following tree:

```bash
qwdata
├── cache
│   └── splits
|       ├── 03BSWV41QNGY5MZV9JYAE1DCGA.split
│       └── 01GSWV41QNGY5MZV9JYAE1DCB7.split
├── delete_task_service
│   └── wikipedia
│       └── scratch
├── indexing
│   └── wikipedia
│       └── _ingest-api-source
│           └── scratch
└── queues
    ├── partition_id
    ├── wal-00000000000000000028
    └── wal-00000000000000000029
```

- `/queues`: This directory contains write ahead log files of the ingest API. When data is sent to Quickwit via the ingest API, the data is stored in these queues to guard against data lost. The indexing pipelines then consume those queues at their own pace. Whenever an indexing pipeline commits based on `commit_timeout_secs`, the queue is truncated to free up the unnecessarily occupied storage. This directory is created only if the ingest API service is running on your node.

- `indexing`: This directory hold the local indexing directory of each index managed by Quickwit. The local indexing directories are further separated by indexing source. In the above tree, you can see there is an indexing folder corresponding to `_ingest-api-source` source for `wikipedia` index. This directory gets created only on nodes running the indexing service.

- `delete_task_service`: This directory is used by the Janitor service to apply deletes on indexes. During this process, splits are downloaded, a new split is created while applying deletes and uploaded to the target storage. This directory gets created only on nodes running the Janitor service.

- `cache`: This directory is used for caching splits created on this node. This avoids downloading young splits that will go through merges. This cache can be [configured](https://quickwit.io/docs/main-branch/configuration/node-config#indexer-configuration) to limit on the number of splits it can hold (`split_store_max_num_splits`) and the overall size in bytes of splits it can hold (`split_store_max_num_bytes`). In this cache, splits are evicted based on age from oldest when space is needed. Also, splits older than two days and matured splits are evicted from the cache.

## Keeping disk space in check

 Merge operation in Quickwit happens on multiple splits. When the targeted splits for merge are not locally available, they need to be downloaded from the storage which can take a while. To save on bandwidth and IO operations, Quickwit tries to cache newly created splits and keeps them till they are mature if possible (old splits are evicated if `split_store_max_num_bytes` or `split_store_max_num_splits` limits are reached).
 
 Quickwit also maintains data ingestion queues that get filled and truncated as data get ingested via the Ingest API. This means one has to take into account how much data (queues data files, new split files) can be kept on disk at a time without running out of space. Some good rules of thumb to follow are:

 ### Splits cache with partitioning

When using [partitions](../overview/concepts/querying.md#partitioning), Quickwit will create one split per partitition and the number of splits can add up very quickly.

Let's take a concrete example with the following assumptions:
- A commit timeout of 1 minute (Quickwit will commit splits every minute)
- A partitioning strategy that creates 100 partitions per minute. Quickwit will then create 100 splits per minute.
- A merge policy that merges splits of same partition as soons as there is 10 splits.

Quickwit will create 100 splits per minute and will merge them every 10 minutes. In order to avoid downloading splits from the storage, it's important to keep the splits cache big enough to hold all the splits that are not yet merged. The following table shows how many splits will be created after a given amount of time:

| Time (minutes) | Number of splits |
| ------------ | ---------------- |
| 1            | 100              |
| 2            | 200              |
| 3            | 300              |
| 4            | 400              |
| 5            | 500              |
| 6            | 600              |
| 7            | 700              |
| 8            | 800              |
| 9            | 900              |
| 10           | 1000             |
| 11           | 100 + 100 (merged) |
| 11           | 200 + 100 (merged) |
| 12           | 300 + 100 (merged) |
| ...          | ... + ... (merged) |

With these assumptions, you have to set `split_store_max_num_splits` to at least 1000 to avoid downloading splits from the storage. And as merging can take a bit of time, you should set `split_store_max_num_splits` to a value that can hold all the splits that are not yet merged plus the incoming splits, a value of 1200 splits should be enough. Don't forget to setup `split_store_max_num_bytes` accordingly.

Keeping the splits of first generation (underwent no merge) in cache is very important to avoid write amplification.

You may also want to keep splits in cache for next generations to reduce write amplification and reduce bandwitdh if you have enough disk space.

## Troubleshooting with a huge number of local splits

When starting, Quickwit is scanning all the splits in the cache directory to know which split is present locally, this can take a few minutes if you have tens/hundreds of thousands splits. This can be an issue on Kubernetes as your pod can be restarted if it takes too long to start. You may want to clean up the data directory or increase the liveliness probe timeout. And don't hesitate to report such a behavior on [GitHub](https://github.com/quickwit-oss/quickwit).
