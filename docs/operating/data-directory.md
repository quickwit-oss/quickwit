# Quickwit data directory

Quickwit operates on a local directory to store temporary files. By default, this working directory is named `qwdata` and placed right next to the Quickwit binary. The following is a tree of how that directory might look like after some time of operating Quickwit.

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

- `cache`: This directory is used for caching splits created on this node. This avoids downloading young splits that will go through merges. This cache can be [configured](https://quickwit.io/docs/main-branch/configuration/node-config#indexer-configuration) to limit on the number of splits it can hold (`split_store_max_num_splits`) and the overall size in bytes of splits it can hold (`split_store_max_num_bytes`). In this cache, splits are evicted based on age from oldest when space is needed. Also, because the splits life cycle is well known within [merges](https://quickwit.io/docs/main-branch/concepts/indexing/#merge-process-and-merge-policy), splits older than two days are always evicted and matured splits are not even stored in the cache.

## Keeping disk space in check

 Merge operation in Quickwit happens on multiple splits. When the targeted splits for merge are not locally available, they need to be downloaded from the storage which can take a while. To save on bandwidth and IO operations, Quickwit tries to cache newly created splits and keeps them till they have gone through a merge operation. Quickwit also maintains data ingestion queues that get filled and truncated as data get ingested via the Ingest API. This means one has to take into account how much data (queues data files, new split files) can be kept on disk at a time without running out of space. Some good rules of thumb to follow are:

 -  
