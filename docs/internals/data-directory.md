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

- `delete_task_service`: This directory is used by the Janitor service to apply deletes on indexes. During this process, splits are downloaded, a new split is created while applying deletes and uploaded to the target storage. This directory gets created only on node running Janitor service.

- `cache`: This directory is used for caching splits created on this node. As we want to reduce time-to-search, we often creates splits that are far from mature in size and number of document. This leads to new splits going through a set of merges before getting mature. In order to avoid downloading a split that already existed on disk, we keep hold of newly created splits in cache as much as possible. This cache limits on number of splits as well as the overall size of splits it holds. Splits are evicted based on age only (oldest gets evicted).

## Keeping disk space in check

 Merge operation in Quickwit happens on multiple splits. When the targeted splits for merge are not locally available, they need to be downloaded from the storage which can take a while. To save on bandwidth and IO operations, Quickwit tries to cache newly created splits and keeps them till they have gone through a merge operation. Quickwit also maintains data ingestion queues that get filled and truncated as data get ingested via the Ingest API. This means one has to take into account how much data (queues data files, new split files) can be kept on disk at a time without running out of space. The following need to be considered:

- Indexing `commit_timeout_secs`: This is the rate at which new splits are created assuming permanent data ingestion.
- Merging `merge_factor` and `max_merge_factor`: These are respectively the minimum and maximum number of splits from the same level that can be merged together at a time.

By default, the `commit_timeout_secs` is 1 minutes. The default merge policy (`stable_log`) has a `merge_factor` of `10` and a `max_merge_factor` of `12`: That is, splits are created every minute and merge happens every 10 minutes when at least 10 new splits of the level 0 are accumulated. 100 minutes for a merge to trigger on level 1 and 1000 minutes for a merge to trigger on level 2 and so on. Assuming our splits get mature at level 3, we need as much disk space to hold N number of splits per level where `merge_factor <= N <= max_merge_factor` . The overall number of splits allowed in the cache would be 3 (maximum level) * N.

When continuously pushing large amount of data on ingest API, we also need to make sure queues are truncated more often than they are filled with data by adjusting the `commit_timeout_secs`. Adjusting `commit_timeout_secs` requires more though since:
- It reduces time-to-search when decreased at the expense of creating splits that are very far from being mature, thus need to go through lot of merge cycle (io operations).
- It create splits that are close to being mature when increased at the expense of increasing the time-to-search.
