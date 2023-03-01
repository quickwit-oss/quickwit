# Quickwit data directory

Quickwit operates on a local directory to store intermediates files. By default this working directory is named `qwdata` and placed right next to the quickwit binary. The following is an tree of how that directory might look like after some time of operating quickwit.


```bash
./qwdata
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

- `/queues`: This directory contains write ahead log files allowing hight performance data ingestion on the ingest API. When data is sent to Quickwit via the ingest API, the data is stored in these queues to guard against data lost. The indexing pipelines then consume those queues at their own pace. Whenever an indexing pipeline commits based on `commit_timeout_secs`, the queue is truncated to free up the unnecessarily occupied storage. This directory gets created only on node running the ingest API service.

- `indexing`: This directory hold the local indexing directory of each index managed by quickwit. The local indexing directories are further separated by indexing source. In the above tree, you can see there is an indexing folder corresponding to `_ingest-api-source` source for `wikipedia` index. This directory gets created only on node running the indexing service.

- `delete_task_service`: This directory is used by the Janitor service to apply deletes on indexes. During this process, splits are downloaded, a new split is created while applying deletes and uploaded to the target storage. This directory gets created only on node running Janitor service.

- `cache`: This directory is used for caching splits created on this node. As we want to reduce time-to-search, we often creates splits that are far from mature in size and number of document. This leads to new splits going through a set of merges before getting mature. In order to avoid downloading a split that already existed on disk, we keep hold of newly created splits in cache as much as possible. This cache limits on number of splits as well as the overall size of splits it holds. Splits are evicted based on age only from oldest.

## Keeping disk space in check

Quickwit uses cache to save bandwidth and perform less io when merging splits, aside we also have the queues that get filled and truncated based on ingestion speed. This means one has to take into account how much data (queues data files, new split files) can be kept on disk at a time without running out of space. The following need to be considered:

- Indexing `commit_timeout_secs`: This is the rate at which new splits are created assuming permanent data ingestion.
- Merging `merge_factor`: This is the number of splits from the same level that can be merged together.

The default `commit_timeout_secs` is 1 minutes and the default `stable_log` merge policy `merge_factor` is 10: That is, splits are created every minute and merge happens every 10 minutes when 10 new splits of the level 0 are accumulated. 100 minutes to merge on level 1 and 1000 minutes to merge on level 2 and so on. Assuming our splits get mature at level 3, we need as much disk space to hold N number of splits per level where `merge_factor <= N <= max_merge_factor` . The overall number splits allowed in the cache would be 3 * N.

We also need to make sure queues are truncated more often than they are filled with data by adjusting the `commit_timeout_secs` if we are pushing large amount of data on ingest API.
