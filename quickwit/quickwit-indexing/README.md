The Indexer serializes and packages splits inline. The MergeExecutor packages merged
splits inline. Serialization I/O metrics use the `indexer` component. Delete-task
pipeline observations report packaging failures under `delete_task_executor`.

Cooperative indexing holds its permit through document indexing, serialization, and
packaging. It releases the permit before waiting on the uploader, so upload congestion
does not consume indexing capacity.

```mermaid
flowchart LR
    subgraph Indexing pipeline
        direction LR
        publisher --inf--> source
        source[Source] --10--> doc_processor
        doc_processor[DocProcessor] --10--> indexer
        indexer[Indexer] --0--> uploader
        uploader[Uploader] --upload concurrency--> sequencer
        sequencer[Sequencer] --1--> publisher
    end
    subgraph Merge pipeline
        direction LR
        merge_downloader[MergeDownloader] --1--> merge_executor
        merge_executor[MergeExecutor] --0--> merge_uploader
        merge_uploader[MergeUploader] --inf--> merge_publisher
    end
    merge_planner[MergePlanner] --1--> merge_downloader
    merge_publisher[MergePublisher] --1--> merge_planner
    publisher[Publisher] --1--> merge_planner
```
