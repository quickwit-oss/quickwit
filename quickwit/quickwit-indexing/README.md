```mermaid
flowchart LR
    subgraph Indexing pipeline
        direction LR
        source[Source] --> indexer
        indexer[Indexer] --> packager
        packager[Packager] --> uploader
        uploader[Uploader] --> publisher
    end
    subgraph Merge pipeline
        direction LR
        merge_packager[MergePackager] --> merge_uploader
        merge_uploader[MergeUploader] --> merge_publisher
    end
    merge_planner[MergePlanner] --> merge_packager
    merge_publisher[MergePublisher] --> merge_planner
    publisher[Publisher] --> merge_planner
```
