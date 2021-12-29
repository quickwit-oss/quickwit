---
title: Add full-text search to Clickhouse
sidebar_position: 2
---


This guide will help you add full-text search to a well-known OLAP database, Clickhouse, using the Quickwit search streaming feature. Indeed Quickwit exposes a REST endpoint that streams ids or whatever attributes matching a search query **extremely fast** (up to 50 million in 1 second), and Clickhouse can easily use them with joins queries. 

We will take the [Github archive dataset](https://www.gharchive.org/), which gathers more than 3 billion Github events: `WathEvent`, `PullRequestEvent`, `IssuesEvent`... You can dive into this [great analysis](https://ghe.clickhouse.tech/) made by Clickhouse to have a good understanding of the dataset. We also took strong inspiration from this work, and we are very grateful to them for sharing this.

## Install

```bash
curl -L https://install.quickwit.io | sh
cd quickwit-0.2
# Quickwit detects the config from CLI args or the QW_CONFIG env variable.
# Let's set QW_CONFIG to the default config.
export QW_CONFIG=./config/quickwit.yaml
```

## Create a Quickwit index

After [installing quickwit], let's create an index configured to receive these events.  Let's first look at the data to ingest. Here is an event example:

```JSON
{
  "id": 11410577343,
  "event_type": "PullRequestEvent",
  "actor_login": "renovate[bot]",
  "repo_name": "dmtrKovalenko/reason-date-fns",
  "created_at": 1580515200000,
  "action": "closed",
  "number": 44,
  "title": "Update dependency rollup to ^1.31.0",
  "labels": [],
  "ref": null,
  "additions": 5,
  "deletions": 5,
  "commit_id": null,
  "body":"This PR contains the following updates..."
}
```



We don't need to index all fields described above as Quickwit. The `title` and `body` are the fields of interest for our full-text search tutorial. `id` will be helpful to make the join in Clickhouse, `created_at` and `event_type` may also be beneficial for timestamp pruning and filtering.

```yaml title="gh-archive-index-config.yaml"
version: 0
index_id: gh-archive
index_uri: INDEX_URI
doc_mapping:
  field_mappings:
    - name: id
      type: u64
      fast: true
      stored: true
    - name: created_at
      type: i64
      fast: true
      stored: true
    - name: event_type
      type: text
      tokenizer: raw
      stored: true
    - name: title
      type: text
      tokenizer: default
      record: position
      stored: true
    - name: body
      type: text
      tokenizer: default
      record: position
      stored: true
search_settings:
  default_search_fields: [title, body]
}
```

```bash
curl -o gh-archive-index-config.yaml https://raw.githubusercontent.com/quickwit-inc/quickwit/main/config/tutorials/gh-archive/index-config.yaml
./quickwit index create --index gh-archive --index-config gh-archive-index-config.yaml
```

## Indexing events

The dataset is a compressed [ndjson file](https://quickwit-datasets-public.s3.amazonaws.com/gh-archive/gh-archive-2021-12.json.gz).
Let's index it.

```bash
curl https://quickwit-datasets-public.s3.amazonaws.com/gh-archive/gh-archive-2021-12-text-only.json.gz
gunzip gh-archive-2021-12-text-only.json.gz | quickwit index ingest --index gh-archive
```

You can check it's working by using the `search` command and looking for `tantivy` word:
```bash
quickwit index search --index gh-archive --query "tantivy"
```


## Start a searcher

```bash
quickwit service run searcher
```

This command will start an HTTP server with a [REST API](../reference/search-api.md). We are now
ready to fetch some ids with the search stream endpoint. Let's start by streaming them on a simple
query and with a `CSV` output format.

```bash
curl -v "http://0.0.0.0:8080/api/v1/gh-archive/search/stream?query=tantivy&outputFormat=Csv&fastField=id"
```

We will use the `Clikchouse` binary output format in the following sections to speed up queries.


## Clickhouse

Let's leave Quickwit for now and [install a Clickhouse server](https://clickhouse.com/docs/en/getting-started/install/).

### Create database and table

Once installed, just [start a client](https://clickhouse.com/docs/en/getting-started/install/) and execute the following sql statements:
```SQL
CREATE DATABASE "gh-archive";
USE "gh-archive";


CREATE TABLE github_events
(
    id UInt64,
    type Enum('CommitCommentEvent' = 1, 'CreateEvent' = 2, 'DeleteEvent' = 3, 'ForkEvent' = 4,
                    'GollumEvent' = 5, 'IssueCommentEvent' = 6, 'IssuesEvent' = 7, 'MemberEvent' = 8,
                    'PublicEvent' = 9, 'PullRequestEvent' = 10, 'PullRequestReviewCommentEvent' = 11,
                    'PushEvent' = 12, 'ReleaseEvent' = 13, 'SponsorshipEvent' = 14, 'WatchEvent' = 15,
                    'GistEvent' = 16, 'FollowEvent' = 17, 'DownloadEvent' = 18, 'PullRequestReviewEvent' = 19,
                    'ForkApplyEvent' = 20, 'Event' = 21, 'TeamAddEvent' = 22),
    actor_login LowCardinality(String),
    repo_name LowCardinality(String),
    created_at Int64,
    action Enum('none' = 0, 'created' = 1, 'added' = 2, 'edited' = 3, 'deleted' = 4, 'opened' = 5, 'closed' = 6, 'reopened' = 7, 'assigned' = 8, 'unassigned' = 9,
                'labeled' = 10, 'unlabeled' = 11, 'review_requested' = 12, 'review_request_removed' = 13, 'synchronize' = 14, 'started' = 15, 'published' = 16, 'update' = 17, 'create' = 18, 'fork' = 19, 'merged' = 20),
    comment_id UInt64,
    body String,
    ref LowCardinality(String),
    number UInt32,
    title String,
    labels Array(LowCardinality(String)),
    additions UInt32,
    deletions UInt32,
    commit_id String,
) ENGINE = MergeTree ORDER BY (type, repo_name, created_at);
```

### Import events

We have created a second dataset, `gh-archive-2021-12.json.gz`, which gathers all events, even ones with no
text. So it's better to insert it into Clickhouse, but if you don't have the time, you can use the dataset
`gh-archive-2021-12-text-only.json.gz` used for Quickwit.

```bash
curl https://quickwit-datasets-public.s3.amazonaws.com/gh-archive/gh-archive-2021-12.json.gz
gunzip . | clickhouse-client -d gh-archive --query="INSERT INTO github_events FORMAT JSONEachRow"
```

Let's check it's working:
```SQL
// Top repositories by stars
SELECT repo_name, count() AS stars 
FROM github_events 
WHERE event_type = 'WatchEvent' 
GROUP BY repo_name 
ORDER BY stars DESC LIMIT 5

┌─repo_name────────────┬─stars─┐
│ TencentARC/GFPGAN    │  5659 │
│ Eugeny/tabby         │  4027 │
│ prabhatsharma/zinc   │  3936 │
│ AppFlowy-IO/appflowy │  3382 │
│ vercel/turborepo     │  3314 │
└──────────────────────┴───────┘
```

### Use Quickwit search inside Clickhouse

Clikhouse has an exciting feature called [URL Table Engine](https://clickhouse.com/docs/en/engines/table-engines/special/url/) that queries data from a remote HTTP/HTTPS server.
This is precisely what we need: by creating a table pointing to Quickwit search stream endpoint, we will fetch ids that match a query from Clickhouse. 

```SQL
SELECT count(*) FROM url('http://127.0.0.1:7280/api/v1/gh-archive/search/stream?query=log4j+OR+log4shell&fastField=id&outputFormat=clickHouseRowBinary', RowBinary, 'id UInt64')

┌─count()─┐
│   99584 │
└─────────┘

1 rows in set. Elapsed: 0.012 sec. Processed 99.13 thousand rows, 793.00 KB (7.96 million rows/s., 63.66 MB/s.)
```

We are fetching 100 000 u64 ids in 0.012 seconds. That's 8 million rows per second, not bad. And it's possible to increase the throughput with more extensive queries.


Let's do another example with a more exciting query that will match `log4j` or `log4shell` and count events per day:

SELECT
    count(*),
    toDate(fromUnixTimestamp64Milli(created_at)) AS date
FROM github_events
WHERE id IN (
    SELECT id
    FROM url('http://127.0.0.1:7280/api/v1/gh-archive/search/stream?query=log4j+OR+log4shell&fastField=id&outputFormat=clickHouseRowBinary', RowBinary, 'id UInt64')
)
GROUP BY date

Query id: 10cb0d5a-7817-424e-8248-820fa2c425b8

┌─count()─┬───────date─┐
│      93 │ 2021-12-01 │
│      69 │ 2021-12-02 │
│      68 │ 2021-12-03 │
│      57 │ 2021-12-04 │
│      74 │ 2021-12-05 │
│     158 │ 2021-12-06 │
│     143 │ 2021-12-07 │
│     102 │ 2021-12-08 │
│     158 │ 2021-12-09 │
│   87777 │ 2021-12-10 │
│    3225 │ 2021-12-11 │
│    1530 │ 2021-12-12 │
│    5762 │ 2021-12-13 │
└─────────┴────────────┘

14 rows in set. Elapsed: 0.180 sec. Processed 45.53 million rows, 396.64 MB (253.59 million rows/s., 2.21 GB/s.)

```

We can see the spike on the 2021-12-10.

## Wrapping up

We have just scratched the surface of full-text search from Clikchouse with this small subset of Github archive. You can play with the complete dataset that you can download from our public S3 bucket. We have made available monthly gzipped ndjson files from 2015 until 2021. Here are `2015-01` links:
- full JSON dataset https://quickwit-datasets-public.s3.amazonaws.com/gh-archive/gh-archive-2015-01.json.gz
- text-only JSON dataset https://quickwit-datasets-public.s3.amazonaws.com/gh-archive/gh-archive-2015-01-text-only.json.gz

The search stream endpoint is powerful enough to stream 100 million ids to Clickhouse in less than 2 seconds on a multi TB dataset. And you should be comfortable playing with search stream on even bigger datasets.
