---
title: Local file
description: A short tutorial describing how to index a local file with the Quickiwt CLI 
tags: [local-ingest, integration]
icon_url: /img/tutorials/file-ndjson.svg
sidebar_position: 2
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

In this tutorial, we will describe how to index a local file with the Quickwit CLI.

You will need the [Quickwit binary](/docs/get-started/installation.md) to follow this tutorial.

## Create an index

First, let's create a schemaless index. We need to start a Quickwit server only for the creation so we will start it and shut it down afterwards.

Start the Quickwit server.

```bash
./quickwit run
```

And create the index in a separate terminal.

```bash
# Create the index config file.
cat << EOF > stackoverflow-schemaless-config.yaml
version: 0.5
index_id: stackoverflow-schemaless
doc_mapping:
  mode: dynamic
indexing_settings:
  commit_timeout_secs: 1
EOF

./quickwit index create --index-config stackoverflow-schemaless-config.yaml
```

You can now shutdown the server by pressing `Ctrl+C` in the first terminal.

## Ingest the file

To ingest a file, you just need to execute the following command:

```bash
./quickwit tool local-ingest --index stackoverflow-schemaless --input-path stackoverflow.posts.transformed-10000.json
```

After a few seconds you should see the following output:

```bash
❯ Ingesting documents locally...

---------------------------------------------------
 Connectivity checklist
 ✔ metastore
 ✔ storage
 ✔ _ingest-cli-source

 Num docs   10000 Parse errs     0 PublSplits   1 Input size     6MB Thrghput  3.34MB/s Time 00:00:02
 Num docs   10000 Parse errs     0 PublSplits   1 Input size     6MB Thrghput  2.23MB/s Time 00:00:03
 Num docs   10000 Parse errs     0 PublSplits   1 Input size     6MB Thrghput  1.67MB/s Time 00:00:04

Indexed 10,000 documents in 4s.
Now, you can query the index with the following command:
quickwit index search --index stackoverflow-schemaless --config ./config/quickwit.yaml --query "my query"
Clearing local cache directory...
✔ Local cache directory cleared.
✔ Documents successfully indexed.
```

## Tear down resources (optional)

That's it! You can now tear down the resources you created. You can do so by running the following command:

```bash
./quickwit run
```

And in a separate terminal:

```bash
./quickwit index delete --index-id stackoverflow-schemaless
```

This concludes the tutorial. You can now move on to the next tutorial.
