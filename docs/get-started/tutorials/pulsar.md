---
title: Ingest from Pulsar
description: A short tutorial describing how to set up Quickwit to ingest data from Pulsar in a few minutes
tags: [pulsar, integration]
icon_url: /img/tutorials/pulsar.svg
sidebar_position: 5
---

In this tutorial, we will describe how to set up Quickwit to ingest data from Pulsar in a few minutes. First, we will create an index and configure a Pulsar source. Then, we will create a Pulsar topic and load some events from the [Stackoverflow dataset](link to SO) into it. Finally, we will execute some search.

## Prerequisites

You will need the following to complete this tutorial:
- A local running [Quickwit instance](/docs/get-started/installation)
- A local running [Pulsar instance](https://pulsar.apache.org/docs/next/getting-started-standalone/). 

### Quickwit setup

[Download](/docs/get-started/installation) Quicwkit and start a server. Then open a new terminal to execute CLI commands with the same binary. 

```bash
./quickwit run
```

Test the cluster is running:

```bash
./quickwit index list
```

### Pulsar setup

```
wget https://archive.apache.org/dist/pulsar/pulsar-2.11.0/apache-pulsar-2.11.0-bin.tar.gz
tar xvfz apache-pulsar-2.11.0-bin.tar.gz
cd apache-pulsar-2.11.0
bin/pulsar standalone
```

## Prepare Quickwit

First, let's create a new index. Here is the index config and doc mapping corresponding to the schema of stackoverflow posts:

```yaml title="index-config.yaml"
#
# Index config file for stackoverflow dataset.
#
version: 0.4

index_id: stackoverflow

doc_mapping:
  field_mappings:
    - name: user
      type: text
      fast: true
      tokenizer: raw
    - name: tags
      type: array<text>
      fast: true
      tokenizer: raw
    - name: type
      type: text
      fast: true
      tokenizer: raw
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
    - name: questionId
      type: u64
    - name: answerId
      type: u64
    - name: acceptedAnswerId
      type: u64
    - name: creationDate
      type: datetime
      fast: true
      input_formats:
        - rfc3339
      precision: seconds
  timestamp_field: creationDate

search_settings:
  default_search_fields: [title, body]

indexing_settings:
  commit_timeout_secs: 10
```

Execute these Bash commands to download the index config and create the `stackoverflow` index.

```bash
# Download stackoverflow index config.
wget -O stackoverflow.yaml https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/stackoverflow/index-config.yaml

# Create index.
./quickwit index create --index-config stackoverflow.yaml
```

## Create and populate a Pulsar topic

Now, let's create a Pulsar topic and send some messages into it.

```bash
# Execute these commands in the pulsar directory.
bin/pulsar-admin tenants create quickwit
# Check it worked.
bin/pulsar-admin tenants list
bin/pulsar-admin namespaces create quickwit/pulsar
# Set retention policy for the namespace
bin/pulsar-admin namespaces set-retention quickwit/pulsar --time 10m --size 1G
# Create topic.
bin/pulsar-admin topics create-partitioned-topic quickwit/pulsar/stackoverflow -p 4
# Check it worked.
bin/pulsar-admin topics list-partitioned-topics quickwit/pulsar
```

To populate the topic, we will use a python script:

```python title=send_messages_to_pulsar.py
import json
import pulsar

client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer('quickwit/pulsar/stackoverflow')

with open('stackoverflow.posts.transformed-10000.json', encoding='utf8') as file:
   for i, line in enumerate(file):
       producer.send(line.encode('utf-8'))
       if i % 100 == 0:
           print(f"{i}/10000 messages sent.", i)

client.close()
```

Install locally the python client, more details on [documentation page](https://pulsar.apache.org/docs/2.11.x/client-libraries-python/):

```bash
# Requires a python version < 3.11
pip3 install 'pulsar-client==2.10.1'
wget https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/stackoverflow/send_messages_to_pulsar.py
python3 send_messages_to_pulsar.py
```

## Create the Pulsar source

```yaml title="pulsar-source.yaml"
#
# Pulsar source config file.
#
version: 0.4
source_id: pulsar-source
source_type: pulsar
params:
  topics:
    - stackoverflow
  address: pulsar://localhost:6650
```

Run these commands to download the source config file and create the source.

```bash
# Download Pulsar source config.
wget -O so-pulsar-source.yaml https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/stackoverflow/pulsar-source.yaml

# Create source.
./quickwit source create --index stackoverflow --source-config so-pulsar-source.yaml
```

As soon as the Pulsar source is created, Quickwit Control Plane will ask an indexer to start a new indexing pipeline. You can observe that by looking at the logs of indexers. On Kubernetes for example:

## Time to search!

You can run this command to inspect the properties of the index and check the current number of published splits and documents:

```bash
# Display some general information about the index.
./quickwit index describe --index stackoverflow
```

You are now ready to execute some queries.

```bash
curl 'http://localhost:7280/api/v1/stackoverflow/search?query=search+AND+engine'
```

It is also possible to access these results through the [Quickwit UI](http://localhost:7280/ui/search?query=&index_id=stackoverflow&max_hits=10).


## Tear down resources (optional)

Let's delete the files and resources created for the purpose of this tutorial.

```bash
# Delete Pulsar tpic.
bin/pulsar-admin topics delete-partitioned-topic quickwit/pulsar/stackoverflow
./quickwit index delete --index stackoverflow --yes
```

This concludes the tutorial. If you have any questions regarding Quickwit or encounter any issues, don't hesitate to ask a [question](https://github.com/quickwit-oss/quickwit/discussions) or open an [issue](https://github.com/quickwit-oss/quickwit/issues) on [GitHub](https://github.com/quickwit-oss/quickwit) or contact us directly on [Discord](https://discord.com/invite/MT27AG5EVE).
