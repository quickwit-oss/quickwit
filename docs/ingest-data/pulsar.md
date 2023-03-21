---
title: Pulsar
description: A short tutorial describing how to set up Quickwit to ingest data from Pulsar in a few minutes
tags: [pulsar, integration]
icon_url: /img/tutorials/pulsar.svg
sidebar_position: 3
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

In this tutorial, we will describe how to set up Quickwit to ingest data from Pulsar in a few minutes. First, we will create an index and configure a Pulsar source. Then, we will create a Pulsar topic and load some events from the [Stack Overflow dataset](https://www.kaggle.com/stackoverflow/stacksample) into it. Finally, we will execute some searches.

## Prerequisites

You will need the following to complete this tutorial:
- A local running [Quickwit instance](/docs/get-started/installation.md)
- A local running [Pulsar instance](https://pulsar.apache.org/docs/next/getting-started-standalone/)

### Quickwit setup

[Download](/docs/get-started/installation.md) Quickwit and start a server. Then open a new terminal to execute CLI commands with the same binary. 

```bash
./quickwit run
```

Test that the cluster is running:

```bash
./quickwit index list
```

### Pulsar setup

<Tabs>

<TabItem value="Local" label="Local">

```bash
wget https://archive.apache.org/dist/pulsar/pulsar-2.11.0/apache-pulsar-2.11.0-bin.tar.gz
tar xvfz apache-pulsar-2.11.0-bin.tar.gz
cd apache-pulsar-2.11.0
bin/pulsar standalone
```

</TabItem>

<TabItem value="Docker" label="Docker">

```bash
docker run -it -p 6650:6650 -p 8080:8080 apachepulsar/pulsar:2.11.0 bin/pulsar standalone
```

See the details on the [official documentation](https://pulsar.apache.org/docs/next/getting-started-docker/).

</TabItem>

</Tabs>

## Prepare Quickwit

First, let's create a new index. Here is the index config and doc mapping corresponding to the schema of Stack Overflow posts:

```yaml title="index-config.yaml"
#
# Index config file for Stack Overflow dataset.
#
version: 0.5

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

## Create the Pulsar source

A Pulsar source just needs to define the list of topics and the instance address.

```yaml title="pulsar-source.yaml"
#
# Pulsar source config file.
#
version: 0.5
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
wget -O stackoverflow-pulsar-source.yaml https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/stackoverflow/pulsar-source.yaml

# Create source.
./quickwit source create --index stackoverflow --source-config stackoverflow-pulsar-source.yaml
```

As soon as the Pulsar source is created, Quickwit control plane will ask an indexer to start a new indexing pipeline. You will see logs like below by looking on the indexer:

```bash
INFO spawn_pipeline{index=stackoverflow gen=0}:pulsar-consumer{subscription_name="quickwit-stackoverflow-pulsar-source" params=PulsarSourceParams { topics: ["stackoverflow"], address: "pulsar://localhost:6650", consumer_name: "quickwit", authentication: None } current_positions={}}: quickwit_indexing::source::pulsar_source: Seeking to last checkpoint positions. positions={}
```

## Create and populate a Pulsar topic

We will use the Pulsar's default tenant/namespace `public/default`. To populate the topic, we will use a python script:

```python title=send_messages_to_pulsar.py
import json
import pulsar

client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer('public/default/stackoverflow')

with open('stackoverflow.posts.transformed-10000.json', encoding='utf8') as file:
   for i, line in enumerate(file):
       producer.send(line.encode('utf-8'))
       if i % 100 == 0:
           print(f"{i}/10000 messages sent.", i)

client.close()
```

Install locally the python client, more details on [documentation page](https://pulsar.apache.org/docs/2.11.x/client-libraries-python/):

```bash
# Download the first 10_000 Stackoverflow posts articles.
curl -O https://quickwit-datasets-public.s3.amazonaws.com/stackoverflow.posts.transformed-10000.json

# Install pulsar python client.
# Requires a python version < 3.11
pip3 install 'pulsar-client==2.10.1'
wget https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/stackoverflow/send_messages_to_pulsar.py
python3 send_messages_to_pulsar.py
```

## Time to search!

You can run this command to inspect the properties of the index and check the current number of published splits and documents:

```bash
# Display some general information about the index.
./quickwit index describe --index stackoverflow
```

You will notably see the number of published documents.

You are now ready to execute some queries.

```bash
curl 'http://localhost:7280/api/v1/stackoverflow/search?query=search+AND+engine'
```

If your Quickwit server is local, you can access to the results through Quickwit UI on [localhost:7280](http://localhost:7280/ui/search?query=&index_id=stackoverflow&max_hits=10).


## Tear down resources (optional)

Let's delete the files and resources created for the purpose of this tutorial.

```bash
# Delete quickwit index.
./quickwit index delete --index stackoverflow --yes
# Delete Pulsar topic.
bin/pulsar-admin topics delete stackoverflow
```

This concludes the tutorial. If you have any questions regarding Quickwit or encounter any issues, don't hesitate to ask a [question](https://github.com/quickwit-oss/quickwit/discussions) or open an [issue](https://github.com/quickwit-oss/quickwit/issues) on [GitHub](https://github.com/quickwit-oss/quickwit) or contact us directly on [Discord](https://discord.com/invite/MT27AG5EVE).
