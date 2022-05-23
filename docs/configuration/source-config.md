---
title: Source configuration
sidebar_position: 4
---

Quickwit can insert data into an index from one or multiple sources. When creating an index, sources are declared in the [index config](index-config.md). Additional sources can be added later using the [CLI command](../reference/cli.md#source) `quickwit source create`.

A source is declared using an object called source config. A source config uniquely identifies and defines a source. It consists of three parameters:

- source ID
- source type
- source parameters

*Source ID*

The source ID is a string that uniquely identifies the source within an index. It may only contain uppercase or lowercase ASCII letters, digits, hyphens (`-`), and underscores (`_`). Finally, it must start with a letter and contain at least 3 characters but no more than 255.

*Source type*

The source type designates the kind of source being configured. As of version 0.3, available source types are `file`, `kafka`, and `kinesis`.

*Source parameters*

The source parameters indicate how to connect to a data store and are specific to the source type.

## File source

A file source reads data from a local file. The file must consist of JSON objects separated by a newline. As of version 0.3, compressed files (bz2, gzip, ...) and remote files (Amazon S3, HTTP, ...) are not supported.

### File source parameters

| Property | Description | Default value |
| --- | --- | --- |
| filepath | Path to a local file consisting of JSON objects separated by a newline. |  |

*Declaring a file source in an [index config](../configuration/index-config.md) (YAML)*

```yaml
# Version of the index config file format
version: 0

# Sources
sources:
  - source_id: my-file-source
    source_type: file
    params:
      filepath: path/to/local/file.json

# The rest of your index config here
# ...
```

*Adding a file source to an index with the [CLI](../reference/cli.md#source)*

```bash
cat << EOF > source-config.yaml
source_id: my-file-source
source_type: file
params:
  filepath: path/to/local/file.json  # The file must exist.
EOF
quickwit source create --index my-index --source-config source-config.yaml
```

Finally, note that the [CLI command](../reference/cli.md#index) `quickwit index ingest` allows ingesting data directly from a file or the standard input without creating a source beforehand.

## Kafka source

A Kafka source reads data from a Kafka stream. Each message in the stream must hold a JSON object.

### Kafka source parameters

The Kafka source consumes a `topic` using the client library [librdkafka](https://github.com/edenhill/librdkafka) and forwards the key-value pairs carried by the parameter `client_params` to the underlying librdkafka consumer. Common `client_params` options are bootstrap servers (`bootstrap.servers`), or security protocol (`security.protocol`). Please, refer to [Kafka](https://kafka.apache.org/documentation/#consumerconfigs) and [librdkafka](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) documentation pages for more advanced options.

| Property | Description | Default value |
| --- | --- | --- |
| topic | Name of the topic to consume. |  |
| client_log_level | librdkafka client log level. Possible values are: debug, info, warn, error. | info |
| client_params | librdkafka client configuration parameters. |  |

Note that the Kafka source manages commit offsets manually thanks to Quickwit’s index checkpoint mechanism and always disables auto-commit.

*Declaring a Kafka source in an [index config](index-config.md) (YAML)*


```yaml
# Version of the index config file format
version: 0

# Sources
sources:
  - source_id: my-kafka-source
    source_type: kafka
    params:
      topic: my-topic
      client_params:
        bootstrap.servers: localhost:9092
        security.protocol: SSL

# The rest of your index config here
# ...
```

*Adding a Kafka source to an index with the [CLI](../reference/cli.md#source)*

```bash
cat << EOF > source-config.yaml
source_id: my-kafka-source
source_type: kafka
params:
  topic: my-topic
  client_params:
    bootstrap.servers: localhost:9092
    security.protocol: SSL
EOF
quickwit source create --index my-index --source-config source-config.yaml
```

## Deleting a source from an index

A source can be removed from an index using the [CLI command](../reference/cli.md) `quickwit source delete`: 

```bash
quickwit source delete --index my-index --source my-source
```

When deleting a source, the checkpoint associated with the source is also removed.