---
title: Source configuration
position: 5
---

Quickwit can insert data into an index from one or multiple sources. When creating an index, sources are declared in the [index config](index-config.md). Additional sources can be added later using the [CLI command](cli.md#source) `quickwit source add`.

A source is declared using an object called source config. A source config uniquely identifies and defines a source. It consists of three parameters:

- source ID
- source type
- source parameters

*Source ID*

The source ID is a string that uniquely identifies the source within an index. It may only contain uppercase or lowercase ASCII letters, digits, hyphens (`-`), periods (`.`) and underscores (`_`). The source ID must start with a letter and must not be longer than 255 characters.

*Source type*

The source type designates the kind of source being configured. As of version 0.2, available source types are `file` and `kafka`.

*Source parameters*

The source parameters indicate how to connect to a data store and are specific to the type of source.

## File source

A file source reads data from a local file. The file must consist of JSON objects separated by a newline. As of version 0.2, compressed files (bz2, gzip, ...) and remote files (Amazon S3, HTTP, ...) are not supported.

### File source parameters

| Property | Description | Default value |
| --- | --- | --- |
| filepath | Path to a local file consisting of JSON objects separated by a newline. |  |

*Declaring a file source in an [index config](index-config.md) (YAML)*

```yaml
# Version of the index config file format
version: 0

# Sources
sources:
  - source_id: my-source-id
    source_type: file
    params:
      filepath: path/to/local/file.json

# The rest of your index config here
# ...
```

*Adding a file source to an index with the [CLI](cli.md#source)*

```bash
quickwit source add --index my-index-id --source my-source-id --type file --params '{"filepath": "path/to/file.json"}'
```

Finally, note that the [CLI command](clid.md#index) `quickwit index ingest` allows ingesting data directly from a file or the standard input without creating a source beforehand.

## Kafka source

A Kafka source reads data from a Kafka stream. Each message in the stream must hold a JSON object.

### Kafka source parameters

The Kafka source consumes a `topic` using the client library [librdkafka](https://github.com/edenhill/librdkafka) and forwards the key-value pairs carried by the parameter `client_params` to the underlying librdkafka consumer. Common `client_params` options are bootstrap servers (`bootstrap.servers`), consumer group ID (`group.id`), or security protocol (`security.protocol`). Please, refer to [Kafka](https://kafka.apache.org/documentation/#consumerconfigs) and [librdkafka](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) documentation pages for more advanced options.

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
  - source_id: my-source-id
    source_type: kafka
    params:
      topic: my-topic
      client_params:
        bootstrap.servers: localhost:9092
        group.id: my-group-id
        security.protocol: SSL

# The rest of your index config here
# ...
```

*Adding a Kafka source to an index with the [CLI](cli.md#source)*

```bash
cat << EOF > my-kafka-source.json
{
  "topic": "my-topic",
  "client_params": {
    "bootstrap.servers": "localhost:9092",
    "group.id": "my-group-id",
    "security.protocol": "SSL"
  }
}
EOF
quickwit source add --index my-index-id --source my-source-id --type kafka --params my-kafka-source.json
```

## Deleting a source from an index
A source can be removed from an index using the [CLI command](cli.md) `quickwit source delete`: 

```bash
quickwit source delete --index my-index-id --source my-source-id
```

When deleting a source, the checkpoint associated with the source is also removed.