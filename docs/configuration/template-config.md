---
title: Template configuration
sidebar_position: 7
toc_max_heading_level: 4
---

This page describes how to configure a template.

Templates let you dynamically create indexes according to predefined rules. Templates are used automatically when documents are received on the ingest API for an index that doesn't exist.

The template configuration lets you define the following parameters:
- `template_id` (required)
- `description`
- `index_id_patterns` (required)
- `index_root_uri`
- `priority`

Besides, the following parameters can also be configured and are the same as those found in the [index configuration](../configuration/index-config.md):
- doc mapping (required)
- indexing settings
- search settings
- retention policy

You can manage templates using the [template API](../reference/rest-api.md#template-api).

## Config file format

The index configuration format is YAML or JSON. When a key is absent from the configuration file, the default value is used.
Here is a complete example:

```yaml
version: 0.9 # File format version.

template_id: "hdfs-dev"

index_root_uri: "s3://my-bucket/hdfs-dev/"

description: "HDFS log management dev"

index_id_patterns:
    - hdfs-dev-*
    - hdfs-staging-*

priority: 100

doc_mapping:
  mode: lenient
  field_mappings:
    - name: timestamp
      type: datetime
      input_formats:
        - unix_timestamp
      output_format: unix_timestamp_secs
      fast_precision: seconds
      fast: true
    - name: severity_text
      type: text
      tokenizer: raw
      fast:
        - tokenizer: lowercase
    - name: body
      type: text
      tokenizer: default
      record: position
    - name: resource
      type: object
      field_mappings:
        - name: service
          type: text
          tokenizer: raw
  tag_fields: ["resource.service"]
  timestamp_field: timestamp
  index_field_presence: true

search_settings:
  default_search_fields: [severity_text, body]

retention:
  period: 90 days
  schedule: daily
```

## Template ID

The `template_id` is a string that uniquely identifies the template within the metastore. It may only contain uppercase or lowercase ASCII letters, digits, hyphens (`-`), and underscores (`_`). It must start with a letter and contain at least 3 characters but no more than 255.

## Description

An optional string that describes what the template is used for.

## Index root uri

The `index_root_uri` defines where the index files (also called splits) should be stored.
This parameter expects a [storage uri](storage-config#storage-uris).

The actual URI of the index is the path concatenation of the `index_root_uri` with the index id. 

If `index_root_uri` is not defined, the `default_index_root_uri` from [Quickwit's node config](node-config) will be used.

## Index ID patterns

`index_id_patterns` is a list of strings that define which indices should be created according to this template. Use [glob-like](https://en.wikipedia.org/wiki/Glob_(programming)) wildcard ( \* ) expressions to target indices that match a pattern: test\* or \*test or te\*t or \*test\*. You can also use negative patterns by prepending the hyphen `-` character.

Patterns must obey the following rules:
- It must follow the regex `^-?[a-zA-Z\*][a-zA-Z0-9-_\.\*]{0,254}$`.
- It cannot contain consecutive asterisks (`*`).
- If it does not contain an asterisk (`*`), the length must be greater than or equal to 3 characters.

## Priority

When multiple templates match a new index ID, the template with the highest `priority` is used to configure the index.
