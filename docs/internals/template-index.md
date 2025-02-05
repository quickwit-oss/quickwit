# Index template API

Index templates are a way to create indexes automatically with some given configuration when Quickwit receives documents for an index that doesn't exist yet.

Example of templates: [https://github.com/quickwit-oss/quickwit/tree/main/config/templates](https://github.com/quickwit-oss/quickwit/tree/main/config/templates).

# Curl to run to use the REST API to create Stackoverflow template

```bash
curl -XPOST -H 'Content-Type: application/yaml' 'http://localhost:7280/api/v1/templates' --data-binary @config/templates/stackoverflow.yaml

# Lists templates.
curl 'http://localhost:7280/api/v1/templates'

# Update Stackoverflow template.
curl -XPUT -H 'Content-Type: application/yaml' 'http://localhost:7280/api/v1/templates/stackoverflow' --data-binary @config/templates/stackoverflow.yaml

# Download dataset.
curl -O https://quickwit-datasets-public.s3.amazonaws.com/stackoverflow.posts.transformed-10000.json

# Ingest 10k docs into `stackoverflow-foo` index.
curl -XPOST "http://127.0.0.1:7280/api/v1/stackoverflow-foo/ingest" --data-binary @stackoverflow.posts.transformed-10000.json

# Ingest 10k docs into `stackoverflow-bar` index.
curl -XPOST "http://127.0.0.1:7280/api/v1/stackoverflow-bar/ingest" --data-binary @stackoverflow.posts.transformed-10000.json

# Delete Stackoverflow template.
curl -XDELETE 'http://localhost:7280/api/v1/templates/stackoverflow'

```bash
