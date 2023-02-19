[![CI](https://github.com/quickwit-oss/quickwit/actions/workflows/ci.yml/badge.svg)](https://github.com/quickwit-oss/quickwit/actions?query=workflow%3ACI+branch%3Amain)
[![codecov](https://codecov.io/gh/quickwit-oss/quickwit/branch/main/graph/badge.svg?token=06SRGAV5SS)](https://codecov.io/gh/quickwit-oss/quickwit)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.0-4baaaa.svg)](CODE_OF_CONDUCT.md)
[![License: AGPL V3](https://img.shields.io/badge/license-AGPL%20V3-blue)](LICENCE.md)
[![Twitter Follow](https://img.shields.io/twitter/follow/Quickwit_Inc?color=%231DA1F2&logo=Twitter&style=plastic)](https://twitter.com/Quickwit_Inc)
[![Discord](https://img.shields.io/discord/908281611840282624?logo=Discord&logoColor=%23FFFFFF&style=plastic)](https://discord.quickwit.io)
![Rust](https://img.shields.io/badge/Rust-black?logo=rust&style=plastic)
<br/>

<br/>
<br/>
<p align="center">
  <img src="docs/assets/images/logo_horizontal.svg#gh-light-mode-only" alt="Quickwit Cloud-Native Search Engine" height="60">
  <img src="docs/assets/images/quickwit-dark-theme-logo.png#gh-dark-mode-only" alt="Quickwit Cloud-Native Search Engine" height="60">
</p>

<h3 align="center">
Search more with less
</h3>

<h4 align="center">The Cloud-Native Search Engine for your observability stack
</h4>
<h4 align="center">
  <a href="https://quickwit.io/docs/get-started/quickstart">Quickstart</a> |
  <a href="https://quickwit.io/docs/">Docs</a> |
  <a href="https://quickwit.io/tutorials">Tutorials</a> |
  <a href="https://discord.quickwit.io">Chat</a> |
  <a href="https://quickwit.io/docs/get-started/installation">Download</a>
</h4>
<br/>

<b> Quickwit 0.5 is coming soon and brings great features: OTEL - Jaeger - Pulsar integrations, RESTful API...</b>

Quickwit has been designed from the ground up to be resource-efficient, easy to operate, and scale to petabytes. It is a perfect fit for observability use cases where you store terabytes of data on cheap storage and enjoy the simplicity of managing decouple compute and storage.
<br/>

## TODO: add short video to install quickwit with https://asciinema.org/ ?

<br/>

# 💡 Features

- Full-text search and aggregation queries
- Sub-second search on object storage
- OTEL-native for logs and traces (0.5)
- Jaeger-native (0.5)
- [Schemaless](https://quickwit.io/docs/guides/schemaless) and strict schema indexing
- Distributed indexing and search
- Multi-tenancy: efficient indexing with many indexes, partitioning
- Kubernetes ready - See our [helm-chart](https://github.com/quickwit-oss/helm-charts)
- Delete tasks - for [GRPR use cases](https://quickwit.io/docs/concepts/deletes)
- Retention policies
- RESTful API to ease Quickwit management
- Kafka / Kinesis / Pulsar native (0.5)
- Extensive language support: Latin, Chinese (Japanese coming soon)
- Sorting
- Easy to install, deploy and maintain

# ⚡ Getting Started

To quick start, follow our [introduction guide](https://quickwit.io/docs/get-started/quickstart) or jump in into one of [our tutorials](https://quickwit.io/tutorials).

Docum

### 🔮 Roadmap
- [Quickwit 0.6 - Q2 2023](https://github.com/quickwit-oss/quickwit/projects/6)
  - Distributed and replicated ingestion queue
  - Elasticsearch JSON queries compatibilty
  - Tiered storage (local drive, block storage, object storage)
  - Grafana data source
  - SSD Caching
- [Long-term roadmap](ROADMAP.md)
  - Pipe-based query language
  - Security (TLS, authentication, RBAC)
  - Transforms
  - [and more...](ROADMAP.md)

# 🙋 FAQ

### How can I switch from Elasticsearch to Quickwit?

In Quickwit 0.3, we released Elasticsearch compatible Ingest-API, so that you can change the configuration of your current log shipper (Vector, Fluent Bit, Syslog, ...) to send data to Quickwit. You can query the logs using the Quickwit Web UI or [Search API](https://quickwit.io/docs/reference/rest-api). We also support [ES compatible Aggregation-API](https://quickwit.io/docs/reference/aggregation).

### How is Quickwit different from traditional search engines like Elasticsearch or Solr?

The core difference and advantage of Quickwit is its architecture that is built from the ground up for cloud and log management. Optimized IO paths make search on object storage sub-second and thanks to the true decoupled compute and storage, search instances are stateless, it is possible to add or remove search nodes within seconds. Last but not least, we implemented a highly-reliable distributed search and exactly-once semantics during indexing so that all engineers can sleep at night. All this slashes costs for log management.

### How does Quickwit compare to Elastic in terms of cost?

We estimate that Quickwit can be up to 10x cheaper on average than Elastic. To understand how, check out our [blog post about searching the web on AWS S3](https://quickwit.io/blog/commoncrawl/).

### What license does Quickwit use?

Quickwit is open-source under the GNU Affero General Public License Version 3 - AGPLv3. Fundamentally, this means that you are free to use Quickwit for your project, as long as you don't modify Quickwit. If you do, you have to make the modifications public.
We also provide a commercial license for enterprises to provide support and a voice on our roadmap.

### Is it possible to setup Quickwit for a High Availability (HA)?

Not today, but HA is on our roadmap.

### What is Quickwit's business model?

Our business model relies on our commercial license. There is no plan to become SaaS in the near future.

# 🪄 Integrations

Ingest your data into Quickwit:
- Jaeger
- Pulsar
- Kafka
- Kinesis
- OTEL logs and traces

Store your data on your favorite object storage:
- AWS S3
- Azure blog storage
- Google Cloud Storage
- Scaleway object storage
- Ceph


# 🤝 Contribute and spread the word

We are always super happy to have contributions: code, documentation, issues, feedback, or even saying hello on [discord](https://discord.quickwit.io)! Here is how you can help us build the future of log management:

- Have a look through GitHub issues labeled "Good first issue".
- Read our [Contributor Covenant Code of Conduct](https://github.com/quickwit-oss/quickwit/blob/0add0562f08e4edd46f5c5537e8ef457d42a508e/CODE_OF_CONDUCT.md).
- Create a fork of Quickwit and submit your pull request!

✨ And to thank you for your contributions, claim your swag by emailing us at hello at quickwit.io.

# 💬 Community

Checkout our [📝 blog Posts](https://quickwit.io/blog) and [📽 Youtube channel](https://www.youtube.com/channel/UCvZVuRm2FiDq1_ul0mY85wA).

Chat with us in [Discord](https://discord.quickwit.io) | Follow us on [Twitter](https://twitter.com/quickwit_inc)


# 🔗 Reference

- [Quickwit CLI](https://quickwit.io/docs/reference/cli)
- [Index Config](https://quickwit.io/docs/configuration/index-config)
- [Search API](https://quickwit.io/docs/reference/rest-api)
- [Query language](https://quickwit.io/docs/reference/query-language)
- [Code of conduct](CODE_OF_CONDUCT.md)
- [Contributing](CONTRIBUTING.md)

[website]: https://quickwit.io/
[youtube]: https://www.youtube.com/channel/UCvZVuRm2FiDq1_ul0mY85wA
[twitter]: https://twitter.com/Quickwit_Inc
[discord]: https://discord.quickwit.io
[blogs]: https://quickwit.io/blog
