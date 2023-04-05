[![CI](https://github.com/quickwit-oss/quickwit/actions/workflows/ci.yml/badge.svg)](https://github.com/quickwit-oss/quickwit/actions?query=workflow%3ACI+branch%3Amain)
[![codecov](https://codecov.io/gh/quickwit-oss/quickwit/branch/main/graph/badge.svg?token=06SRGAV5SS)](https://codecov.io/gh/quickwit-oss/quickwit)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.0-4baaaa.svg)](CODE_OF_CONDUCT.md)
[![License: AGPL V3](https://img.shields.io/badge/license-AGPL%20V3-blue)](LICENCE.md)
[![Twitter Follow](https://img.shields.io/twitter/follow/Quickwit_Inc?color=%231DA1F2&logo=Twitter&style=plastic)](https://twitter.com/Quickwit_Inc)
[![Discord](https://img.shields.io/discord/908281611840282624?logo=Discord&logoColor=%23FFFFFF&style=plastic)](https://discord.quickwit.io)
<br/>

<br/>
<br/>
<p align="center">
  <img src="docs/assets/images/logo_horizontal.svg#gh-light-mode-only" alt="Quickwit Cloud-Native Search Engine" height="40">
  <img src="docs/assets/images/quickwit-dark-theme-logo.png#gh-dark-mode-only" alt="Quickwit Cloud-Native Search Engine" height="40">
</p>

<h2 align="center">
Sub-second search & analytics engine on cloud storage
</h2>

<h4 align="center">
  <a href="https://quickwit.io/docs/get-started/quickstart">Quickstart</a> |
  <a href="https://quickwit.io/docs/">Docs</a> |
  <a href="https://quickwit.io/tutorials">Tutorials</a> |
  <a href="https://discord.quickwit.io">Chat</a> |
  <a href="https://quickwit.io/docs/get-started/installation">Download</a>
</h4>
<br/>

<b> Quickwit 0.5 is now released. Check out our [blog post](https://quickwit.io/blog/quickwit-0.5/) to discover the new features.</b>

![Quickwit Distributed Tracing](./docs/assets/images/quickwit-overview-light.svg#gh-light-mode-only)![Quickwit Distributed Tracing](./docs/assets/images/quickwit-overview-dark.svg#gh-dark-mode-only)

Quickwit is the fastest search engine on cloud storage. It is a great fit for:

- [Log management](https://quickwit.io/docs/log-management/overview)
- [Distributed traces](https://quickwit.io/docs/distributed-tracing/overview)
- Any immutable data: conversational data (emails, texts, messaging platforms) & event-based analytics

… and more!

# 💡 Features

- Full-text search and aggregation queries
- Sub-second search on cloud storage (Amazon S3, Azure Blob Storage, …)
- Decoupled compute and storage, stateless indexers & searchers
- [Schemaless](https://quickwit.io/docs/guides/schemaless) or strict schema indexing
- Kubernetes ready - See our [helm-chart](https://quickwit.io/docs/deployment/kubernetes)
- OTEL-native for [logs](https://quickwit.io/docs/log-management/overview) and [traces](https://quickwit.io/docs/distributed-tracing/overview)
- [Jaeger-native](https://quickwit.io/docs/distributed-tracing/plug-quickwit-to-jaeger)
- RESTful API

## Enterprise ready

- Multiple [data sources](https://quickwit.io/docs/ingest-data/) Kafka / Kinesis / Pulsar native
- Multi-tenancy: indexing with many indexes and partitioning
- Retention policies
- Delete tasks (for GDPR use cases)
- Distributed and highly available* engine that scales out in seconds (*HA indexing only with Kafka)

# 🚀 Quickstart

For a quick guide on how to install Quickwit, start a server, add documents to index, and search them - check out our [Quickstart](https://quickwit.io/docs/get-started/quickstart) guide.

# 📕 Documentation

- [Installation](https://quickwit.io/docs/get-started/installation)
- [Log management with Quickwit](https://quickwit.io/docs/log-management/overview)
- [Distributed Tracing with Quickwit](https://quickwit.io/docs/distributed-tracing/overview)
- [Ingest data](https://quickwit.io/docs/ingest-data/)
- [REST API](https://quickwit.io/docs/reference/rest-api)

# 📚 Resources

- [Blog posts](https://quickwit.io/blog/)
- [Youtube channel](https://www.youtube.com/@quickwit8103)
- [Discord](https://discord.quickwit.io)

# 🔮 Roadmap

- [Quickwit 0.6 - Q2 2023](https://github.com/orgs/quickwit-oss/projects/8)
  - Distributed and replicated native ingestion
  - Local storage caching
  - Grafana data source
  - Compatibility with Elasticsearch query API
- [Long-term roadmap](ROADMAP.md)
  - Live tail
  - SQL
  - Security (TLS, authentication, RBAC)
  - Alerting
  - [and more...](ROADMAP.md)

# 🙋 FAQ

### How can I switch from Elasticsearch to Quickwit?

Quickwit has an Elasticsearch-compatible Ingest-API to make it easier to migrate your log shippers (Vector, Fluent Bit, Syslog, ...) to Quickwit. However, we only support [ES aggregation DSL](https://quickwit.io/docs/reference/aggregation), the query DSL support is planned for Q2 2023.

### How is Quickwit different from traditional search engines like Elasticsearch or Solr?

The core difference and advantage of Quickwit are its architecture built from the ground to search on cloud storage. We optimized IO paths, revamped the index data structures and made search stateless and sub-second on cloud storage.

### How does Quickwit compare to Elastic in terms of cost?

We estimate that Quickwit can be up to 10x cheaper on average than Elastic. To understand how, check out our [blog post](https://quickwit.io/blog/commoncrawl/) about searching the web on AWS S3.

### What license does Quickwit use?

Quickwit is open-source under the GNU Affero General Public License Version 3 - AGPLv3. Fundamentally, this means you are free to use Quickwit for your project if you don't modify Quickwit. However, if you do and you are distributing your modified version to the public, you have to make the modifications public.
We also provide a commercial license for enterprises to provide support and a voice on our roadmap.

### Is it possible to setup Quickwit for a High Availability (HA)?

HA is available for search, for indexing it's available only with a Kafka source. 

### What is Quickwit's business model?

Our business model relies on our commercial license. There is no plan to become SaaS soon.


# 🤝 Contribute and spread the word

We are always super happy to have contributions: code, documentation, issues, feedback, or even saying hello on [Discord](https://discord.quickwit.io)! Here is how you can help us build the future of log management:

- Have a look through GitHub issues labeled "Good first issue".
- Read our [Contributor Covenant Code of Conduct](https://github.com/quickwit-oss/quickwit/blob/0add0562f08e4edd46f5c5537e8ef457d42a508e/CODE_OF_CONDUCT.md).
- Create a fork of Quickwit and submit your pull request!

✨ And to thank you for your contributions, claim your swag by emailing us at hello at quickwit.io.

# 💬 Community

We always welcome contributions: code, documentation, issues, feedback, or even say hello on [Discord](https://discord.quickwit.io)! Here is how you can help us build the next-gen search engine:

- Have a look at the GitHub issues labeled "Good first issue".
- Delve into [contributing guide](CONTRIBUTING.md).
- Read our [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md).
- Create a fork of Quickwit and submit your pull request!

✨ Contributors can claim our swag by emailing us at hello at quickwit.io.

[website]: https://quickwit.io/
[youtube]: https://www.youtube.com/channel/UCvZVuRm2FiDq1_ul0mY85wA
[twitter]: https://twitter.com/Quickwit_Inc
[discord]: https://discord.quickwit.io
[blogs]: https://quickwit.io/blog
