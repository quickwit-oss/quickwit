[![CI](https://github.com/quickwit-oss/quickwit/actions/workflows/ci.yml/badge.svg)](https://github.com/quickwit-oss/quickwit/actions?query=workflow%3ACI+branch%3Amain)
[![codecov](https://codecov.io/gh/quickwit-oss/quickwit/branch/main/graph/badge.svg?token=06SRGAV5SS)](https://codecov.io/gh/quickwit-oss/quickwit)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.0-4baaaa.svg)](CODE_OF_CONDUCT.md)
[![License: AGPL V3](https://img.shields.io/badge/license-AGPL%20V3-blue)](LICENCE.md)
[![Twitter Follow](https://img.shields.io/twitter/follow/Quickwit_Inc?color=%231DA1F2&logo=Twitter&style=plastic)](https://twitter.com/Quickwit_Inc)
[![Discord](https://img.shields.io/discord/908281611840282624?logo=Discord&logoColor=%23FFFFFF&style=plastic)](https://discord.gg/rpRRTezWhW)
![Rust](https://img.shields.io/badge/Rust-black?logo=rust&style=plastic)
<br/>

<br/>
<br/>
<p align="center">
  <img src="docs/assets/images/logo_horizontal.svg#gh-light-mode-only" alt="Quickwit Log Management & Analytics" height="60">
  <img src="docs/assets/images/quickwit-dark-theme-logo.png#gh-dark-mode-only" alt="Quickwit Log Management & Analytics" height="60">
</p>

<h3 align="center">
Search more with less
</h3>

<h4 align="center">The new way to manage your logs at any scale
</h4>
<h4 align="center">
  <a href="https://quickwit.io/docs/get-started/quickstart">Quickstart</a> |
  <a href="https://quickwit.io/docs/">Docs</a> |
  <a href="https://quickwit.io/tutorials">Tutorials</a> |
  <a href="https://discord.gg/rpRRTezWhW">Chat</a> |
  <a href="https://quickwit.io/docs/get-started/installation">Download</a>
</h4>
<br/>

Quickwit is a cloud-native search engine for log management & analytics. It is designed to be very cost-effective, easy to operate, and scale to petabytes.

<br/>

<img src="docs/assets/images/quickwit-ui.png">

<br/>

# 💡 Features

- Index data persisted on object storage (AWS S3, GCS, MinIO, Ceph)
- Ingest JSON documents with or without a strict schema
- Ingest & Aggregation API Elasticsearch compatible
- Lightweight Embedded UI
- Runs on a fraction of the resources: written in Rust, powered by the mighty tantivy
- Works out of the box with sensible defaults
- Optimized for multi-tenancy. Add and scale tenants with no overhead costs
- Distributed search
- Cloud-native: Kubernetes ready
- Add and remove nodes in seconds
- Decoupled compute & storage
- Sleep like a log: all your indexed data is safely stored on object storage
- Ingest your documents with exactly-once semantics
- Kafka-native ingestion
- Search stream API that notably unlocks full-text search in ClickHouse

### 🔮 Roadmap
- [Quickwit 0.4 - August 2022](https://github.com/quickwit-oss/quickwit/projects/5)
  - Native support for Kubernetes
  - Boolean, datetime, and IP address fields
  - Range queries
  - Sort
  - Retention policies
- [Quickwit 0.5 - October 2022](https://github.com/quickwit-oss/quickwit/projects/6)
  - Grafana data source
  - Native support for OpenTelemetry
  - REST API for managing indexes
- [Long-term roadmap](ROADMAP.md)
  - Distributed indexing
  - Pipe-based query language
  - Security (TLS, authentication, RBAC)
  - [and more...](ROADMAP.md)


# 🔎 Uses & Limitations
| :white_check_mark: &nbsp; When to use                                                  	| :x: &nbsp; When not to use                                       	|
|--------------------------------------------------------------	|--------------------------------------------------------------	|
| Your documents are immutable: application logs, system logs, access logs, user actions logs, audit trail  (logs), etc.                    	| Your documents are mutable.   	|
| Your data has a time component. Quickwit includes optimizations and design choices specifically related to time. | You need a low-latency search for e-commerce websites.               	|
| You want a full-text search in a multi-tenant environment.     	| You provide a public-facing search with high QPS.	|
| You want to index directly from Kafka. | You want to re-score documents at query time.
| You want to add full-text search to your ClickHouse cluster.
| You ingest a tremendous amount of logs and don't want to pay huge bills.                                                             	|
| You ingest a tremendous amount of data and you don't want to waste your precious time babysitting your cluster.

# ⚡  Getting Started

Quickwit compiles to a single binary and we provide [various ways to install it](https://quickwit.io/docs/get-started/installation). The easiest is to run the command below from your preferred shell:

```markdown
curl -L https://install.quickwit.io | sh
```

You can now move this executable directory wherever sensible for your environment and possibly add it to your `PATH` environment. 

Take a look at our [Quick Start]([https://quickwit.io/docs/get-started/quickstart) to do amazing things, like [Creating your first index](https://quickwit.io/docs/get-started/quickstart#create-your-first-index) or [Adding some documents](https://quickwit.io/docs/get-started/quickstart#lets-add-some-documents), or take a glance at our full [Installation guide](https://quickwit.io/docs/get-started/installation)!


# 📚 Tutorials

- [Set up a cluster on a local machine](https://quickwit.io/tutorials/tutorial-hdfs-logs/)
- [Set up a distributed search on AWS S3](https://quickwit.io/tutorials/tutorial-hdfs-logs-distributed-search-aws-s3)
- [Send logs from Vector to Quickwit](https://quickwit.io/tutorials/send-logs-from-vector-to-quickwit/)
- [Ingest data from Apache Kafka](https://quickwit.io/tutorials/kafka/)
- [Ingest data from Amazon Kinesis](https://quickwit.io/tutorials/kinesis/)
- [Add full-text search to a well-known OLAP database, ClickHouse](https://quickwit.io/tutorials/add-full-text-search-to-your-olap-db)

# 🙋 FAQ
### How can I switch from Elasticsearch to Quickwit?
In Quickwit 0.3, we released Elasticsearch compatible Ingest-API, so that you can change the configuration of your current log shipper (Vector, Fluent Bit, Syslog, ...) to send data to Quickwit. You can query the logs using the Quickwit Web UI or [Search API](https://quickwit.io/docs/reference/rest-api). We also support [ES compatible Aggregation-API](https://quickwit.io/docs/reference/aggregation).

###  How is Quickwit different from traditional search engines like Elasticsearch or Solr?
The core difference and advantage of Quickwit is its architecture that is built from the ground up for cloud and log management. Optimized IO paths make search on object storage sub-second and thanks to the true decoupled compute and storage, search instances are stateless, it is possible to add or remove search nodes within seconds. Last but not least, we implemented a highly-reliable distributed search and exactly-once semantics during indexing so that all engineers can sleep at night. All this slashes costs for log management.

### How does Quickwit compare to Elastic in terms of cost?
We estimate that Quickwit can be up to 10x cheaper on average than Elastic. To understand how, check out our [blog post about searching the web on AWS S3](https://quickwit.io/blog/commoncrawl/).

### What license does Quickwit use?
Quickwit is open-source under the GNU Affero General Public License Version 3 - AGPLv3. Fundamentally, this means that you are free to use Quickwit for your project, as long as you don't modify Quickwit. If you do, you have to make the modifications public.
We also provide a commercial license for enterprises to provide support and a voice on our roadmap.

### Is is pottible to setup Quickwit for a High Availability (HA)?
Not today, but HA is on our roadmap. 

### What is Quickwit's business model?
Our business model relies on our commercial license. There is no plan to become SaaS in the near future.


# 🪄 Third-Party Integration
<p align="left">
<img align="center" src="docs/assets/images/aws-logo.png#gh-light-mode-only" alt="Store logs on AWS S3" height="25" width="auto" />
<img align="center" src="docs/assets/images/aws-dark-theme-logo.png#gh-dark-mode-only" alt="Store logs on AWS S3" height="25" width="auto" /> &nbsp;
<img align="center" src="docs/assets/images/google-cloud-storage.svg" alt="Google Cloud Storage" height="30" width="auto"/> &nbsp;
<img align="center" src="docs/assets/images/postgresql-logo.png" alt="Metastore Backed by Postgresql" height="30" width="auto"/> &nbsp;&nbsp;
<img align="center" src="docs/assets/images/minio-logo.png" alt="Integrate with Minio" height="10" width="auto"/> &nbsp;
<img align="center" src="docs/assets/images/ceph-light-mode-logo.png#gh-light-mode-only" alt="Integration with Ceph" height="20" width="auto"/> 
<img align="center" src="docs/assets/images/ceph-dark-mode-logo.png#gh-dark-mode-only" alt="Integration with Ceph" height="20" width="auto"/> &nbsp;
<img align="center" src="docs/assets/images/kubernetes-logo.png" alt="Collect Logs on Kubernetes cluster" height="30" width="auto"/> &nbsp;
<img align="center" src="docs/assets/images/kafka-logo.png#gh-light-mode-only" alt="Ingest Logs with Kafka" height="30" width="auto"/>
<img align="center" src="docs/assets/images/kafka-dark-theme.png#gh-dark-mode-only" alt="Ingest Logs with Kafka" height="30" width="auto"/> &nbsp;
<img align="center" src="docs/assets/images/kinesis-logo.svg" alt="Ingest logs with Amazon Kinesis" height="30" width="auto"/> &nbsp;
</p>

# 💬 Community

### [📝 Blog Posts](https://quickwit.io/blog)
  - [ChitChat: Cluster Membership with Failure Detection](https://quickwit.io/blog/chitchat)
  - [How to investigate memory usage of your rust program](https://quickwit.io/blog/memory-inspector-gadget)
### [📽 Youtube Videos](https://www.youtube.com/channel/UCvZVuRm2FiDq1_ul0mY85wA)
  - [Internals of Quickwit & How We Built It](https://www.youtube.com/watch?v=s_C8O5ecZBE)
  - [Stream Ingestion with Kafka & Kinesis](https://www.youtube.com/watch?v=05pS-m6iuQ4)

Chat with us in [Discord](https://discord.gg/rpRRTezWhW) | Follow us on [Twitter](https://twitter.com/quickwit_inc)


# 🤝 Contribute and spread the word

We are always super happy to have contributions: code, documentation, issues, feedback, or even saying hello on [discord](https://discord.gg/rpRRTezWhW)! Here is how you can help us build the future of log management: 
- Have a look through GitHub issues labeled "Good first issue".
- Read our [Contributor Covenant Code of Conduct](https://github.com/quickwit-oss/quickwit/blob/0add0562f08e4edd46f5c5537e8ef457d42a508e/CODE_OF_CONDUCT.md).
- Create a fork of Quickwit and submit your pull request!

✨ And to thank you for your contributions, claim your swag by emailing us at hello at quickwit.io.


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
[discord]: https://discord.gg/MT27AG5EVE
[blogs]: https://quickwit.io/blog
